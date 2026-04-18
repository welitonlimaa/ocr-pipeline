from typing import Optional

from fastapi import APIRouter, Depends, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse

from app.api.dependencies import require_job, require_rate_limit
from app.api.schemas import (
    JobKeySubmitResponse,
    JobPendingResponse,
    JobStatusResponse,
    JobSubmitResponse,
    TrackingInfo,
)
from app.config.settings import settings
from app.scripts.job_state import JobState
from app.services import job_service

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("/submit", status_code=202, response_model=JobSubmitResponse)
async def submit_pdf(
    request: Request,
    file: UploadFile = File(..., description="Arquivo PDF para processamento"),
    chunk_size: Optional[int] = Form(
        None, description="Páginas por chunk (padrão: config)"
    ),
    tags: Optional[str] = Form(
        None, description="Tags CSV opcionais: 'relatorio,2024'"
    ),
    client_ip: str = Depends(require_rate_limit),
) -> JobSubmitResponse:
    """
    Submete um PDF para processamento OCR.
    Síncrono no upload → Assíncrono no pipeline (Celery).
    """
    pdf_bytes = await file.read()
    total_pages = job_service.validate_pdf(file, pdf_bytes, client_ip)

    result = job_service.create_and_submit_job(
        file=file,
        pdf_bytes=pdf_bytes,
        total_pages=total_pages,
        chunk_size=chunk_size,
        tags=tags,
        client_ip=client_ip,
    )

    return JobSubmitResponse(
        job_id=result.job_id,
        status="queued",
        message="PDF recebido. Processamento iniciado em background.",
        tracking=TrackingInfo(
            status_url=f"/jobs/{result.job_id}",
            index_url=f"/jobs/{result.job_id}/index",
            storage_path=f"s3://{settings.S3_BUCKET}/jobs/{result.job_id}/",
        ),
        metadata=result.metadata,
    )


@router.post("/submit-key", status_code=202, response_model=JobKeySubmitResponse)
async def submit_by_s3_key(
    pdf_key: str,
    filename: str = "document.pdf",
    tags: Optional[str] = None,
) -> JobKeySubmitResponse:
    """
    Submete um PDF já existente no S3 para processamento.
    Útil para integrar com outros pipelines que já fazem upload.
    """
    job_id = job_service.create_and_submit_job_from_key(
        pdf_key=pdf_key,
        filename=filename,
        tags=tags,
    )

    return JobKeySubmitResponse(
        job_id=job_id,
        status="queued",
        message="Job enfileirado para PDF existente no S3.",
        tracking=TrackingInfo(status_url=f"/jobs/{job_id}"),
    )


@router.get("/{job_id}", response_model=JobStatusResponse)
def get_job_status(state: JobState = Depends(require_job)) -> JobStatusResponse:
    """Retorna estado atual do job com progresso em tempo real."""
    data = state.get()
    return JobStatusResponse(
        job_id=data["job_id"],
        status=data.get("status"),
        message=data.get("message"),
        progress_pct=data.get("progress_pct", 0),
        progress_pages=data.get("progress_pages", 0),
        total_pages=data.get("total_pages", 0),
        created_at=data.get("created_at"),
        updated_at=data.get("updated_at"),
        outputs=data.get("outputs", {}),
        metadata=data.get("metadata", {}),
    )


@router.get("/{job_id}/index")
def get_job_index(job_id: str, state: JobState = Depends(require_job)):
    """
    Retorna índice completo do documento processado.
    Disponível apenas quando o job está COMPLETED.
    Contém todas as chaves S3 dos chunks extraídos, prontas para uso em LLM.
    """
    result = job_service.get_job_index(job_id)

    if result.get("_pending"):
        data = result["data"]
        return JSONResponse(
            status_code=202,
            content=JobPendingResponse(
                job_id=job_id,
                status=data.get("status"),
                message="Processamento ainda em andamento. Consulte /jobs/{job_id} para acompanhar.",
                progress_pct=data.get("progress_pct", 0),
            ).model_dump(),
        )

    return result


@router.get("/{job_id}/chunks/{chunk_index}")
def get_chunk(job_id: str, chunk_index: int, state: JobState = Depends(require_job)):
    """Retorna metadados e URLs de um chunk específico."""
    return job_service.get_chunk_with_urls(job_id, chunk_index)


@router.get("/{job_id}/chunks/{chunk_index}/content")
def get_chunk_content(
    job_id: str, chunk_index: int, state: JobState = Depends(require_job)
):
    """Retorna o conteúdo markdown de um chunk para uso direto como contexto LLM."""
    return job_service.get_chunk_content(job_id, chunk_index)
