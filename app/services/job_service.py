"""
Serviço de orquestração de jobs OCR.
"""

import pdfplumber
from io import BytesIO
from dataclasses import dataclass
from typing import Optional

from fastapi import HTTPException, UploadFile

from app.scripts.job_state import registry, JobStatus
from app.scripts.storage import storage
from app.config.settings import settings
from app.config.logging_config import get_logger
from app.services import pipeline_dispatcher

logger = get_logger(__name__)


@dataclass
class SubmitResult:
    job_id: str
    pdf_key: str
    metadata: dict


def validate_pdf(
    file: UploadFile,
    pdf_bytes: bytes,
    client_ip: str,
) -> int:
    """
    Valida tipo, tamanho e integridade do PDF.
    Retorna total_pages em caso de sucesso; levanta HTTPException em caso de falha.
    """
    size_mb = len(pdf_bytes) / (1024 * 1024)

    if (
        not file.filename.lower().endswith(".pdf")
        or file.content_type != "application/pdf"
    ):
        logger.warning(
            "Arquivo rejeitado: não é PDF",
            extra={
                "action": "invalid_file_type",
                "file_name": file.filename,
                "content_type": file.content_type,
                "client_ip": client_ip,
            },
        )
        raise HTTPException(400, "Apenas arquivos PDF são aceitos")

    if size_mb > settings.MAX_FILE_SIZE_MB:
        logger.warning(
            "Arquivo rejeitado: tamanho excede limite",
            extra={
                "action": "file_too_large",
                "size_mb": round(size_mb, 2),
                "limit_mb": settings.MAX_FILE_SIZE_MB,
                "file_name": file.filename,
                "client_ip": client_ip,
            },
        )
        raise HTTPException(
            413,
            f"Arquivo muito grande: {size_mb:.1f}MB (máximo: {settings.MAX_FILE_SIZE_MB}MB)",
        )

    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            total_pages = len(pdf.pages)
    except Exception as exc:
        logger.error(
            "PDF inválido ou corrompido ao inspecionar páginas",
            extra={
                "action": "pdf_invalid",
                "file_name": file.filename,
                "size_mb": round(size_mb, 2),
                "error": str(exc),
                "client_ip": client_ip,
            },
            exc_info=True,
        )
        raise HTTPException(400, "PDF inválido ou corrompido")

    if total_pages == 0:
        logger.warning(
            "PDF rejeitado: sem páginas",
            extra={
                "action": "pdf_empty",
                "file_name": file.filename,
                "client_ip": client_ip,
            },
        )
        raise HTTPException(400, "PDF sem páginas")

    if total_pages > settings.MAX_PAGES:
        logger.warning(
            "PDF rejeitado: excede limite de páginas",
            extra={
                "action": "pdf_too_many_pages",
                "total_pages": total_pages,
                "limit": settings.MAX_PAGES,
                "file_name": file.filename,
                "client_ip": client_ip,
            },
        )
        raise HTTPException(
            400,
            f"PDF excede o limite de {settings.MAX_PAGES} páginas (recebido: {total_pages})",
        )

    return total_pages


def create_and_submit_job(
    file: UploadFile,
    pdf_bytes: bytes,
    total_pages: int,
    chunk_size: Optional[int],
    tags: Optional[str],
    client_ip: str,
) -> SubmitResult:
    """
    Cria o job no Redis, faz upload do PDF para o S3 e despacha o pipeline.
    Retorna SubmitResult com job_id, pdf_key e metadata.
    """
    size_mb = len(pdf_bytes) / (1024 * 1024)

    metadata = {
        "file_name": file.filename,
        "size_bytes": len(pdf_bytes),
        "size_mb": round(size_mb, 2),
        "content_type": file.content_type,
        "tags": [t.strip() for t in tags.split(",")] if tags else [],
        "chunk_size_override": chunk_size,
    }

    state = registry.create(filename=file.filename, metadata=metadata)
    job_id = state.get()["job_id"]

    logger.info(
        "Job criado — iniciando upload para S3",
        extra={
            "action": "job_created",
            "job_id": job_id,
            "file_name": file.filename,
            "size_mb": round(size_mb, 2),
            "total_pages": total_pages,
            "client_ip": client_ip,
        },
    )

    pdf_key = f"jobs/{job_id}/input/{file.filename}"

    state.set_status(JobStatus.UPLOADING, "Enviando PDF para storage...")
    try:
        storage.upload_bytes(pdf_key, pdf_bytes, content_type="application/pdf")
    except Exception as exc:
        logger.error(
            "Falha no upload do PDF para o S3",
            extra={
                "action": "s3_upload_failed",
                "job_id": job_id,
                "pdf_key": pdf_key,
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
            exc_info=True,
        )
        state.set_status(JobStatus.FAILED, f"Falha no upload: {str(exc)}")
        raise HTTPException(500, f"Erro ao armazenar PDF: {str(exc)}")

    state.set(
        pdf_key=pdf_key,
        status=JobStatus.QUEUED.value,
        message="PDF armazenado. Pipeline enfileirado.",
    )

    pipeline_dispatcher.dispatch(job_id, pdf_key)

    return SubmitResult(job_id=job_id, pdf_key=pdf_key, metadata=metadata)


def create_and_submit_job_from_key(
    pdf_key: str,
    filename: str,
    tags: Optional[str],
) -> str:
    """
    Cria um job a partir de um PDF já existente no S3 e despacha o pipeline.
    Retorna o job_id criado.
    """
    if not storage.object_exists(pdf_key):
        logger.warning(
            "Submit por chave S3: objeto não encontrado",
            extra={"action": "s3_key_not_found", "pdf_key": pdf_key},
        )
        raise HTTPException(404, f"Objeto não encontrado no S3: {pdf_key}")

    state = registry.create(
        filename=filename,
        metadata={"pdf_key": pdf_key, "tags": tags.split(",") if tags else []},
    )
    job_id = state.get()["job_id"]
    state.set(pdf_key=pdf_key)

    pipeline_dispatcher.dispatch(job_id, pdf_key)

    logger.info(
        "Job enfileirado via chave S3",
        extra={"action": "job_queued_by_key", "job_id": job_id, "pdf_key": pdf_key},
    )

    return job_id


def get_job_index(job_id: str) -> dict:
    """
    Recupera o índice completo de um job concluído, enriquecido com URLs pré-assinadas.
    Levanta HTTPException se o job não existir, não estiver concluído, ou o índice falhar.
    """
    state = registry.get(job_id)
    if not state:
        logger.warning(
            "Consulta de índice para job inexistente",
            extra={"action": "job_index_not_found", "job_id": job_id},
        )
        raise HTTPException(404, f"Job {job_id} não encontrado")

    data = state.get()
    if data.get("status") != JobStatus.COMPLETED.value:
        return {"_pending": True, "job_id": job_id, "data": data}

    outputs = data.get("outputs", {})
    index_key = outputs.get("index_key")

    if not index_key:
        logger.error(
            "Índice ausente nos outputs de job concluído",
            extra={"action": "index_key_missing", "job_id": job_id, "outputs": outputs},
        )
        raise HTTPException(500, "Índice não encontrado nos outputs do job")

    try:
        index = storage.download_json(index_key)
        for chunk in index.get("chunks", []):
            chunk["markdown_url"] = storage.get_presigned_url(chunk["markdown_key"])
            chunk["json_url"] = storage.get_presigned_url(chunk["json_key"])
        return index
    except Exception as exc:
        logger.error(
            "Falha ao recuperar índice do S3",
            extra={
                "action": "index_download_failed",
                "job_id": job_id,
                "index_key": index_key,
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
            exc_info=True,
        )
        raise HTTPException(500, f"Erro ao recuperar índice: {str(exc)}")


def get_chunk_with_urls(job_id: str, chunk_index: int) -> dict:
    """
    Recupera metadados de um chunk específico, enriquecidos com URLs pré-assinadas.
    """
    state = registry.get(job_id)
    if not state:
        raise HTTPException(404, f"Job {job_id} não encontrado")

    chunk = state.get_chunk(chunk_index)
    if not chunk:
        logger.warning(
            "Chunk consultado ainda não disponível",
            extra={
                "action": "chunk_not_ready",
                "job_id": job_id,
                "chunk_index": chunk_index,
            },
        )
        raise HTTPException(
            404, f"Chunk {chunk_index} não encontrado ou ainda não processado"
        )

    if chunk.get("markdown_key"):
        chunk["markdown_url"] = storage.get_presigned_url(chunk["markdown_key"])
    if chunk.get("json_key"):
        chunk["json_url"] = storage.get_presigned_url(chunk["json_key"])

    return chunk


def get_chunk_content(job_id: str, chunk_index: int) -> dict:
    """
    Recupera o conteúdo markdown de um chunk, pronto para uso como contexto LLM.
    """
    state = registry.get(job_id)
    if not state:
        raise HTTPException(404, f"Job {job_id} não encontrado")

    chunk = state.get_chunk(chunk_index)
    if not chunk or chunk.get("status") != "done":
        raise HTTPException(404, f"Chunk {chunk_index} ainda não disponível")

    try:
        markdown = storage.download_text(chunk["markdown_key"])
        return {
            "job_id": job_id,
            "chunk_index": chunk_index,
            "start_page": chunk["start_page"],
            "end_page": chunk["end_page"],
            "tokens_estimate": chunk.get("tokens_estimate", 0),
            "content": markdown,
        }
    except Exception as exc:
        logger.error(
            "Falha ao recuperar conteúdo do chunk",
            extra={
                "action": "chunk_content_download_failed",
                "job_id": job_id,
                "chunk_index": chunk_index,
                "markdown_key": chunk.get("markdown_key"),
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
            exc_info=True,
        )
        raise HTTPException(500, f"Erro ao recuperar conteúdo: {str(exc)}")
