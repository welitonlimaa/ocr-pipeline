"""
API FastAPI: ponto de entrada síncrono para disparar o pipeline OCR.

Endpoints:
  POST /jobs/submit       - Envia PDF e inicia processamento (síncrono no upload, async no OCR)
  POST /jobs/submit-url   - Submete PDF por URL ou caminho no S3
  GET  /jobs/{job_id}     - Status do job em tempo real
  GET  /jobs/{job_id}/index - Índice completo com chaves de output
  GET  /jobs/{job_id}/chunks/{chunk_index} - Resultado de um chunk específico
  GET  /health            - Health check
"""

import pdfplumber
import time as _time
from io import BytesIO
from typing import Optional
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from app.scripts.job_state import registry, JobStatus
from app.scripts.storage import storage
from app.config.settings import settings
from app.utils.check_rate_limit import RateLimitExceeded, check_rate_limit
from app.utils.get_client_ip import get_client_ip
from app.config.redis import redis_client
from app.config.logging_config import get_logger, configure_logging

configure_logging()
logger = get_logger(__name__)

try:
    from workers.pipeline import process_document

    CELERY_AVAILABLE = True
except Exception:
    CELERY_AVAILABLE = False
    logger.warning(
        "Celery não disponível — pipeline rodará em modo local (threads)",
        extra={"action": "celery_unavailable"},
    )

app = FastAPI(
    title="OCR Pipeline API",
    description="Pipeline desacoplado de OCR para PDFs grandes com S3 + Redis + Celery",
    version="1.0.0",
)

allow_origins = settings.CORS_ORIGINS
allow_credentials = False if allow_origins == ["*"] else True

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Middleware: log de acesso
# ---------------------------------------------------------------------------


@app.middleware("http")
async def log_requests(request: Request, call_next):
    t0 = _time.time()
    response = await call_next(request)
    elapsed = round(_time.time() - t0, 3)

    level = logger.warning if response.status_code >= 400 else logger.info
    level(
        "HTTP request",
        extra={
            "action": "http_request",
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "elapsed_seconds": elapsed,
            "client_ip": request.client.host if request.client else "unknown",
        },
    )
    return response


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------


def _job_response(job_id: str, state_data: dict) -> dict:
    """Formata resposta padrão de job."""
    return {
        "job_id": job_id,
        "status": state_data.get("status"),
        "message": state_data.get("message"),
        "progress_pct": state_data.get("progress_pct", 0),
        "progress_pages": state_data.get("progress_pages", 0),
        "total_pages": state_data.get("total_pages", 0),
        "created_at": state_data.get("created_at"),
        "updated_at": state_data.get("updated_at"),
        "outputs": state_data.get("outputs", {}),
        "metadata": state_data.get("metadata", {}),
    }


def _dispatch_pipeline(job_id: str, pdf_key: str):
    """Despacha task Celery ou executa em modo local para testes."""
    if CELERY_AVAILABLE:
        process_document.delay(job_id, pdf_key)
    else:
        import threading
        from workers.pipeline import process_document as _pd

        t = threading.Thread(target=_pd, args=(None, job_id, pdf_key), daemon=True)
        t.start()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/health")
def health():
    """Health check do serviço."""
    redis_ok = False
    s3_ok = False

    try:
        r = registry.get_redis()
        r.ping()
        redis_ok = True
    except Exception as exc:
        logger.error(
            "Health check: Redis indisponível",
            extra={"action": "health_redis_fail", "error": str(exc)},
        )

    try:
        storage.client.head_bucket(Bucket=settings.S3_bucket)
        s3_ok = True
    except Exception as exc:
        logger.error(
            "Health check: S3 indisponível",
            extra={"action": "health_s3_fail", "error": str(exc)},
        )

    overall = "ok" if (redis_ok and s3_ok) else "degraded"

    if overall == "degraded":
        logger.warning(
            "Health check degraded",
            extra={
                "action": "health_degraded",
                "redis": redis_ok,
                "s3": s3_ok,
            },
        )

    return {
        "status": overall,
        "redis": "ok" if redis_ok else "error",
        "s3": "ok" if s3_ok else "error",
        "celery": "ok" if CELERY_AVAILABLE else "unavailable (modo local)",
    }


@app.post("/jobs/submit", status_code=202)
async def submit_pdf(
    request: Request,
    file: UploadFile = File(..., description="Arquivo PDF para processamento"),
    chunk_size: Optional[int] = Form(
        None, description="Páginas por chunk (padrão: config)"
    ),
    tags: Optional[str] = Form(
        None, description="Tags CSV opcionais: 'relatorio,2024'"
    ),
):
    """
    Submete um PDF para processamento OCR.
    Síncrono no upload → Assíncrono no pipeline (Celery).
    """
    client_ip = get_client_ip(request)

    try:
        check_rate_limit(redis_client, client_ip)
    except RateLimitExceeded:
        logger.warning(
            "Rate limit excedido",
            extra={"action": "rate_limit_exceeded", "client_ip": client_ip},
        )
        raise HTTPException(
            status_code=429, detail="Limite de requisições diárias atingido"
        )

    if (
        not file.filename.lower().endswith(".pdf")
        or file.content_type != "application/pdf"
    ):
        logger.warning(
            "Arquivo rejeitado: não é PDF",
            extra={
                "action": "invalid_file_type",
                "filename": file.filename,
                "content_type": file.content_type,
                "client_ip": client_ip,
            },
        )
        raise HTTPException(400, "Apenas arquivos PDF são aceitos")

    pdf_bytes = await file.read()
    size_mb = len(pdf_bytes) / (1024 * 1024)

    if size_mb > settings.max_file_size_mb:
        logger.warning(
            "Arquivo rejeitado: tamanho excede limite",
            extra={
                "action": "file_too_large",
                "size_mb": round(size_mb, 2),
                "limit_mb": settings.max_file_size_mb,
                "filename": file.filename,
                "client_ip": client_ip,
            },
        )
        raise HTTPException(
            413,
            f"Arquivo muito grande: {size_mb:.1f}MB (máximo: {settings.max_file_size_mb}MB)",
        )

    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            total_pages = len(pdf.pages)
    except Exception as exc:
        logger.error(
            "PDF inválido ou corrompido ao inspecionar páginas",
            extra={
                "action": "pdf_invalid",
                "filename": file.filename,
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
                "filename": file.filename,
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
                "filename": file.filename,
                "client_ip": client_ip,
            },
        )
        raise HTTPException(
            400,
            f"PDF excede o limite de {settings.MAX_PAGES} páginas (recebido: {total_pages})",
        )

    metadata = {
        "filename": file.filename,
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
            "filename": file.filename,
            "size_mb": round(size_mb, 2),
            "total_pages": total_pages,
            "client_ip": client_ip,
        },
    )

    state.set_status(JobStatus.UPLOADING, "Enviando PDF para storage...")
    pdf_key = f"jobs/{job_id}/input/{file.filename}"

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

    if CELERY_AVAILABLE:
        process_document.delay(job_id, pdf_key)
        logger.info(
            "Task Celery despachada",
            extra={
                "action": "celery_task_dispatched",
                "job_id": job_id,
                "pdf_key": pdf_key,
            },
        )
    else:
        import threading
        from workers.pipeline import _run_pipeline_local

        threading.Thread(
            target=_run_pipeline_local, args=(job_id, pdf_key), daemon=True
        ).start()
        logger.info(
            "Pipeline iniciado em modo local (thread)",
            extra={"action": "local_thread_started", "job_id": job_id},
        )

    return JSONResponse(
        status_code=202,
        content={
            "job_id": job_id,
            "status": "queued",
            "message": "PDF recebido. Processamento iniciado em background.",
            "tracking": {
                "status_url": f"/jobs/{job_id}",
                "index_url": f"/jobs/{job_id}/index",
                "storage_path": f"s3://{settings.S3_bucket}/jobs/{job_id}/",
            },
            "metadata": metadata,
        },
    )


@app.post("/jobs/submit-key", status_code=202)
async def submit_by_s3_key(
    pdf_key: str,
    filename: str = "document.pdf",
    tags: Optional[str] = None,
):
    """
    Submete PDF já existente no S3 para processamento.
    Útil para integrar com outros pipelines que já fazem upload.
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

    if CELERY_AVAILABLE:
        process_document.delay(job_id, pdf_key)
        logger.info(
            "Job enfileirado via chave S3",
            extra={"action": "job_queued_by_key", "job_id": job_id, "pdf_key": pdf_key},
        )

    return JSONResponse(
        status_code=202,
        content={
            "job_id": job_id,
            "status": "queued",
            "message": "Job enfileirado para PDF existente no S3.",
            "tracking": {"status_url": f"/jobs/{job_id}"},
        },
    )


@app.get("/jobs/{job_id}")
def get_job_status(job_id: str):
    """Retorna estado atual do job com progresso em tempo real."""
    state = registry.get(job_id)
    if not state:
        logger.warning(
            "Consulta de status para job inexistente",
            extra={"action": "job_not_found", "job_id": job_id},
        )
        raise HTTPException(404, f"Job {job_id} não encontrado")
    return _job_response(job_id, state.get())


@app.get("/jobs/{job_id}/index")
def get_job_index(job_id: str):
    """
    Retorna índice completo do documento processado.
    Disponível apenas quando o job está COMPLETED.
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
        return JSONResponse(
            status_code=202,
            content={
                "job_id": job_id,
                "status": data.get("status"),
                "message": "Processamento ainda em andamento. Consulte /jobs/{job_id} para acompanhar.",
                "progress_pct": data.get("progress_pct", 0),
            },
        )

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


@app.get("/jobs/{job_id}/chunks/{chunk_index}")
def get_chunk(job_id: str, chunk_index: int):
    """Retorna resultado de um chunk específico."""
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


@app.get("/jobs/{job_id}/chunks/{chunk_index}/content")
def get_chunk_content(job_id: str, chunk_index: int):
    """Retorna o conteúdo markdown de um chunk para uso direto como contexto LLM."""
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
