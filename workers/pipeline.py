"""
Workers Celery: pipeline desacoplado de processamento OCR.

Fluxo:
  1. process_document (task principal)
  2. extract_chunk (por chunk, em paralelo)
  3. finalize_document (após todos os chunks)

Cada task atualiza o estado do job no Redis em tempo real.
"""

import time
from celery import Celery, chord, group
from celery.utils.log import get_task_logger

from app.config.settings import settings
from app.scripts.extractor import (
    extract_chunk,
    compute_chunks,
    get_pdf_page_count,
    ChunkResult,
)
from app.scripts.storage import storage
from app.scripts.job_state import registry, JobStatus
from app.scripts.knowledge_condenser import KnowledgeCondenser
from app.utils.compute_text_stats import compute_text_stats
from app.utils.create_zip_from_keys import create_zip_from_keys
from app.config.logging_config import get_logger


logger = get_logger(__name__)

_celery_logger = get_task_logger(__name__)

celery_app = Celery(
    "ocr_pipeline",
    broker=settings.celery_broker,
    backend=settings.celery_backend,
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    result_expires=settings.REDIS_JOB_TTL,
)


# ---------------------------------------------------------------------------
# Helpers de log
# ---------------------------------------------------------------------------


def _job_ctx(job_id: str, **extra) -> dict:
    """Monta dicionário de contexto base para logs de job."""
    return {"job_id": job_id, **extra}


def _chunk_ctx(job_id: str, chunk_index: int, **extra) -> dict:
    """Monta dicionário de contexto base para logs de chunk."""
    return {"job_id": job_id, "chunk_index": chunk_index, **extra}


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@celery_app.task(bind=True, name="pipeline.process_document", max_retries=2)
def process_document(self, job_id: str, pdf_object_key: str):
    """
    Task principal: orquestra o pipeline completo para um PDF.
    Divide em chunks e dispara processamento paralelo.
    """
    ctx = _job_ctx(job_id, pdf_key=pdf_object_key, celery_task_id=self.request.id)

    state = registry.get(job_id)
    if not state:
        logger.error(
            "Job não encontrado no Redis — descartando task",
            extra={**ctx, "action": "job_not_found"},
        )
        return

    try:
        logger.info(
            "Iniciando download do PDF do storage",
            extra={**ctx, "action": "pdf_download_start"},
        )
        state.set_status(JobStatus.PROCESSING, "Baixando PDF do storage...")
        t_download = time.time()
        pdf_bytes = storage.download_bytes(pdf_object_key)
        download_elapsed = round(time.time() - t_download, 2)

        logger.info(
            "PDF baixado com sucesso",
            extra={
                **ctx,
                "action": "pdf_download_ok",
                "size_bytes": len(pdf_bytes),
                "elapsed_seconds": download_elapsed,
            },
        )

        state.set_status(JobStatus.PROCESSING, "Analisando estrutura do PDF...")
        total_pages = get_pdf_page_count(pdf_bytes)
        chunks = compute_chunks(total_pages)
        total_chunks = len(chunks)

        logger.info(
            "Estrutura do PDF analisada — disparando chunks",
            extra={
                **ctx,
                "action": "chunks_dispatched",
                "total_pages": total_pages,
                "total_chunks": total_chunks,
                "CHUNK_SIZE_PAGES": settings.CHUNK_SIZE_PAGES,
            },
        )

        state.set(
            total_pages=total_pages,
            total_chunks=total_chunks,
            processed_chunks=0,
        )
        state.set_status(
            JobStatus.EXTRACTING,
            f"Extraindo {total_pages} páginas em {total_chunks} chunks...",
        )

        chord_tasks = group(
            extract_chunk_task.s(job_id, pdf_object_key, idx, start, end)
            for idx, (start, end) in enumerate(chunks)
        )
        callback = finalize_document.s(job_id, total_pages, total_chunks)
        chord(chord_tasks)(callback)

    except Exception as exc:
        retry_count = self.request.retries
        logger.error(
            "Falha na orquestração do pipeline — agendando retry",
            extra={
                **ctx,
                "action": "pipeline_failed",
                "retry_attempt": retry_count,
                "max_retries": self.max_retries,
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
            exc_info=True,
        )
        state.set_status(JobStatus.FAILED, f"Erro: {str(exc)}")
        raise self.retry(exc=exc, countdown=10)


@celery_app.task(bind=True, name="pipeline.extract_chunk", max_retries=3)
def extract_chunk_task(
    self,
    job_id: str,
    pdf_object_key: str,
    chunk_index: int,
    start_page: int,
    end_page: int,
) -> dict:
    """
    Extrai texto e tabelas de um chunk de páginas.
    Salva resultado no S3 e registra progresso no Redis.
    """
    ctx = _chunk_ctx(
        job_id,
        chunk_index,
        start_page=start_page + 1,
        end_page=end_page + 1,
        celery_task_id=self.request.id,
    )
    state = registry.get(job_id)

    try:
        logger.info(
            "Iniciando extração de chunk",
            extra={**ctx, "action": "chunk_start"},
        )

        pdf_bytes = storage.download_bytes(pdf_object_key)

        t0 = time.time()
        result: ChunkResult = extract_chunk(
            pdf_bytes, chunk_index, start_page, end_page
        )
        extraction_elapsed = round(time.time() - t0, 2)

        logger.debug(
            "Extração concluída — iniciando upload para S3",
            extra={
                **ctx,
                "action": "chunk_extracted",
                "elapsed_seconds": extraction_elapsed,
                "tokens_estimate": result.summary_tokens_estimate,
                "tables_found": len(result.tables_combined),
                "pages_in_chunk": len(result.pages),
            },
        )

        md_key = f"jobs/{job_id}/chunks/chunk_{chunk_index:04d}.md"
        t_upload = time.time()
        storage.upload_text(md_key, result.markdown_combined)

        structured = {
            "job_id": job_id,
            "chunk_index": chunk_index,
            "start_page": result.start_page,
            "end_page": result.end_page,
            "pages_count": len(result.pages),
            "tables_count": len(result.tables_combined),
            "tokens_estimate": result.summary_tokens_estimate,
            "tables": result.tables_combined,
            "pages_summary": [
                {
                    "page_num": p.page_num,
                    "word_count": p.word_count,
                    "has_tables": p.has_tables,
                    "has_images": p.has_images,
                }
                for p in result.pages
            ],
        }
        json_key = f"jobs/{job_id}/chunks/chunk_{chunk_index:04d}.json"
        storage.upload_json(json_key, structured)
        upload_elapsed = round(time.time() - t_upload, 2)

        chunk_summary = {
            "chunk_index": chunk_index,
            "start_page": result.start_page,
            "end_page": result.end_page,
            "markdown_key": md_key,
            "json_key": json_key,
            "tokens_estimate": result.summary_tokens_estimate,
            "tables_count": len(result.tables_combined),
            "elapsed_seconds": extraction_elapsed,
            "status": "done",
        }

        if state:
            state.add_chunk_result(chunk_index, chunk_summary)
            state.increment_progress_chunks()
        else:
            logger.warning(
                "Estado do job ausente ao finalizar chunk — progresso não atualizado",
                extra={**ctx, "action": "chunk_state_missing"},
            )

        logger.info(
            "Chunk processado com sucesso",
            extra={
                **ctx,
                "action": "chunk_done",
                "extraction_elapsed_seconds": extraction_elapsed,
                "upload_elapsed_seconds": upload_elapsed,
                "tokens_estimate": result.summary_tokens_estimate,
                "tables_count": len(result.tables_combined),
                "md_key": md_key,
            },
        )
        return chunk_summary

    except Exception as exc:
        retry_count = self.request.retries
        will_retry = retry_count < self.max_retries

        logger.error(
            "Falha na extração do chunk",
            extra={
                **ctx,
                "action": "chunk_failed",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "retry_attempt": retry_count,
                "max_retries": self.max_retries,
                "will_retry": will_retry,
            },
            exc_info=True,
        )

        if state:
            state.add_chunk_result(
                chunk_index,
                {"chunk_index": chunk_index, "status": "error", "error": str(exc)},
            )

        if not will_retry:
            logger.critical(
                "Chunk esgotou todas as tentativas — marcado como falho definitivamente",
                extra={
                    **ctx,
                    "action": "chunk_exhausted_retries",
                    "error_type": type(exc).__name__,
                },
            )

        raise self.retry(exc=exc, countdown=5, max_retries=3)


@celery_app.task(name="pipeline.finalize_document")
def finalize_document(
    chunk_results: list[dict], job_id: str, total_pages: int, total_chunks: int
):
    ctx = _job_ctx(job_id, total_pages=total_pages, total_chunks=total_chunks)
    state = registry.get(job_id)

    try:
        successful = [c for c in chunk_results if c and c.get("status") == "done"]
        failed = [c for c in chunk_results if not c or c.get("status") == "error"]

        logger.info(
            "Iniciando finalização do documento",
            extra={
                **ctx,
                "action": "finalize_start",
                "chunks_received": len(chunk_results),
                "chunks_ok": len(successful),
                "chunks_failed": len(failed),
            },
        )

        if failed:
            logger.warning(
                "Chunks com falha detectados na finalização",
                extra={
                    **ctx,
                    "action": "finalize_partial_failure",
                    "failed_chunk_indexes": [c.get("chunk_index") for c in failed if c],
                    "chunks_failed": len(failed),
                },
            )

        state.set_status(JobStatus.INDEXING, "Consolidando índice do documento...")

        total_tokens = sum(c.get("tokens_estimate", 0) for c in successful)
        total_tables = sum(c.get("tables_count", 0) for c in successful)

        # --- Condensação de conhecimento ---
        state.set_status(JobStatus.INDEXING, "Condensando conhecimento global...")

        md_texts = []
        for c in sorted(successful, key=lambda x: x.get("chunk_index", 0)):
            md_key = c["markdown_key"]
            text = storage.download_text(md_key)
            md_texts.append(text)

        logger.debug(
            "Textos markdown carregados — iniciando condensação",
            extra={
                **ctx,
                "action": "condense_start",
                "chunks_to_condense": len(md_texts),
                "total_tokens_estimate": total_tokens,
            },
        )

        t_condense = time.time()
        condenser = KnowledgeCondenser(n_topics=10, sentences_per_topic=8)
        condensed_text = condenser.condense(md_texts)
        condense_elapsed = round(time.time() - t_condense, 2)

        stats = compute_text_stats(condensed_text)

        logger.info(
            "Condensação concluída",
            extra={
                **ctx,
                "action": "condense_done",
                "elapsed_seconds": condense_elapsed,
                "condensed_word_count": stats.get("word_count"),
                "condensed_tokens_estimate": stats.get("tokens_estimate"),
            },
        )

        condensed_metadata = {
            "job_id": job_id,
            "tokens_estimate": stats["tokens_estimate"],
            "word_count": stats["word_count"],
        }

        condensed_json_key = f"jobs/{job_id}/condensed.json"
        condensed_json_url = storage.upload_json(condensed_json_key, condensed_metadata)

        condensed_md_key = f"jobs/{job_id}/condensed.md"
        condensed_md_url = storage.upload_text(condensed_md_key, condensed_text)

        # --- Índice principal ---
        index = {
            "job_id": job_id,
            "total_pages": total_pages,
            "total_chunks": total_chunks,
            "chunks_processed": len(successful),
            "chunks_failed": len(failed),
            "total_tokens_estimate": total_tokens,
            "total_tables_extracted": total_tables,
            "chunks": sorted(successful, key=lambda c: c.get("chunk_index", 0)),
            "failed_chunks": failed,
            "llm_context": {
                "format": "markdown",
                "chunk_keys": [
                    c["markdown_key"]
                    for c in sorted(successful, key=lambda c: c.get("chunk_index", 0))
                ],
                "structured_keys": [
                    c["json_key"]
                    for c in sorted(successful, key=lambda c: c.get("chunk_index", 0))
                ],
                "recommended_chunk_size_tokens": 8000,
                "total_estimated_tokens": total_tokens,
            },
            "condensed_context": {
                "md_key": condensed_md_key,
                "md_url": condensed_md_url,
                "json_key": condensed_json_key,
                "json_url": condensed_json_url,
                "strategy": "tfidf_lsa_textrank",
            },
        }

        index_key = f"jobs/{job_id}/index.json"
        index_url = storage.upload_json(index_key, index)

        logger.debug(
            "Índice principal gerado",
            extra={**ctx, "action": "index_uploaded", "index_key": index_key},
        )

        # --- ZIP final ---
        state.set_status(JobStatus.INDEXING, "Gerando pacote final (ZIP)...")

        files_to_zip = []
        for c in successful:
            files_to_zip.append(c["markdown_key"])
            files_to_zip.append(c["json_key"])
        files_to_zip.extend([condensed_md_key, condensed_json_key, index_key])

        t_zip = time.time()
        zip_key, zip_url = create_zip_from_keys(job_id, storage, files_to_zip)
        zip_elapsed = round(time.time() - t_zip, 2)

        logger.debug(
            "ZIP gerado",
            extra={
                **ctx,
                "action": "zip_created",
                "zip_key": zip_key,
                "files_zipped": len(files_to_zip),
                "elapsed_seconds": zip_elapsed,
            },
        )

        index["zip"] = {"key": zip_key, "url": zip_url}
        storage.upload_json(index_key, index)

        state.set(
            status=JobStatus.COMPLETED.value,
            message=f"Processamento concluído: {len(successful)}/{total_chunks} chunks",
            progress_pct=100,
            progress_pages=total_pages,
            processed_chunks=total_chunks,
            outputs={
                "index_key": index_key,
                "index_url": index_url,
                "total_chunks": total_chunks,
                "chunks_ok": len(successful),
                "chunks_failed": len(failed),
                "total_tokens_estimate": total_tokens,
                "total_tables": total_tables,
                "zip_key": zip_key,
                "zip_url": zip_url,
            },
        )

        logger.info(
            "Pipeline concluído com sucesso",
            extra={
                **ctx,
                "action": "pipeline_completed",
                "chunks_ok": len(successful),
                "chunks_failed": len(failed),
                "total_tokens_estimate": total_tokens,
                "total_tables": total_tables,
                "index_key": index_key,
                "zip_key": zip_key,
            },
        )
        return {"job_id": job_id, "index_key": index_key, "status": "completed"}

    except Exception as exc:
        logger.critical(
            "Falha crítica na finalização do pipeline",
            extra={
                **ctx,
                "action": "finalize_failed",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "chunks_ok": len(successful) if "successful" in dir() else "unknown",
            },
            exc_info=True,
        )
        if state:
            state.set_status(JobStatus.FAILED, f"Erro na finalização: {str(exc)}")
        raise
