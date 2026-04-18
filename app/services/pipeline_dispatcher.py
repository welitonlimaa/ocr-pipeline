"""
Abstração do dispatch do pipeline OCR.
"""

import threading
from app.config.logging_config import get_logger

logger = get_logger(__name__)


try:
    from workers.pipeline import process_document as _celery_task

    CELERY_AVAILABLE = True
except Exception:
    _celery_task = None
    CELERY_AVAILABLE = False
    logger.warning(
        "Celery não disponível — pipeline rodará em modo local (threads)",
        extra={"action": "celery_unavailable"},
    )


def dispatch(job_id: str, pdf_key: str) -> None:
    """
    Despacha o pipeline para um job.

    - Se Celery estiver disponível: enfileira via `.delay()`.
    - Caso contrário: executa em thread daemon local (desenvolvimento/teste).
    """
    if CELERY_AVAILABLE:
        _dispatch_celery(job_id, pdf_key)
    else:
        _dispatch_local(job_id, pdf_key)


def _dispatch_celery(job_id: str, pdf_key: str) -> None:
    _celery_task.delay(job_id, pdf_key)
    logger.info(
        "Task Celery despachada",
        extra={
            "action": "celery_task_dispatched",
            "job_id": job_id,
            "pdf_key": pdf_key,
        },
    )


def _dispatch_local(job_id: str, pdf_key: str) -> None:
    from workers.pipeline import _run_pipeline_local

    threading.Thread(
        target=_run_pipeline_local,
        args=(job_id, pdf_key),
        daemon=True,
    ).start()
    logger.info(
        "Pipeline iniciado em modo local (thread)",
        extra={"action": "local_thread_started", "job_id": job_id},
    )
