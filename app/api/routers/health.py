from fastapi import APIRouter

from app.scripts.job_state import registry
from app.scripts.storage import storage
from app.config.settings import settings
from app.api.schemas import HealthResponse
from app.services.pipeline_dispatcher import CELERY_AVAILABLE
from app.config.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    """Health check dos componentes críticos: Redis, S3 e Celery."""
    redis_ok = _check_redis()
    s3_ok = _check_s3()

    overall = "ok" if (redis_ok and s3_ok) else "degraded"

    if overall == "degraded":
        logger.warning(
            "Health check degraded",
            extra={"action": "health_degraded", "redis": redis_ok, "s3": s3_ok},
        )

    return HealthResponse(
        status=overall,
        redis="ok" if redis_ok else "error",
        s3="ok" if s3_ok else "error",
        celery="ok" if CELERY_AVAILABLE else "unavailable (modo local)",
    )


def _check_redis() -> bool:
    try:
        registry.get_redis().ping()
        return True
    except Exception as exc:
        logger.error(
            "Health check: Redis indisponível",
            extra={"action": "health_redis_fail", "error": str(exc)},
        )
        return False


def _check_s3() -> bool:
    try:
        storage.client.head_bucket(Bucket=settings.S3_BUCKET)
        return True
    except Exception as exc:
        logger.error(
            "Health check: S3 indisponível",
            extra={"action": "health_s3_fail", "error": str(exc)},
        )
        return False
