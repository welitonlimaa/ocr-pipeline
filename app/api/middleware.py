import time
from fastapi import Request
from app.config.logging_config import get_logger

logger = get_logger(__name__)


async def log_requests(request: Request, call_next):
    """Loga todas as requisições HTTP com método, path, status e latência."""
    t0 = time.time()
    response = await call_next(request)
    elapsed = round(time.time() - t0, 3)

    log = logger.warning if response.status_code >= 400 else logger.info
    log(
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
