"""
Factory da aplicação FastAPI.

Responsável exclusivamente por:
  - Instanciar o app
  - Registrar middlewares
  - Montar os routers

Nenhuma lógica de negócio ou acesso a infraestrutura aqui.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config.settings import settings
from app.config.logging_config import configure_logging
from app.api.middleware import log_requests
from app.api.routers import health, jobs

configure_logging()


def create_app() -> FastAPI:
    app = FastAPI(
        title="OCR Pipeline API",
        description="Pipeline desacoplado de OCR para PDFs grandes com S3 + Redis + Celery",
        version="1.0.0",
    )

    _register_middlewares(app)
    _register_routers(app)

    return app


def _register_middlewares(app: FastAPI) -> None:
    allow_origins = settings.CORS_ORIGINS
    allow_credentials = allow_origins != ["*"]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.middleware("http")(log_requests)


def _register_routers(app: FastAPI) -> None:
    app.include_router(health.router)
    app.include_router(jobs.router)


app = create_app()
