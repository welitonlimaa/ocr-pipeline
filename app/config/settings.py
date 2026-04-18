import os
from dataclasses import dataclass, field


def get_cors_origins():
    raw = os.getenv("CORS_ORIGINS", "*")
    if raw == "*":
        return ["*"]
    return [origin.strip() for origin in raw.split(",")]


@dataclass
class Settings:
    # Redis
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379")
    REDIS_JOB_TTL: int = int(os.getenv("REDIS_JOB_TTL", "86400"))
    REDIS_HOST: str = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    MAX_REQUESTS_PER_DAY: int = int(os.getenv("MAX_REQUESTS_PER_DAY", "2"))
    WINDOW_SECONDS: int = int(os.getenv("WINDOW_SECONDS", "86400"))

    # S3
    S3_ENDPOINT: str = os.getenv("S3_ENDPOINT", "")
    S3_ACCESS_KEY: str = os.getenv("S3_ACCESS_KEY", "")
    S3_SECRET_KEY: str = os.getenv("S3_SECRET_KEY", "")
    S3_BUCKET: str = os.getenv("S3_BUCKET", "ocr-pipeline")
    S3_REGION: str = os.getenv("S3_REGION", "us-east-1")
    S3_SECURE: bool = os.getenv("S3_SECURE", "true").lower() == "true"

    # Pipeline
    CHUNK_SIZE_PAGES: int = int(os.getenv("CHUNK_SIZE_PAGES", "10"))
    MAX_FILE_SIZE_MB: int = int(os.getenv("MAX_FILE_SIZE_MB", "20"))
    MAX_PAGES: int = int(os.getenv("MAX_PAGES", "150"))

    # Celery
    celery_broker: str = os.getenv(
        "CELERY_BROKER", os.getenv("REDIS_URL", "redis://localhost:6379/0")
    )
    celery_backend: str = os.getenv(
        "CELERY_BACKEND", os.getenv("REDIS_URL", "redis://localhost:6379/1")
    )

    CORS_ORIGINS: list = field(default_factory=get_cors_origins)


settings = Settings()
