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
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379")
    redis_job_ttl: int = int(os.getenv("REDIS_JOB_TTL", "86400"))
    REDIS_HOST: str = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    MAX_REQUESTS_PER_DAY: int = int(os.getenv("MAX_REQUESTS_PER_DAY", "2"))
    WINDOW_SECONDS: int = int(os.getenv("WINDOW_SECONDS", "86400"))

    # S3
    S3_endpoint: str = os.getenv("S3_ENDPOINT", "")
    S3_access_key: str = os.getenv("S3_ACCESS_KEY", "")
    S3_secret_key: str = os.getenv("S3_SECRET_KEY", "")
    S3_bucket: str = os.getenv("S3_BUCKET", "ocr-pipeline")
    S3_region: str = os.getenv("S3_REGION", "us-east-1")
    S3_secure: bool = os.getenv("S3_SECURE", "true").lower() == "true"

    # Pipeline
    chunk_size_pages: int = int(os.getenv("CHUNK_SIZE_PAGES", "10"))
    max_file_size_mb: int = int(os.getenv("MAX_FILE_SIZE_MB", "20"))
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
