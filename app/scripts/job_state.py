import json
import uuid
from datetime import datetime
from enum import Enum
from typing import Optional

import redis

from app.config.settings import settings
from app.config.logging_config import get_logger

logger = get_logger(__name__)


class JobStatus(str, Enum):
    QUEUED = "queued"
    UPLOADING = "uploading"
    PROCESSING = "processing"
    EXTRACTING = "extracting"
    INDEXING = "indexing"
    COMPLETED = "completed"
    FAILED = "failed"


class JobState:
    """Interface para leitura/escrita do estado de um job no Redis."""

    def __init__(self, job_id: str, r: redis.Redis):
        self.job_id = job_id
        self._r = r
        self._key = f"job:{job_id}"

    def set(self, **fields):
        """Atualiza campos do job (merge parcial)."""
        try:
            current = self._load()
            current.update(fields)
            current["updated_at"] = datetime.utcnow().isoformat()
            self._r.setex(
                self._key,
                settings.REDIS_JOB_TTL,
                json.dumps(current, ensure_ascii=False),
            )
        except redis.RedisError as exc:
            logger.error(
                "Falha ao atualizar estado do job no Redis",
                extra={
                    "action": "redis_set_failed",
                    "job_id": self.job_id,
                    "fields": list(fields.keys()),
                    "error": str(exc),
                },
                exc_info=True,
            )
            raise

    def set_status(self, status: JobStatus, message: str = ""):
        prev = self._load().get("status", "unknown")
        self.set(status=status.value, message=message)
        logger.info(
            "Status do job atualizado",
            extra={
                "action": "job_status_change",
                "job_id": self.job_id,
                "status_from": prev,
                "status_to": status.value,
                "status_message": message,
            },
        )

    def set_progress(self, current_page: int, total_pages: int):
        pct = round((current_page / total_pages) * 100, 1) if total_pages else 0
        self.set(
            progress_pages=current_page,
            total_pages=total_pages,
            progress_pct=pct,
        )

    def increment_progress_chunks(self):
        try:
            current = self._r.incr(f"job:{self.job_id}:processed_chunks")
        except redis.RedisError as exc:
            logger.error(
                "Falha ao incrementar contador de chunks no Redis",
                extra={
                    "action": "redis_incr_failed",
                    "job_id": self.job_id,
                    "error": str(exc),
                },
                exc_info=True,
            )
            raise

        total = self.get().get("total_chunks", 1)
        pct = min(round((current / total) * 100, 1), 99.0)

        self.set(processed_chunks=current, progress_pct=pct)

        logger.debug(
            "Progresso de chunks atualizado",
            extra={
                "action": "chunk_progress_update",
                "job_id": self.job_id,
                "processed_chunks": current,
                "total_chunks": total,
                "progress_pct": pct,
            },
        )

    def add_chunk_result(self, chunk_index: int, result: dict):
        """Registra resultado de um chunk processado."""
        chunk_key = f"job:{self.job_id}:chunk:{chunk_index}"
        done_key = f"job:{self.job_id}:done_chunks"
        try:
            self._r.setex(
                chunk_key,
                settings.REDIS_JOB_TTL,
                json.dumps(result, ensure_ascii=False),
            )
            self._r.rpush(done_key, chunk_index)
            self._r.expire(done_key, settings.REDIS_JOB_TTL)
        except redis.RedisError as exc:
            logger.error(
                "Falha ao registrar resultado de chunk no Redis",
                extra={
                    "action": "redis_chunk_result_failed",
                    "job_id": self.job_id,
                    "chunk_index": chunk_index,
                    "chunk_status": result.get("status"),
                    "error": str(exc),
                },
                exc_info=True,
            )
            raise

    def get(self) -> dict:
        return self._load()

    def get_chunk(self, chunk_index: int) -> Optional[dict]:
        try:
            raw = self._r.get(f"job:{self.job_id}:chunk:{chunk_index}")
            return json.loads(raw) if raw else None
        except (redis.RedisError, json.JSONDecodeError) as exc:
            logger.error(
                "Falha ao ler chunk do Redis",
                extra={
                    "action": "redis_get_chunk_failed",
                    "job_id": self.job_id,
                    "chunk_index": chunk_index,
                    "error": str(exc),
                },
                exc_info=True,
            )
            return None

    def _load(self) -> dict:
        try:
            raw = self._r.get(self._key)
            return json.loads(raw) if raw else {}
        except (redis.RedisError, json.JSONDecodeError) as exc:
            logger.error(
                "Falha ao ler estado do job no Redis",
                extra={
                    "action": "redis_load_failed",
                    "job_id": self.job_id,
                    "error": str(exc),
                },
                exc_info=True,
            )
            return {}

    def exists(self) -> bool:
        try:
            return bool(self._r.exists(self._key))
        except redis.RedisError as exc:
            logger.error(
                "Falha ao verificar existência de job no Redis",
                extra={
                    "action": "redis_exists_failed",
                    "job_id": self.job_id,
                    "error": str(exc),
                },
                exc_info=True,
            )
            return False


class JobRegistry:
    """Fábrica e repositório de jobs."""

    def __init__(self):
        try:
            self._r = redis.from_url(settings.REDIS_URL, decode_responses=True)
            self._r.ping()
            logger.info(
                "Conexão com Redis estabelecida",
                extra={"action": "redis_connected", "url": settings.REDIS_URL},
            )
        except redis.RedisError as exc:
            logger.critical(
                "Falha ao conectar ao Redis na inicialização",
                extra={
                    "action": "redis_connect_failed",
                    "url": settings.REDIS_URL,
                    "error": str(exc),
                },
                exc_info=True,
            )
            raise

    def create(self, filename: str, metadata: dict = None) -> JobState:
        job_id = str(uuid.uuid4())
        state = JobState(job_id, self._r)
        state.set(
            job_id=job_id,
            filename=filename,
            status=JobStatus.QUEUED.value,
            message="Job criado, aguardando processamento",
            progress_pct=0,
            progress_pages=0,
            processed_chunks=0,
            total_pages=0,
            total_chunks=0,
            created_at=datetime.utcnow().isoformat(),
            updated_at=datetime.utcnow().isoformat(),
            metadata=metadata or {},
            outputs={},
        )

        try:
            self._r.lpush("jobs:all", job_id)
        except redis.RedisError as exc:
            logger.warning(
                "Falha ao registrar job na lista global jobs:all",
                extra={
                    "action": "jobs_list_push_failed",
                    "job_id": job_id,
                    "error": str(exc),
                },
            )

        logger.info(
            "Job registrado no Redis",
            extra={
                "action": "job_registered",
                "job_id": job_id,
                "file_name": filename,
            },
        )
        return state

    def get(self, job_id: str) -> Optional[JobState]:
        state = JobState(job_id, self._r)
        return state if state.exists() else None

    def get_redis(self) -> redis.Redis:
        return self._r


registry = JobRegistry()
