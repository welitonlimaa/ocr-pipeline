"""
Gerenciamento de estado dos jobs via Redis.
Cada job tem um estado rastreado em tempo real com progresso por página.
"""

import json
import uuid
from datetime import datetime
from enum import Enum
from typing import Optional

import redis

from app.config.settings import settings


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
        current = self._load()
        current.update(fields)
        current["updated_at"] = datetime.utcnow().isoformat()
        self._r.setex(
            self._key,
            settings.redis_job_ttl,
            json.dumps(current, ensure_ascii=False),
        )

    def set_status(self, status: JobStatus, message: str = ""):
        self.set(status=status.value, message=message)

    def set_progress(self, current_page: int, total_pages: int):
        pct = round((current_page / total_pages) * 100, 1) if total_pages else 0
        self.set(
            progress_pages=current_page,
            total_pages=total_pages,
            progress_pct=pct,
        )

    def increment_progress_chunks(self):
        current = self._r.incr(f"job:{self.job_id}:processed_chunks")

        total = self.get().get("total_chunks", 1)

        pct = min(round((current / total) * 100, 1), 99.0)

        self.set(
            processed_chunks=current,
            progress_pct=pct,
        )

    def add_chunk_result(self, chunk_index: int, result: dict):
        """Registra resultado de um chunk processado."""
        key = f"job:{self.job_id}:chunk:{chunk_index}"
        self._r.setex(
            key, settings.redis_job_ttl, json.dumps(result, ensure_ascii=False)
        )

        done_key = f"job:{self.job_id}:done_chunks"
        self._r.rpush(done_key, chunk_index)
        self._r.expire(done_key, settings.redis_job_ttl)

    def get(self) -> dict:
        return self._load()

    def get_chunk(self, chunk_index: int) -> Optional[dict]:
        raw = self._r.get(f"job:{self.job_id}:chunk:{chunk_index}")
        return json.loads(raw) if raw else None

    def _load(self) -> dict:
        raw = self._r.get(self._key)
        return json.loads(raw) if raw else {}

    def exists(self) -> bool:
        return bool(self._r.exists(self._key))


class JobRegistry:
    """Fábrica e repositório de jobs."""

    def __init__(self):
        self._r = redis.from_url(settings.redis_url, decode_responses=True)

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

        self._r.lpush("jobs:all", job_id)
        return state

    def get(self, job_id: str) -> Optional[JobState]:
        state = JobState(job_id, self._r)
        return state if state.exists() else None

    def get_redis(self) -> redis.Redis:
        return self._r


registry = JobRegistry()
