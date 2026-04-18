"""
Schemas Pydantic para contratos de request e response da API.
"""

from typing import Any, Optional
from pydantic import BaseModel


class TrackingInfo(BaseModel):
    status_url: str
    index_url: Optional[str] = None
    storage_path: Optional[str] = None


class JobSubmitResponse(BaseModel):
    job_id: str
    status: str
    message: str
    tracking: TrackingInfo
    metadata: dict[str, Any]


class JobKeySubmitResponse(BaseModel):
    job_id: str
    status: str
    message: str
    tracking: TrackingInfo


class JobStatusResponse(BaseModel):
    job_id: str
    status: Optional[str]
    message: Optional[str]
    progress_pct: float = 0
    progress_pages: int = 0
    total_pages: int = 0
    created_at: Optional[str]
    updated_at: Optional[str]
    outputs: dict[str, Any] = {}
    metadata: dict[str, Any] = {}


class JobPendingResponse(BaseModel):
    job_id: str
    status: Optional[str]
    message: str
    progress_pct: float = 0


class HealthResponse(BaseModel):
    status: str
    redis: str
    s3: str
    celery: str
