"""SQLAlchemy schema for the UWS database."""

from __future__ import annotations

from .base import SchemaBase
from .job import Job
from .result import JobResult

__all__ = [
    "Job",
    "JobResult",
    "SchemaBase",
]
