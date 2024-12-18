"""Models for Wobbly."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Annotated, Self, override

from pydantic import BaseModel, Field
from safir.database import DatetimeIdCursor
from safir.metadata import Metadata as SafirMetadata
from safir.uws import (
    JobUpdateAborted,
    JobUpdateCompleted,
    JobUpdateError,
    JobUpdateExecuting,
    JobUpdateMetadata,
    JobUpdateQueued,
    SerializedJob,
)
from sqlalchemy.orm import InstrumentedAttribute
from vo_models.uws.types import ExecutionPhase

from .schema import Job as SQLJob

__all__ = [
    "HealthCheck",
    "HealthStatus",
    "Index",
    "JobCursor",
    "JobIdentifier",
    "JobSearch",
    "JobUpdate",
]


class HealthStatus(Enum):
    """Status of health check.

    Since errors are returned as HTTP 500 errors, currently the only status is
    the healthy status.
    """

    HEALTHY = "healthy"


class HealthCheck(BaseModel):
    """Results of an internal health check."""

    status: Annotated[HealthStatus, Field(title="Health status")]


class Index(BaseModel):
    """Metadata returned by the external root URL of the application."""

    metadata: SafirMetadata = Field(..., title="Package metadata")


@dataclass
class JobIdentifier:
    """Information required to identify a unique job.

    This always includes service information. Owner information is optional
    but enforced if present. In other words, if an owner is specified and the
    job exists but doesn't match that owner, it is treated as if it doesn't
    exist.
    """

    service: str
    """Service that owns the job."""

    id: str
    """Identifier of the job."""

    owner: str | None = None
    """User who owns the job."""


class JobCursor(DatetimeIdCursor[SerializedJob]):
    """Cursor for paginated lists of jobs."""

    @override
    @staticmethod
    def id_column() -> InstrumentedAttribute:
        return SQLJob.id

    @override
    @staticmethod
    def time_column() -> InstrumentedAttribute:
        return SQLJob.creation_time

    @override
    @classmethod
    def from_entry(
        cls, entry: SerializedJob, *, reverse: bool = False
    ) -> Self:
        return cls(
            id=int(entry.id), time=entry.creation_time, previous=reverse
        )


@dataclass
class JobSearch:
    """Collects common search parameters for jobs."""

    phases: set[ExecutionPhase] | None = None
    """Include only jobs in the given phases."""

    since: datetime | None = None
    """Include only jobs created after the given time."""

    cursor: JobCursor | None = None
    """Cursor for retrieving paginated results."""

    limit: int | None = None
    """Limit the number of jobs returned to at most this count."""


type JobUpdate = Annotated[
    JobUpdateAborted
    | JobUpdateCompleted
    | JobUpdateError
    | JobUpdateExecuting
    | JobUpdateQueued
    | JobUpdateMetadata,
    Field(title="Update to job"),
]
