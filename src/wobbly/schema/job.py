"""SQLAlchemy schema for UWS jobs."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from vo_models.uws.types import ExecutionPhase

from .base import SchemaBase
from .error import JobError
from .result import JobResult

__all__ = ["Job"]


class Job(SchemaBase):
    """Table holding UWS jobs.

    This table is shared by all clients of Wobbly, so every job has to be
    tagged with the service that owns that job. Apart from that, this is the
    standard UWS schema from the IVOA UWS standard.
    """

    __tablename__ = "job"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    service: Mapped[str]
    owner: Mapped[str]
    phase: Mapped[ExecutionPhase]
    run_id: Mapped[str | None]
    json_parameters: Mapped[dict[str, Any]] = mapped_column(JSONB)
    message_id: Mapped[str | None]
    creation_time: Mapped[datetime]
    start_time: Mapped[datetime | None]
    end_time: Mapped[datetime | None]
    destruction_time: Mapped[datetime]
    execution_duration: Mapped[int | None]
    quote: Mapped[datetime | None]

    # The details of how these relationships are defined were chosen to allow
    # this schema to be used with async SQLAlchemy. Review the SQLAlchemy
    # asyncio documentation carefully before making changes here. There are a
    # lot of surprises and sharp edges.
    errors: Mapped[list[JobError]] = relationship(
        cascade="save-update, merge, delete, delete-orphan", lazy="selectin"
    )
    results: Mapped[list[JobResult]] = relationship(
        cascade="save-update, merge, delete, delete-orphan",
        lazy="selectin",
        order_by=JobResult.sequence,
    )

    __table_args__ = (
        Index(
            "by_service_owner_phase",
            "service",
            "owner",
            "phase",
            "creation_time",
        ),
        Index("by_service_owner_time", "service", "owner", "creation_time"),
    )
