"""SQLAlchemy schema for UWS jobs."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from vo_models.uws.types import ErrorType, ExecutionPhase

from ..models import JobParameters
from .base import SchemaBase
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
    service: Mapped[str] = mapped_column(String(64))
    owner: Mapped[str] = mapped_column(String(64))
    phase: Mapped[ExecutionPhase]
    run_id: Mapped[str | None] = mapped_column(String(64))
    parameters: Mapped[JobParameters] = mapped_column(JSONB)
    message_id: Mapped[str | None] = mapped_column(String(64))
    creation_time: Mapped[datetime]
    start_time: Mapped[datetime | None]
    end_time: Mapped[datetime | None]
    destruction_time: Mapped[datetime]
    execution_duration: Mapped[int | None]
    quote: Mapped[datetime | None]
    error_type: Mapped[ErrorType | None]
    error_code: Mapped[str | None]
    error_message: Mapped[str | None] = mapped_column(Text)
    error_detail: Mapped[str | None] = mapped_column(Text)

    # The details of how these relationship is defined was chosen to allow
    # this schema to be used with async SQLAlchemy. Review the SQLAlchemy
    # asyncio documentation carefully before making changes here. There are a
    # lot of surprises and sharp edges.
    results: Mapped[list[JobResult]] = relationship(
        cascade="delete", lazy="selectin", uselist=True
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
