"""SQLAlchemy schema for results for a UWS job."""

from __future__ import annotations

from sqlalchemy import ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column

from .base import SchemaBase

__all__ = ["JobResult"]


class JobResult(SchemaBase):
    """Table holding UWS job results."""

    __tablename__ = "job_result"

    job_id: Mapped[int] = mapped_column(
        ForeignKey("job.id", ondelete="CASCADE"), primary_key=True
    )
    id: Mapped[str] = mapped_column(primary_key=True)
    sequence: Mapped[int]
    url: Mapped[str]
    size: Mapped[int | None]
    mime_type: Mapped[str | None]

    __table_args__ = (Index("by_sequence", "job_id", "sequence", unique=True),)
