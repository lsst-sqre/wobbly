"""SQLAlchemy schema for errors for a UWS job."""

from __future__ import annotations

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
from vo_models.uws.types import ErrorType

from .base import SchemaBase

__all__ = ["JobError"]


class JobError(SchemaBase):
    """Table holding UWS job errors."""

    __tablename__ = "job_error"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    job_id: Mapped[int] = mapped_column(
        ForeignKey("job.id", ondelete="CASCADE")
    )
    type: Mapped[ErrorType | None]
    code: Mapped[str | None]
    message: Mapped[str | None]
    detail: Mapped[str | None]
