"""Exceptions for the UWS storage service."""

from __future__ import annotations

from fastapi import status
from safir.fastapi import ClientRequestError
from safir.models import ErrorLocation

__all__ = ["UnknownJobError"]


class UnknownJobError(ClientRequestError):
    """The named job could not be found in the database."""

    error = "unknown_job"
    status_code = status.HTTP_404_NOT_FOUND

    def __init__(
        self,
        job_id: str,
        location: ErrorLocation | None = None,
        field_path: list[str] | None = None,
    ) -> None:
        super().__init__(f"Job {job_id} not found", location, field_path)
        self.job_id = job_id
