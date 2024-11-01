"""Component factory for Wobbly."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import async_scoped_session
from structlog.stdlib import BoundLogger

from .service import JobService
from .storage import JobStorage

__all__ = ["Factory"]


class Factory:
    """Component factory for the UWS storage service.

    Parameters
    ----------
    session
        Database session.
    logger
        Logger to use.
    """

    def __init__(
        self, session: async_scoped_session, logger: BoundLogger
    ) -> None:
        self._session = session
        self._logger = logger

    def create_job_service(self) -> JobService:
        """Create a job service.

        Returns
        -------
        JobService
            Newly-created job service.
        """
        return JobService(JobStorage(self._session), self._logger)
