"""Component factory for Wobbly."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import async_scoped_session
from structlog.stdlib import BoundLogger

from .events import Events
from .service import JobService
from .storage import JobStore

__all__ = ["Factory"]


class Factory:
    """Component factory for the UWS storage service.

    Parameters
    ----------
    session
        Database session.
    events
        Event publishers.
    logger
        Logger to use.
    """

    def __init__(
        self,
        session: async_scoped_session,
        events: Events,
        logger: BoundLogger,
    ) -> None:
        self._session = session
        self._events = events
        self._logger = logger

    def create_job_service(self) -> JobService:
        """Create a job service.

        Returns
        -------
        JobService
            Newly-created job service.
        """
        return JobService(JobStore(self._session), self._events, self._logger)

    def set_logger(self, logger: BoundLogger) -> None:
        """Replace the internal logger.

        Used by the context dependency to update the logger for all
        newly-created components when it's rebound with additional context.

        Parameters
        ----------
        logger
            New logger.
        """
        self._logger = logger
