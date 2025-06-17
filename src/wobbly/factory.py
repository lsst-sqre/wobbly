"""Component factory for Wobbly."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Self

from safir.database import create_async_session
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_scoped_session
from structlog.stdlib import BoundLogger

from .config import config
from .events import Events
from .schema import Job as SQLJob
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

    @classmethod
    @asynccontextmanager
    async def standalone(
        cls, engine: AsyncEngine, logger: BoundLogger
    ) -> AsyncGenerator[Self]:
        """Async context manager for Wobbly components.

        Intended for background jobs.

        Parameters
        ----------
        engine
            Database engine.
        logger
            Logger to use.

        Yields
        ------
        Factory
            The factory. Must be used as an async context manager.
        """
        stmt = select(SQLJob)
        session = await create_async_session(engine, statement=stmt)
        event_manager = config.metrics.make_manager()
        await event_manager.initialize()
        events = Events()
        await events.initialize(event_manager)

        try:
            yield cls(session, events, logger)
        finally:
            await session.remove()
            await event_manager.aclose()

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
