"""Request context dependency for FastAPI.

This dependency gathers a variety of information into a single object for the
convenience of writing request handlers. It also provides a place to store a
`structlog.BoundLogger` that can gather additional context during processing,
including from dependencies.
"""

from dataclasses import dataclass
from typing import Annotated, Any

from fastapi import Depends, Request
from safir.dependencies.db_session import db_session_dependency
from safir.dependencies.logger import logger_dependency
from safir.metrics import EventManager
from sqlalchemy.ext.asyncio import async_scoped_session
from structlog.stdlib import BoundLogger

from ..events import Events
from ..factory import Factory

__all__ = [
    "ContextDependency",
    "RequestContext",
    "context_dependency",
]


@dataclass(slots=True)
class RequestContext:
    """Holds the incoming request and its surrounding context.

    The primary reason for the existence of this class is to allow the
    functions involved in request processing to repeated rebind the request
    logger to include more information, without having to pass both the
    request and the logger separately to every function.
    """

    request: Request
    """The incoming request."""

    logger: BoundLogger
    """The request logger, rebound with discovered context."""

    session: async_scoped_session
    """The database session."""

    events: Events
    """Events publishers."""

    factory: Factory
    """The component factory."""

    def rebind_logger(self, **values: Any) -> None:
        """Add the given values to the logging context.

        Parameters
        ----------
        **values
            Additional values that should be added to the logging context.
        """
        self.logger = self.logger.bind(**values)
        self.factory.set_logger(self.logger)


class ContextDependency:
    """Provide a per-request context as a FastAPI dependency."""

    def __init__(self) -> None:
        self._events: Events

    async def __call__(
        self,
        *,
        request: Request,
        session: Annotated[
            async_scoped_session, Depends(db_session_dependency)
        ],
        logger: Annotated[BoundLogger, Depends(logger_dependency)],
    ) -> RequestContext:
        """Create a per-request context and return it."""
        return RequestContext(
            request=request,
            logger=logger,
            session=session,
            events=self._events,
            factory=Factory(session, self._events, logger),
        )

    async def initialize(self, event_manager: EventManager) -> None:
        """Initialize the process-wide shared context.

        Parameters
        ----------
        event_manager
            Global event manager.
        """
        self._events = Events()
        await self._events.initialize(event_manager)


context_dependency = ContextDependency()
"""The dependency that will return the per-request context."""
