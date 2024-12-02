"""The main application factory for the Wobbly service.

Notes
-----
Be aware that, following the normal pattern for FastAPI services, the app is
constructed when this module is loaded and is not deferred until a function is
called.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.metadata import metadata, version

import structlog
from fastapi import FastAPI
from safir.database import create_database_engine, is_database_current
from safir.dependencies.db_session import db_session_dependency
from safir.fastapi import ClientRequestError, client_request_error_handler
from safir.logging import configure_logging, configure_uvicorn_logging
from safir.middleware.x_forwarded import XForwardedMiddleware
from safir.slack.webhook import SlackRouteErrorHandler

from .config import config
from .handlers import admin, internal, service

__all__ = ["app"]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Set up and tear down the application."""
    logger = structlog.get_logger("wobbly")
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    if not await is_database_current(engine, logger):
        raise RuntimeError("Database schema out of date")
    await engine.dispose()
    await db_session_dependency.initialize(
        config.database_url,
        config.database_password,
        isolation_level="REPEATABLE READ",
    )

    yield

    await db_session_dependency.aclose()


configure_logging(
    profile=config.profile,
    log_level=config.log_level,
    name="wobbly",
)
configure_uvicorn_logging(config.log_level)

app = FastAPI(
    title="Wobbly",
    description=metadata("wobbly")["Summary"],
    version=version("wobbly"),
    openapi_url=f"{config.path_prefix}/openapi.json",
    docs_url=f"{config.path_prefix}/docs",
    redoc_url=f"{config.path_prefix}/redoc",
    lifespan=lifespan,
)
"""The main FastAPI application for wobbly."""

# Attach the routers.
app.include_router(internal.router)
app.include_router(admin.router, prefix=config.path_prefix)
app.include_router(service.router, prefix=config.path_prefix)

# Add middleware.
app.add_middleware(XForwardedMiddleware)

# Add exception handlers.
app.exception_handler(ClientRequestError)(client_request_error_handler)

# Configure Slack alerts.
if config.slack_webhook:
    logger = structlog.get_logger("wobbly")
    SlackRouteErrorHandler.initialize(config.slack_webhook, "wobbly", logger)
    logger.debug("Initialized Slack webhook")
