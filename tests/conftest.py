"""Test fixtures for wobbly tests."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest_asyncio
import structlog
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from safir.database import create_database_engine, initialize_database

from wobbly import main
from wobbly.config import config
from wobbly.schema import SchemaBase

__all__ = [
    "app",
    "client",
]


@pytest_asyncio.fixture
async def app() -> AsyncIterator[FastAPI]:
    """Return a configured test application.

    Wraps the application in a lifespan manager so that startup and shutdown
    events are sent during test execution.
    """
    logger = structlog.get_logger(__name__)
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    await initialize_database(
        engine, logger, schema=SchemaBase.metadata, reset=True
    )
    await engine.dispose()
    async with LifespanManager(main.app):
        yield main.app


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    """Return an ``httpx.AsyncClient`` configured to talk to the test app."""
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="https://example.com/"
    ) as client:
        yield client
