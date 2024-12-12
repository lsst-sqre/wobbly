"""Tests for the wobbly.handlers.internal module and routes."""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient
from safir.database import create_database_engine, drop_database

from wobbly.config import config
from wobbly.main import app
from wobbly.schema import SchemaBase


@pytest.mark.asyncio
async def test_health(client: AsyncClient) -> None:
    r = await client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "healthy"}

    # Force a health check failure by dropping the database, which should
    # produce database errors.
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    await drop_database(engine, SchemaBase.metadata)
    await engine.dispose()
    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="https://example.com/",
    ) as error_client:
        r = await error_client.get("/health")
        assert r.status_code == 500


@pytest.mark.asyncio
async def test_get_index(client: AsyncClient) -> None:
    """Test ``GET /``."""
    response = await client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == config.name
    assert isinstance(data["version"], str)
    assert isinstance(data["description"], str)
    assert isinstance(data["repository_url"], str)
    assert isinstance(data["documentation_url"], str)
