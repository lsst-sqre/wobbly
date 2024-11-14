"""Tests for the Wobbly database schema."""

from __future__ import annotations

import subprocess

import pytest
from safir.database import create_database_engine, unstamp_database

from wobbly.config import config
from wobbly.schema import SchemaBase


@pytest.mark.asyncio
async def test_schema() -> None:
    """Test for any unmanaged schema changes.

    Compare the current database schema in its SQLAlchemy ORM form against a
    dump of the SQL generated from the last known Alembic migration and ensure
    that Alembic doesn't detect any schema changes.
    """
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    async with engine.begin() as conn:
        await conn.run_sync(SchemaBase.metadata.drop_all)
    await unstamp_database(engine)
    await engine.dispose()
    subprocess.run(["alembic", "upgrade", "head"], check=True)
    subprocess.run(["alembic", "check"], check=True)
