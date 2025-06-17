"""Tests for the Wobbly command-line interface.

Be careful when writing tests in this framework because the click command
handling code spawns its own async worker pools when needed. None of these
tests can therefore be async, and should instead run coroutines by creating an
event loop when needed.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path

import structlog
from click.testing import CliRunner
from safir.database import (
    create_database_engine,
    initialize_database,
    stamp_database_async,
)
from safir.datetime import current_datetime
from safir.uws import JobCreate

from wobbly.cli import main
from wobbly.config import config
from wobbly.factory import Factory
from wobbly.models import JobSearch
from wobbly.schema import SchemaBase


def test_expire() -> None:
    event_loop = asyncio.new_event_loop()
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    logger = structlog.get_logger(__name__)
    job_create_one = JobCreate(
        json_parameters={}, destruction_time=current_datetime()
    )
    job_create_two = JobCreate(
        json_parameters={},
        destruction_time=current_datetime() + timedelta(days=30),
    )

    async def setup() -> None:
        await initialize_database(
            engine, logger, schema=SchemaBase.metadata, reset=True
        )
        await stamp_database_async(engine)
        async with Factory.standalone(engine, logger) as factory:
            job_service = factory.create_job_service()
            await job_service.create("service", "owner", job_create_one)
            await job_service.create("service", "owner", job_create_two)
            jobs = await job_service.list_jobs(JobSearch())
            assert len(jobs.entries) == 2

    event_loop.run_until_complete(setup())
    runner = CliRunner()
    alembic_config_path = str(Path(__file__).parent.parent / "alembic.ini")
    result = runner.invoke(
        main,
        ["expire", "--alembic-config-path", alembic_config_path],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    async def check() -> None:
        async with Factory.standalone(engine, logger) as factory:
            job_service = factory.create_job_service()
            jobs = await job_service.list_jobs(JobSearch())
            assert len(jobs.entries) == 1
            seen = jobs.entries[0].destruction_time
            assert seen == job_create_two.destruction_time
        await engine.dispose()

    event_loop.run_until_complete(check())
