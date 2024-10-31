"""Administrative command-line interface."""

from __future__ import annotations

import click
import structlog
from safir.asyncio import run_with_asyncio
from safir.click import display_help
from safir.database import create_database_engine, initialize_database

from .config import config
from .schema import SchemaBase

__all__ = [
    "help",
    "init",
    "main",
]


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(message="%(version)s")
def main() -> None:
    """Administrative command-line interface for wobbly."""


@main.command()
@click.argument("topic", default=None, required=False, nargs=1)
@click.pass_context
def help(ctx: click.Context, topic: str | None) -> None:
    """Show help for any command."""
    display_help(main, ctx, topic)


@main.command()
@click.option(
    "--reset", is_flag=True, help="Delete all existing database data."
)
@run_with_asyncio
async def init(*, reset: bool) -> None:
    """Initialize the database storage."""
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    logger = structlog.get_logger("wobbly")
    await initialize_database(
        engine, logger, schema=SchemaBase.metadata, reset=reset
    )
    await engine.dispose()
