"""Dependency collecting common parameters for a job search."""

from typing import Annotated

from fastapi import Query
from safir.pydantic import UtcDatetime
from vo_models.uws.types import ExecutionPhase

from ..models import JobCursor, JobSearch

__all__ = ["job_search_dependency"]


async def job_search_dependency(
    *,
    phase: Annotated[
        list[ExecutionPhase] | None,
        Query(
            title="Execution phase",
            description="Limit results to the provided execution phases",
        ),
    ] = None,
    since: Annotated[
        UtcDatetime | None,
        Query(
            title="Creation date",
            description="Limit results to jobs created after this date",
        ),
    ] = None,
    cursor: Annotated[
        str | None,
        Query(
            title="Pagination cursor",
            description="Cursor used when moving between pages of results",
        ),
    ] = None,
    limit: Annotated[
        int | None,
        Query(
            title="Number of jobs",
            description="Return at most the given number of jobs",
        ),
    ] = None,
) -> JobSearch:
    """Collect common search parameters for a job."""
    return JobSearch(
        phases=set(phase) if phase else None,
        since=since,
        cursor=JobCursor.from_str(cursor) if cursor else None,
        limit=limit,
    )
