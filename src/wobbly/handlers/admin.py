"""Administrative routes.

These routes allow read-only access to job records for any service and user.
Write or delete access may be added later if needed. They should be protected
by an admin-only ``GafaelfawrIngress``.
"""

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from safir.models import ErrorLocation
from safir.slack.webhook import SlackRouteErrorHandler
from vo_models.uws.types import ExecutionPhase

from ..dependencies.context import RequestContext, context_dependency
from ..exceptions import UnknownJobError
from ..models import Job, JobIdentifier

__all__ = ["router"]

router = APIRouter(route_class=SlackRouteErrorHandler)
"""FastAPI router for all admin handlers."""


@router.get(
    "/admin/services",
    description="List services with at least one job stored",
    summary="List services",
    tags=["admin"],
)
async def list_services(
    *,
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> list[str]:
    job_service = context.factory.create_job_service()
    return await job_service.list_services()


@router.get(
    "/admin/services/{service}/users",
    description="List users with at least one job stored",
    summary="List users",
    tags=["admin"],
)
async def list_users(
    service: str,
    *,
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> list[str]:
    job_service = context.factory.create_job_service()
    return await job_service.list_users(service)


@router.get(
    "/admin/services/{service}/users/{user}/jobs",
    description="List jobs for a user and service",
    response_model_exclude_defaults=True,
    summary="List jobs",
    tags=["admin"],
)
async def list_jobs(
    service: str,
    user: str,
    *,
    phase: Annotated[
        list[ExecutionPhase] | None,
        Query(
            title="Execution phase",
            description="Limit results to the provided execution phases",
        ),
    ] = None,
    after: Annotated[
        datetime | None,
        Query(
            title="Creation date",
            description="Limit results to jobs created after this date",
        ),
    ] = None,
    count: Annotated[
        int | None,
        Query(
            title="Number of jobs",
            description="Return at most the given number of jobs",
        ),
    ] = None,
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> list[Job]:
    job_service = context.factory.create_job_service()
    return await job_service.list_jobs(
        service, user, phases=phase, after=after, count=count
    )


@router.get(
    "/admin/services/{service}/users/{user}/jobs/{job_id}",
    description="Retrieve the record for a single job",
    response_model_exclude_defaults=True,
    summary="Get job",
)
async def get_job(
    service: str,
    user: str,
    job_id: str,
    *,
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> Job:
    job_service = context.factory.create_job_service()
    identifier = JobIdentifier(service=service, owner=user, id=job_id)
    try:
        return await job_service.get(identifier)
    except UnknownJobError as e:
        e.location = ErrorLocation.path
        e.field_path = ["job_id"]
        raise
