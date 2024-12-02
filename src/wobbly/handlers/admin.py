"""Administrative routes.

These routes allow read-only access to job records for any service and user.
Write or delete access may be added later if needed. They should be protected
by an admin-only ``GafaelfawrIngress``.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, Response
from safir.models import ErrorLocation
from safir.slack.webhook import SlackRouteErrorHandler

from ..dependencies.context import RequestContext, context_dependency
from ..dependencies.search import job_search_dependency
from ..exceptions import UnknownJobError
from ..models import Job, JobIdentifier, JobSearch

__all__ = ["router"]

router = APIRouter(route_class=SlackRouteErrorHandler)
"""FastAPI router for all admin handlers."""


@router.get(
    "/admin/jobs",
    description="List jobs for any user or service",
    response_model_exclude_defaults=True,
    summary="List jobs",
    tags=["admin"],
)
async def list_jobs(
    *,
    search: Annotated[JobSearch, Depends(job_search_dependency)],
    context: Annotated[RequestContext, Depends(context_dependency)],
    response: Response,
) -> list[Job]:
    job_service = context.factory.create_job_service()
    results = await job_service.list_jobs(search)
    if search.cursor or search.limit:
        response.headers["Link"] = results.link_header(context.request.url)
    return results.entries


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
async def list_service_users(
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
async def list_service_user_jobs(
    service: str,
    user: str,
    *,
    search: Annotated[JobSearch, Depends(job_search_dependency)],
    context: Annotated[RequestContext, Depends(context_dependency)],
    response: Response,
) -> list[Job]:
    job_service = context.factory.create_job_service()
    results = await job_service.list_jobs(search, service, user)
    if search.cursor or search.limit:
        response.headers["Link"] = results.link_header(context.request.url)
    return results.entries


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


@router.get(
    "/admin/users",
    description="List users of any service with at least one job stored",
    summary="List users",
    tags=["admin"],
)
async def list_users(
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> list[str]:
    job_service = context.factory.create_job_service()
    return await job_service.list_users()


@router.get(
    "/admin/users/{user}/jobs",
    description="List jobs for a user",
    response_model_exclude_defaults=True,
    summary="List jobs",
    tags=["admin"],
)
async def list_user_jobs(
    user: str,
    *,
    search: Annotated[JobSearch, Depends(job_search_dependency)],
    context: Annotated[RequestContext, Depends(context_dependency)],
    response: Response,
) -> list[Job]:
    job_service = context.factory.create_job_service()
    results = await job_service.list_jobs(search, user=user)
    if search.cursor or search.limit:
        response.headers["Link"] = results.link_header(context.request.url)
    return results.entries
