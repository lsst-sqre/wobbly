"""Routes used by other services.

These routes should be protected by a service-only ``GafaelfawrIngress`` and
thus only be accessible to other services using delegated credentials. The
service and username can then be extracted from the header information added
by Gafaelfawr and used to limit the view of the UWS database accessible to the
service.
"""

from typing import Annotated

from fastapi import APIRouter, Body, Depends, Header, Path, Response
from safir.dependencies.gafaelfawr import auth_dependency
from safir.models import ErrorLocation
from safir.slack.webhook import SlackRouteErrorHandler
from safir.uws import JobCreate, SerializedJob

from ..dependencies.context import RequestContext, context_dependency
from ..dependencies.search import job_search_dependency
from ..exceptions import UnknownJobError
from ..models import JobIdentifier, JobSearch, JobUpdate

__all__ = ["router"]

router = APIRouter(route_class=SlackRouteErrorHandler)
"""FastAPI router for all internal handlers."""


async def auth_service_dependency(
    x_auth_request_service: Annotated[str, Header(include_in_schema=False)],
) -> str:
    """Get the authenticated service from the Gaelfawr headers.

    Returns
    -------
    str
        Identifier of the service making the request.
    """
    return x_auth_request_service


async def job_identifier_dependency(
    *,
    service: Annotated[str, Depends(auth_service_dependency)],
    user: Annotated[str, Depends(auth_dependency)],
    job_id: Annotated[str, Path(title="Job ID")],
) -> JobIdentifier:
    return JobIdentifier(service=service, owner=user, id=job_id)


@router.get(
    "/jobs",
    description="List the jobs for the authenticated user",
    response_model_exclude_defaults=True,
    summary="List jobs",
)
async def list_jobs(
    *,
    service: Annotated[str, Depends(auth_service_dependency)],
    user: Annotated[str, Depends(auth_dependency)],
    search: Annotated[JobSearch, Depends(job_search_dependency)],
    context: Annotated[RequestContext, Depends(context_dependency)],
    response: Response,
) -> list[SerializedJob]:
    job_service = context.factory.create_job_service()
    results = await job_service.list_jobs(search, service, user)
    if search.cursor or search.limit:
        response.headers["Link"] = results.link_header(context.request.url)
    return results.entries


@router.post(
    "/jobs",
    description="Create a new job for the authenticated user",
    response_model_exclude_defaults=True,
    status_code=201,
    summary="Create job",
)
async def create_job(
    *,
    service: Annotated[str, Depends(auth_service_dependency)],
    user: Annotated[str, Depends(auth_dependency)],
    job_data: JobCreate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    response: Response,
) -> SerializedJob:
    job_service = context.factory.create_job_service()
    job = await job_service.create(service, user, job_data)
    url = context.request.url_for("get_job", job_id=job.id)
    response.headers["Location"] = str(url)
    return job


@router.get(
    "/jobs/{job_id}",
    description="Retrieve the record for a single job",
    response_model_exclude_defaults=True,
    summary="Get job",
)
async def get_job(
    *,
    job_id: Annotated[JobIdentifier, Depends(job_identifier_dependency)],
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> SerializedJob:
    job_service = context.factory.create_job_service()
    try:
        return await job_service.get(job_id)
    except UnknownJobError as e:
        e.location = ErrorLocation.path
        e.field_path = ["job_id"]
        raise


@router.delete(
    "/jobs/{job_id}",
    description="Delete the record for a single job",
    status_code=204,
    summary="Delete job",
)
async def delete_job(
    *,
    job_id: Annotated[JobIdentifier, Depends(job_identifier_dependency)],
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> None:
    job_service = context.factory.create_job_service()
    try:
        await job_service.delete(job_id)
    except UnknownJobError as e:
        e.location = ErrorLocation.path
        e.field_path = ["job_id"]
        raise


@router.patch(
    "/jobs/{job_id}",
    description="Update the record for a single job",
    response_model_exclude_defaults=True,
    summary="Update job",
)
async def patch_job(
    *,
    job_id: Annotated[JobIdentifier, Depends(job_identifier_dependency)],
    update: Annotated[JobUpdate, Body()],
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> SerializedJob:
    job_service = context.factory.create_job_service()
    try:
        return await job_service.update(job_id, update)
    except UnknownJobError as e:
        e.location = ErrorLocation.path
        e.field_path = ["job_id"]
        raise
