"""Routes used by other services.

These routes should be protected by a service-only ``GafaelfawrIngress`` and
thus only be accessible to other services using delegated credentials. The
service and username can then be extracted from the header information added
by Gafaelfawr and used to limit the view of the UWS database accessible to the
service.
"""

from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, Path, Query, Response
from safir.dependencies.gafaelfawr import auth_dependency
from safir.models import ErrorLocation
from vo_models.uws.types import ExecutionPhase

from ..dependencies.context import RequestContext, context_dependency
from ..exceptions import UnknownJobError
from ..models import Job, JobCreate, JobIdentifier, JobUpdate

__all__ = ["router"]

router = APIRouter()
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
    response_model=list[Job],
    response_model_exclude_defaults=True,
    summary="List jobs",
)
async def list_jobs(
    *,
    service: Annotated[str, Depends(auth_service_dependency)],
    user: Annotated[str, Depends(auth_dependency)],
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
) -> list[dict[str, Any]]:
    job_service = context.factory.create_job_service()
    jobs = await job_service.list_jobs(
        service, user, phases=phase, after=after, count=count
    )
    return [j.model_dump(exclude={"service"}) for j in jobs]


@router.post(
    "/jobs",
    description="Create a new job for the authenticated user",
    response_model=Job,
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
) -> Job:
    job_service = context.factory.create_job_service()
    job = await job_service.create(service, user, job_data)
    url = context.request.url_for("get_job", job_id=job.id)
    response.headers["Location"] = str(url)
    return job


@router.get(
    "/jobs/{job_id}",
    description="Retrieve the record for a single job",
    response_model=Job,
    response_model_exclude_defaults=True,
    summary="Get job",
)
async def get_job(
    *,
    job_id: Annotated[JobIdentifier, Depends(job_identifier_dependency)],
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> Job:
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
    response_model=Job,
    response_model_exclude_defaults=True,
    summary="Update job",
)
async def patch_job(
    *,
    job_id: Annotated[JobIdentifier, Depends(job_identifier_dependency)],
    update: JobUpdate,
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> Job:
    job_service = context.factory.create_job_service()
    try:
        return await job_service.update(job_id, update)
    except UnknownJobError as e:
        e.location = ErrorLocation.path
        e.field_path = ["job_id"]
        raise
