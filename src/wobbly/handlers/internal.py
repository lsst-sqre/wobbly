"""Internal HTTP handlers that serve relative to the root path, ``/``.

These handlers aren't externally visible since the app is available at a path,
``/wobbly``. These handlers should be used for monitoring, health checks,
internal status, or other information that should not be visible outside the
Kubernetes cluster.
"""

from typing import Annotated

from fastapi import APIRouter, Depends
from safir.metadata import Metadata, get_metadata
from safir.slack.webhook import SlackRouteErrorHandler

from ..config import config
from ..dependencies.context import RequestContext, context_dependency
from ..models import HealthCheck

__all__ = ["router"]

router = APIRouter(route_class=SlackRouteErrorHandler)
"""FastAPI router for all internal handlers."""


@router.get(
    "/",
    description=(
        "Return metadata about the running application. This route is not"
        " exposed outside the cluster and therefore cannot be used by"
        " external clients."
    ),
    include_in_schema=False,
    response_model_exclude_none=True,
    summary="Application metadata",
)
async def get_index() -> Metadata:
    return get_metadata(package_name="wobbly", application_name=config.name)


@router.get(
    "/health",
    description=(
        "Perform service health check. This route is not exposed outside the"
        " cluster and therefore cannot be used by external clients."
    ),
    include_in_schema=False,
    summary="Health check",
)
async def get_health(
    *,
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> HealthCheck:
    job_service = context.factory.create_job_service()
    return await job_service.health()
