"""Models for Wobbly."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any, Literal, TypeAlias

from pydantic import AfterValidator, BaseModel, Field, PlainSerializer
from safir.metadata import Metadata as SafirMetadata
from safir.pydantic import SecondsTimedelta, normalize_datetime
from vo_models.uws.types import ErrorType, ExecutionPhase

JobParameters: TypeAlias = dict[str, Any] | list[str]
"""Possible types of job parameters.

This can either be a serialized parameters model (the `dict` case), or a list
of old-style input parameters, which are stored as simple strings.
"""

UtcDatetime: TypeAlias = Annotated[
    datetime, AfterValidator(normalize_datetime)
]
"""Data type representing a date in ISO format in UTC."""

__all__ = [
    "Index",
    "Job",
    "JobBase",
    "JobCreate",
    "JobError",
    "JobIdentifier",
    "JobParameters",
    "JobResult",
    "JobUpdate",
    "JobUpdateAborted",
    "JobUpdateCompleted",
    "JobUpdateExecuting",
    "JobUpdateError",
    "JobUpdateMetadata",
    "JobUpdateQueued",
    "UtcDatetime",
]


class Index(BaseModel):
    """Metadata returned by the external root URL of the application."""

    metadata: SafirMetadata = Field(..., title="Package metadata")


@dataclass
class JobIdentifier:
    """Information required to identify a unique job.

    This always includes service information. Owner information is optional
    but enforced if present. In other words, if an owner is specified and the
    job exists but doesn't match that owner, it is treated as if it doesn't
    exist.
    """

    service: str
    """Service that owns the job."""

    id: str
    """Identifier of the job."""

    owner: str | None = None
    """User who owns the job."""


class JobError(BaseModel):
    """Failure information about a job."""

    type: Annotated[
        ErrorType,
        Field(
            title="Error type",
            description="Type of the error",
            examples=[ErrorType.TRANSIENT, ErrorType.FATAL],
        ),
    ]

    code: Annotated[
        str,
        Field(
            title="Error code",
            description="Code for this class of error",
            examples=["ServiceUnavailable"],
        ),
    ]

    message: Annotated[
        str,
        Field(
            title="Error message",
            description="Brief error messages",
            examples=["Short error message"],
        ),
    ]

    detail: Annotated[
        str | None,
        Field(
            title="Extended error message",
            description="Extended error message with additional detail",
            examples=["Some longer error message with details", None],
        ),
    ] = None


class JobResult(BaseModel):
    """A single result from a job."""

    id: Annotated[
        str,
        Field(
            title="Result ID",
            description="Identifier for this result",
            examples=["image", "metadata"],
        ),
    ]

    url: Annotated[
        str,
        Field(
            title="Result URL",
            description="URL where the result is stored",
            examples=["s3://service-result-bucket/some-file"],
        ),
    ]

    size: Annotated[
        int | None,
        Field(
            title="Size of result",
            description="Size of the result in bytes if known",
            examples=[1238123, None],
        ),
    ] = None

    mime_type: Annotated[
        str | None,
        Field(
            title="MIME type of result",
            description="MIME type of the result if known",
            examples=["application/fits", "application/x-votable+xml", None],
        ),
    ] = None


class JobBase(BaseModel):
    """Fields common to job creation and the stored job record."""

    parameters: Annotated[
        JobParameters,
        Field(
            title="Job parameters",
            description=(
                "May be any JSON-serialized object or list of objects. Stored"
                " opaquely and returned as part of the job record."
            ),
            examples=[
                {
                    "ids": ["data-id"],
                    "stencils": [
                        {
                            "type": "circle",
                            "center": [1.1, 2.1],
                            "radius": 0.001,
                        }
                    ],
                }
            ],
        ),
    ]

    run_id: Annotated[
        str | None,
        Field(
            title="Client-provided run ID",
            description=(
                "The run ID allows the client to add a unique identifier to"
                " all jobs that are part of a single operation, which may aid"
                " in tracing issues through a complex system or identifying"
                " which operation a job is part of"
            ),
            examples=["daily-2024-10-29"],
        ),
    ] = None

    destruction_time: Annotated[
        UtcDatetime,
        Field(
            title="Destruction time",
            description=(
                "At this time, the job will be aborted if it is still"
                " running, its results will be deleted, and it will either"
                " change phase to ARCHIVED or all record of the job will be"
                " discarded"
            ),
            examples=["2024-11-29T23:57:55+00:00"],
        ),
    ]

    execution_duration: Annotated[
        SecondsTimedelta | None,
        Field(
            title="Maximum execution duration",
            description=(
                "Allowed maximum execution duration. This is specified in"
                " elapsed wall clock time (not CPU time). If null, the"
                " execution time is unlimited. If the job runs for longer than"
                " this time period, it will be aborted."
            ),
        ),
        PlainSerializer(
            lambda t: int(t.total_seconds()) if t is not None else None,
            return_type=int,
        ),
    ] = None


class JobCreate(JobBase):
    """Information required to create a new UWS job."""


class Job(JobBase):
    """A single UWS job as stored in the UWS data store."""

    id: Annotated[
        str,
        Field(
            title="Job ID",
            description="Unique identifier of the job",
            examples=["47183"],
        ),
    ]

    service: Annotated[
        str,
        Field(
            title="Service",
            description="Service responsible for this job",
            examples=["vo-cutouts"],
        ),
    ]

    owner: Annotated[
        str,
        Field(
            title="Job owner",
            description="Identity of the owner of the job",
            examples=["someuser"],
        ),
    ]

    phase: Annotated[
        ExecutionPhase,
        Field(
            title="Execution phase",
            description="Current execution phase of the job",
            examples=[
                ExecutionPhase.PENDING,
                ExecutionPhase.EXECUTING,
                ExecutionPhase.COMPLETED,
            ],
        ),
    ]

    message_id: Annotated[
        str | None,
        Field(
            title="Work queue message ID",
            description=(
                "Internal message identifier for the work queuing system."
                " Only meaningful to the service that stored this ID."
            ),
            examples=["e621a175-e3bf-4a61-98d7-483cb5fb9ec2"],
        ),
    ] = None

    creation_time: Annotated[
        UtcDatetime,
        Field(
            title="Creation time",
            description="When the job was created",
            examples=["2024-10-29T23:57:55+00:00"],
        ),
    ]

    start_time: Annotated[
        UtcDatetime | None,
        Field(
            title="Start time",
            description="When the job started executing (if it has)",
            examples=["2024-10-30T00:00:21+00:00", None],
        ),
    ] = None

    end_time: Annotated[
        UtcDatetime | None,
        Field(
            title="End time",
            description="When the job stopped executing (if it has)",
            examples=["2024-10-30T00:08:45+00:00", None],
        ),
    ] = None

    quote: Annotated[
        UtcDatetime | None,
        Field(
            title="Expected completion time",
            description=(
                "Expected completion time of the job if it were started now,"
                " or null to indicate that the expected duration is not known."
                " If later than the destruction time, indicates that the job"
                " is not possible due to resource constraints."
            ),
        ),
    ] = None

    error: Annotated[
        JobError | None,
        Field(
            title="Error", description="Error information if the job failed"
        ),
    ] = None

    results: Annotated[
        list[JobResult],
        Field(
            title="Job results",
            description="Results of the job, if it has finished",
        ),
    ] = []


class JobUpdateAborted(BaseModel):
    """Input model when aborting a job."""

    phase: Annotated[
        Literal[ExecutionPhase.ABORTED],
        Field(
            title="New phase",
            description="New phase of job",
            examples=[ExecutionPhase.ABORTED],
        ),
    ]


class JobUpdateCompleted(BaseModel):
    """Input model when marking a job as complete."""

    phase: Annotated[
        Literal[ExecutionPhase.COMPLETED],
        Field(
            title="New phase",
            description="New phase of job",
            examples=[ExecutionPhase.COMPLETED],
        ),
    ]

    results: Annotated[
        list[JobResult],
        Field(title="Job results", description="All the results of the job"),
    ]


class JobUpdateExecuting(BaseModel):
    """Input model when marking a job as executing."""

    phase: Annotated[
        Literal[ExecutionPhase.EXECUTING],
        Field(
            title="New phase",
            description="New phase of job",
            examples=[ExecutionPhase.EXECUTING],
        ),
    ]

    start_time: Annotated[
        UtcDatetime,
        Field(
            title="Start time",
            description="When the job started executing",
            examples=["2024-11-01T12:15:45+00:00"],
        ),
    ]


class JobUpdateError(BaseModel):
    """Input model when marking a job as failed."""

    phase: Annotated[
        Literal[ExecutionPhase.ERROR],
        Field(
            title="New phase",
            description="New phase of job",
            examples=[ExecutionPhase.ERROR],
        ),
    ]

    error: Annotated[
        JobError,
        Field(
            title="Failure details",
            description="Job failure error message and details",
        ),
    ]


class JobUpdateQueued(BaseModel):
    """Input model when marking a job as queued."""

    phase: Annotated[
        Literal[ExecutionPhase.QUEUED],
        Field(
            title="New phase",
            description="New phase of job",
            examples=[ExecutionPhase.QUEUED],
        ),
    ]

    message_id: Annotated[
        str | None,
        Field(
            title="Queue message ID",
            description="Corresponding message within a job queuing system",
            examples=["4ce850a7-d877-4827-a3f6-f84534ec3fad"],
        ),
    ]


class JobUpdateMetadata(BaseModel):
    """Input model when updating job metadata."""

    phase: Annotated[
        None,
        Field(
            title="New phase", description="New phase of job", examples=[None]
        ),
    ] = None

    destruction_time: Annotated[
        UtcDatetime,
        Field(
            title="Destruction time",
            description=(
                "At this time, the job will be aborted if it is still"
                " running, its results will be deleted, and it will either"
                " change phase to ARCHIVED or all record of the job will be"
                " discarded"
            ),
            examples=["2024-11-29T23:57:55+00:00"],
        ),
    ]

    execution_duration: Annotated[
        SecondsTimedelta | None,
        Field(
            title="Maximum execution duration",
            description=(
                "Allowed maximum execution duration. This is specified in"
                " elapsed wall clock time (not CPU time). If null, the"
                " execution time is unlimited. If the job runs for longer than"
                " this time period, it will be aborted."
            ),
        ),
        PlainSerializer(lambda t: int(t.total_seconds()), return_type=int),
    ]


JobUpdate: TypeAlias = Annotated[
    JobUpdateAborted
    | JobUpdateCompleted
    | JobUpdateError
    | JobUpdateExecuting
    | JobUpdateQueued
    | JobUpdateMetadata,
    Field(title="Update to job"),
]
