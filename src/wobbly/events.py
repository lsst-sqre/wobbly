"""Metrics implementation for Wobbly."""

from __future__ import annotations

from datetime import timedelta
from typing import Annotated

from pydantic import Field
from safir.dependencies.metrics import EventMaker
from safir.metrics import EventManager, EventPayload

__all__ = [
    "AbortedJobEvent",
    "CompletedJobEvent",
    "CreatedJobEvent",
    "Events",
    "FailedJobEvent",
    "QueuedJobEvent",
]


class JobEvent(EventPayload):
    """Base class for job events to define shared fields."""

    service: Annotated[
        str, Field(title="Service", description="Service managing the job")
    ]

    owner: Annotated[str, Field(title="Owner", description="Owner of job")]


class AbortedJobEvent(JobEvent):
    """A job was aborted."""


class CreatedJobEvent(JobEvent):
    """A job was created."""


class CompletedJobEvent(JobEvent):
    """A job finished execution successfully."""

    elapsed: Annotated[
        timedelta,
        Field(
            title="Elapsed time",
            description="How long the job took to execute",
        ),
    ]


class FailedJobEvent(JobEvent):
    """A job failed."""

    error_code: Annotated[
        str,
        Field(title="Error code", description="Error code for job failure"),
    ]

    elapsed: Annotated[
        timedelta,
        Field(
            title="Elapsed time", description="How long before the job failed"
        ),
    ]


class QueuedJobEvent(JobEvent):
    """A job was queued for execution."""


class Events(EventMaker):
    """Event publishers for Wobbly events.

    Attributes
    ----------
    aborted
        Event publisher for aborted jobs.
    completed
        Event publisher for completed jobs.
    failed
        Event publisher for failed jobs.
    queued
        Event publisher for queued jobs.
    """

    async def initialize(self, manager: EventManager) -> None:
        self.aborted = await manager.create_publisher(
            "aborted", AbortedJobEvent
        )
        self.created = await manager.create_publisher(
            "created", CreatedJobEvent
        )
        self.completed = await manager.create_publisher(
            "completed", CompletedJobEvent
        )
        self.failed = await manager.create_publisher("failed", FailedJobEvent)
        self.queued = await manager.create_publisher("queued", QueuedJobEvent)
