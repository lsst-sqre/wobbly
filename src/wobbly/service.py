"""Service layer for the UWS storage service."""

from __future__ import annotations

from typing import assert_never

from safir.database import PaginatedList
from safir.datetime import format_datetime_for_logging
from safir.uws import (
    JobCreate,
    JobUpdateAborted,
    JobUpdateCompleted,
    JobUpdateError,
    JobUpdateExecuting,
    JobUpdateMetadata,
    JobUpdateQueued,
    SerializedJob,
)
from structlog.stdlib import BoundLogger

from .events import (
    AbortedJobEvent,
    CompletedJobEvent,
    CreatedJobEvent,
    Events,
    FailedJobEvent,
    QueuedJobEvent,
)
from .exceptions import UnknownJobError
from .models import JobCursor, JobIdentifier, JobSearch, JobUpdate
from .storage import JobStore

__all__ = ["JobService"]


class JobService:
    """Manipulate job records for UWS services.

    Parameters
    ----------
    job_storage
        Underlying database storage.
    events
        Event publishers.
    logger
        Logger to use.
    """

    def __init__(
        self, job_storage: JobStore, events: Events, logger: BoundLogger
    ) -> None:
        self._storage = job_storage
        self._events = events
        self._logger = logger

    async def create(
        self, service: str, owner: str, job_data: JobCreate
    ) -> SerializedJob:
        """Create a new job.

        The job will be created in pending status.

        Parameters
        ----------
        service
            Service that owns this job.
        owner
            Username that owns this job.
        job_data
            Client-provided information for the UWS job.

        Returns
        -------
        SerializedJob
            Full job record of the newly-created job.
        """
        job = await self._storage.add(service, owner, job_data)
        event = CreatedJobEvent(service=service, owner=owner)
        await self._events.created.publish(event)
        self._logger.info(
            "Created job", service=service, owner=owner, job=job.id
        )
        return job

    async def delete(self, job_id: JobIdentifier) -> None:
        """Delete a job by ID.

        Parameters
        ----------
        job_id
            Identifier of job to dleete.

        Raises
        ------
        UnknownJobError
            Raised if the job was not found.
        """
        status = await self._storage.delete(job_id)
        if not status:
            raise UnknownJobError(job_id.id)
        self._logger.info(
            "Deleted job",
            service=job_id.service,
            owner=job_id.owner,
            job=job_id.id,
        )

    async def get(self, job_id: JobIdentifier) -> SerializedJob:
        """Retrieve a job by ID.

        Parameters
        ----------
        job_id
            ID of the job to retrieve.

        Returns
        -------
        SerializedJob
            Corresponding job.

        Raises
        ------
        UnknownJobError
            Raised if the job was not found.
        """
        return await self._storage.get(job_id)

    async def list_jobs(
        self,
        search: JobSearch,
        service: str | None = None,
        user: str | None = None,
    ) -> PaginatedList[SerializedJob, JobCursor]:
        """List jobs.

        Parameters
        ----------
        search
            Job search parameters.
        service
            Name of the service that owns the job, or `None` to include jobs
            owned by any service.
        user
            Name of the user who owns the job, or `None` to include jobs owned
            by any user.

        Returns
        -------
        PaginatedList of SerializedJob
            List of jobs matching the search criteria.
        """
        return await self._storage.list_jobs(search, service, user)

    async def list_services(self) -> list[str]:
        """List the services that have any jobs stored.

        Returns
        -------
        list of str
            List of service names.
        """
        return await self._storage.list_services()

    async def list_users(self, service: str | None = None) -> list[str]:
        """List the users who have jobs stored.

        Parameters
        ----------
        service
            Name of the service, or `None` to list users who have a job stored
            for any service.

        Returns
        -------
        list of str
            List of users.
        """
        return await self._storage.list_users(service)

    async def update(
        self, job_id: JobIdentifier, update: JobUpdate
    ) -> SerializedJob:
        """Update an existing job.

        Parameters
        ----------
        job_id
            ID of the job to update.
        update
            Update to apply to the job.

        Returns
        -------
        SerializedJob
            Job after the update has been applied.

        Raises
        ------
        UnknownJobError
            Raised if the job was not found.
        """
        logger = self._logger.bind(
            service=job_id.service, owner=job_id.owner, job=job_id.id
        )
        match update:
            case JobUpdateAborted():
                job = await self._storage.mark_aborted(job_id)
                aborted_event = AbortedJobEvent(
                    service=job.service, owner=job.owner
                )
                await self._events.aborted.publish(aborted_event)
                logger = logger.bind(phase=str(job.phase))
            case JobUpdateCompleted():
                job = await self._storage.mark_completed(
                    job_id, update.results
                )
                if not (job.start_time and job.end_time):
                    msg = "Completed job has no start or end time"
                    raise RuntimeError(msg)
                completed_event = CompletedJobEvent(
                    service=job.service,
                    owner=job.owner,
                    elapsed=job.end_time - job.start_time,
                )
                await self._events.completed.publish(completed_event)
                logger = logger.bind(phase=str(job.phase))
            case JobUpdateError():
                job = await self._storage.mark_failed(job_id, update.errors)
                if not (job.start_time and job.end_time):
                    msg = "Failed job has no start or end time"
                    raise RuntimeError(msg)
                failed_event = FailedJobEvent(
                    service=job.service,
                    owner=job.owner,
                    error_code=update.errors[0].code,
                    elapsed=job.end_time - job.start_time,
                )
                await self._events.failed.publish(failed_event)
                logger = logger.bind(
                    phase=str(job.phase),
                    errors=[
                        {"code": e.code, "message": e.message}
                        for e in update.errors
                    ],
                )
            case JobUpdateExecuting():
                job = await self._storage.mark_executing(
                    job_id, update.start_time
                )
                logger = logger.bind(
                    phase=str(job.phase),
                    start_time=format_datetime_for_logging(update.start_time),
                )
            case JobUpdateQueued():
                job = await self._storage.mark_queued(
                    job_id, update.message_id
                )
                queued_event = QueuedJobEvent(
                    service=job.service, owner=job.owner
                )
                await self._events.queued.publish(queued_event)
                logger = logger.bind(
                    phase=str(job.phase), message_id=update.message_id
                )
            case JobUpdateMetadata():
                job = await self._storage.update(job_id, update)
                time = format_datetime_for_logging(update.destruction_time)
                logger = logger.bind(
                    destruction_time=time,
                    execution_duration=update.execution_duration,
                )
            case _ as unreachable:
                assert_never(unreachable)
        logger.info("Updated job")
        return job
