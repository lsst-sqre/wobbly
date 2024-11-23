"""Service layer for the UWS storage service."""

from __future__ import annotations

from typing import assert_never

from safir.database import PaginatedList
from safir.datetime import format_datetime_for_logging
from structlog.stdlib import BoundLogger

from .exceptions import UnknownJobError
from .models import (
    Job,
    JobCreate,
    JobCursor,
    JobIdentifier,
    JobSearch,
    JobUpdate,
    JobUpdateAborted,
    JobUpdateCompleted,
    JobUpdateError,
    JobUpdateExecuting,
    JobUpdateMetadata,
    JobUpdateQueued,
)
from .storage import JobStore

__all__ = ["JobService"]


class JobService:
    """Manipulate job records for UWS services.

    Parameters
    ----------
    job_storage
        Underlying database storage.
    logger
        Logger to use.
    """

    def __init__(self, job_storage: JobStore, logger: BoundLogger) -> None:
        self._storage = job_storage
        self._logger = logger

    async def create(
        self, service: str, owner: str, job_data: JobCreate
    ) -> Job:
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
        Job
            Full job record of the newly-created job.
        """
        job = await self._storage.add(service, owner, job_data)
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

    async def get(self, job_id: JobIdentifier) -> Job:
        """Retrieve a job by ID.

        Parameters
        ----------
        job_id
            ID of the job to retrieve.

        Returns
        -------
        Job
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
    ) -> PaginatedList[Job, JobCursor]:
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
        PaginatedList of Job
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

    async def update(self, job_id: JobIdentifier, update: JobUpdate) -> Job:
        """Update an existing job.

        Parameters
        ----------
        job_id
            ID of the job to update.
        update
            Update to apply to the job.

        Returns
        -------
        Job
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
                logger = logger.bind(phase=str(job.phase))
            case JobUpdateCompleted():
                job = await self._storage.mark_completed(
                    job_id, update.results
                )
                logger = logger.bind(phase=str(job.phase))
            case JobUpdateError():
                job = await self._storage.mark_failed(job_id, update.errors)
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
