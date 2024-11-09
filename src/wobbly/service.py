"""Service layer for the UWS storage service."""

from __future__ import annotations

from datetime import datetime
from typing import assert_never

from safir.datetime import format_datetime_for_logging
from structlog.stdlib import BoundLogger
from vo_models.uws.types import ExecutionPhase

from .exceptions import UnknownJobError
from .models import (
    Job,
    JobCreate,
    JobIdentifier,
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
        service: str,
        user: str | None = None,
        *,
        phases: list[ExecutionPhase] | None = None,
        after: datetime | None = None,
        count: int | None = None,
    ) -> list[Job]:
        """List jobs.

        Parameters
        ----------
        service
            Name of the service that owns the job.
        user
            Name of the user who owns the job, or `None` to include jobs owned
            by all users.
        phases
            Limit the result to jobs in this list of possible execution
            phases.
        after
            Limit the result to jobs created after the given datetime in UTC.
        count
            Limit the results to the most recent count jobs.

        Returns
        -------
        list of Job
            List of jobs matching the search criteria.
        """
        return await self._storage.list_jobs(
            service,
            user,
            phases=set(phases) if phases else None,
            after=after,
            count=count,
        )

    async def list_services(self) -> list[str]:
        """List the services that have any jobs stored.

        Returns
        -------
        list of str
            List of service names.
        """
        return await self._storage.list_services()

    async def list_users(self, service: str) -> list[str]:
        """List the users who have jobs for a given service.

        Parameters
        ----------
        service
            Name of the service.

        Returns
        -------
        list of str
            List of service names.
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
                job = await self._storage.mark_failed(job_id, update.error)
                logger = logger.bind(
                    phase=str(job.phase),
                    error_code=update.error.code,
                    error=update.error.message,
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
