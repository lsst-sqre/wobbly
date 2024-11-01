"""Service layer for the UWS storage service."""

from __future__ import annotations

from datetime import datetime

from structlog.stdlib import BoundLogger
from vo_models.uws.types import ExecutionPhase

from .exceptions import UnknownJobError
from .models import (
    Job,
    JobIdentifier,
    JobUpdate,
    JobUpdateAborted,
    JobUpdateCompleted,
    JobUpdateError,
    JobUpdateExecuting,
    JobUpdateMetadata,
    JobUpdateQueued,
)
from .storage import JobStorage

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

    def __init__(self, job_storage: JobStorage, logger: BoundLogger) -> None:
        self._storage = job_storage
        self._logger = logger

    async def list(
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
        return await self._storage.list(
            service,
            user,
            phases=set(phases) if phases else None,
            after=after,
            count=count,
        )

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
        match update:
            case JobUpdateAborted():
                return await self._storage.mark_aborted(job_id)
            case JobUpdateCompleted():
                return await self._storage.mark_completed(
                    job_id, update.results
                )
            case JobUpdateError():
                return await self._storage.mark_failed(job_id, update.error)
            case JobUpdateExecuting():
                return await self._storage.mark_executing(
                    job_id, update.start_time
                )
            case JobUpdateQueued():
                return await self._storage.mark_queued(
                    job_id, update.message_id
                )
            case JobUpdateMetadata():
                return await self._storage.update(job_id, update)
