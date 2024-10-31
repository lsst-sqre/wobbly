"""Storage layer for the UWS implementation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import ParamSpec, TypeVar

from safir.database import (
    datetime_from_db,
    datetime_to_db,
    retry_async_transaction,
)
from sqlalchemy import delete, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import async_scoped_session
from vo_models.uws.types import ExecutionPhase
from vo_models.vosi.availability import Availability

from .exceptions import UnknownJobError
from .models import Job, JobCreate, JobError, JobResult
from .schema import Job as SQLJob
from .schema import JobResult as SQLJobResult

T = TypeVar("T")
P = ParamSpec("P")

__all__ = ["JobStore"]


def _convert_job(job: SQLJob) -> Job:
    """Convert the SQL representation of a job to its model.

    The internal representation of a job uses a model that is kept separate
    from the database schema so that the conversion can be done explicitly and
    the API isolated from the SQLAlchemy database models. This internal helper
    function converts from the database representation to the model.
    """
    error = None
    if job.error_code and job.error_type and job.error_message:
        error = JobError(
            type=job.error_type,
            code=job.error_code,
            message=job.error_message,
            detail=job.error_detail,
        )
    return Job(
        job_id=str(job.id),
        owner=job.owner,
        phase=job.phase,
        message_id=job.message_id,
        run_id=job.run_id,
        parameters=job.parameters,
        creation_time=datetime_from_db(job.creation_time),
        start_time=datetime_from_db(job.start_time),
        end_time=datetime_from_db(job.end_time),
        destruction_time=datetime_from_db(job.destruction_time),
        execution_duration=timedelta(seconds=job.execution_duration),
        quote=job.quote,
        results=[
            JobResult(
                id=r.result_id, url=r.url, size=r.size, mime_type=r.mime_type
            )
            for r in sorted(job.results, key=lambda r: r.sequence)
        ],
        error=error,
    )


class JobStore:
    """Stores and manipulates jobs in the database.

    The canonical representation of any UWS job is in the database. This class
    provides methods to create, update, and delete UWS job records and their
    associated results and errors.

    Parameters
    ----------
    session
        The underlying database session.
    """

    def __init__(self, session: async_scoped_session) -> None:
        self._session = session

    async def add(self, service: str, owner: str, job_data: JobCreate) -> Job:
        """Create a record of a new job.

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
        execution_duration = int(job_data.execution_duration.total_seconds())
        job = SQLJob(
            service=service,
            owner=owner,
            phase=ExecutionPhase.PENDING,
            run_id=job_data.run_id,
            parameters=job_data.parameters,
            creation_time=datetime_to_db(datetime.now(tz=UTC)),
            destruction_time=datetime_to_db(job_data.destruction_time),
            execution_duration=execution_duration,
            results=[],
        )
        async with self._session.begin():
            self._session.add(job)
            await self._session.flush()
            return _convert_job(job)

    async def availability(self) -> Availability:
        """Check that the database is up.

        Returns
        -------
        Availability
            An IVOA availability data structure.
        """
        try:
            async with self._session.begin():
                await self._session.execute(select(SQLJob.id).limit(1))
            return Availability(available=True)
        except OperationalError:
            note = "cannot query UWS job database"
            return Availability(available=False, note=[note])
        except Exception as e:
            note = f"{type(e).__name__}: {e!s}"
            return Availability(available=False, note=[note])

    async def delete(self, service: str, job_id: str) -> bool:
        """Delete a job by ID.

        Parameters
        ----------
        service
            Service associated with the job. If the job ID exists but doesn't
            match the service, the result will be exactly the same as if the
            job ID doesn't exist.
        job_id
            ID of the job to delete.

        Returns
        -------
        bool
            `True` if a job with that ID associated with that service was
            found and deleted, `False` otherwise.
        """
        stmt = delete(SQLJob).where(
            SQLJob.service == service, SQLJob.id == int(job_id)
        )
        async with self._session.begin():
            result = await self._session.execute(stmt)
            return result.rowcount >= 1

    async def get(self, job_id: str) -> Job:
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
        async with self._session.begin():
            job = await self._get_job(job_id)
            return _convert_job(job)

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
        stmt = select(SQLJob).where(SQLJob.service == service)
        if user:
            stmt = stmt.where(SQLJob.owner == user)
        if phases:
            stmt = stmt.where(SQLJob.phase.in_(phases))
        if after:
            stmt = stmt.where(SQLJob.creation_time > datetime_to_db(after))
        stmt = stmt.order_by(SQLJob.creation_time.desc())
        if count:
            stmt = stmt.limit(count)
        async with self._session.begin():
            jobs = await self._session.execute(stmt)
            return [_convert_job(j) for j in jobs.all()]

    async def list_expired(self) -> list[Job]:
        """List jobs that have passed their destruction time.

        Excludes jobs that are already marked as archived.

        Returns
        -------
        list of Job
            List of expired jobs that are not currently archived.
        """
        now = datetime_to_db(datetime.now(tz=UTC))
        stmt = select(SQLJob).where(
            SQLJob.destruction_time <= now,
            SQLJob.phase != ExecutionPhase.ARCHIVED,
        )
        async with self._session.begin():
            jobs = await self._session.execute(stmt)
            return [_convert_job(j) for j in jobs.all()]

    @retry_async_transaction
    async def mark_aborted(self, service: str, job_id: str) -> None:
        """Mark a job as aborted.

        Parameters
        ----------
        service
            Service that owns the job.
        job_id
            Identifier of the job.

        Raises
        ------
        UnknownJobError
            Raised if the job was not found or has a non-matching service.
        """
        async with self._session.begin():
            job = await self._get_job(job_id, service)
            job.phase = ExecutionPhase.ABORTED
            if job.start_time:
                job.end_time = datetime_to_db(datetime.now(tz=UTC))

    @retry_async_transaction
    async def mark_completed(
        self, service: str, job_id: str, results: list[JobResult]
    ) -> None:
        """Mark a job as completed.

        Parameters
        ----------
        service
            Service that owns the job.
        job_id
            Identifier of the job.
        results
            Results of the job.

        Raises
        ------
        UnknownJobError
            Raised if the job was not found or has a non-matching service.
        """
        async with self._session.begin():
            job = await self._get_job(job_id, service)
            job.end_time = datetime_to_db(datetime.now(tz=UTC))
            if job.phase == ExecutionPhase.ABORTED:
                return
            job.phase = ExecutionPhase.COMPLETED
            for sequence, result in enumerate(results, start=1):
                sql_result = SQLJobResult(
                    job_id=job.id,
                    result_id=result.id,
                    sequence=sequence,
                    url=result.url,
                    size=result.size,
                    mime_type=result.mime_type,
                )
                self._session.add(sql_result)

    @retry_async_transaction
    async def mark_failed(
        self, service: str, job_id: str, error: JobError
    ) -> None:
        """Mark a job as failed with an error.

        Parameters
        ----------
        service
            Service that owns the job.
        job_id
            Identifier of the job.
        error
            Error that caused the job failure.

        Raises
        ------
        UnknownJobError
            Raised if the job was not found or has a non-matching service.
        """
        async with self._session.begin():
            job = await self._get_job(job_id, service)
            job.end_time = datetime_to_db(datetime.now(tz=UTC))
            if job.phase == ExecutionPhase.ABORTED:
                return
            job.phase = ExecutionPhase.ERROR
            job.error_type = error.type
            job.error_code = error.code
            job.error_message = error.message
            job.error_detail = error.detail

    @retry_async_transaction
    async def mark_executing(
        self, service: str, job_id: str, start_time: datetime
    ) -> None:
        """Mark a job as executing.

        Parameters
        ----------
        service
            Service that owns the job.
        job_id
            Identifier of the job.
        start_time
            Time at which the job started executing.

        Raises
        ------
        UnknownJobError
            Raised if the job was not found or has a non-matching service.
        """
        start_time = start_time.replace(microsecond=0)
        async with self._session.begin():
            job = await self._get_job(job_id, service)
            if job.phase in (ExecutionPhase.PENDING, ExecutionPhase.QUEUED):
                job.phase = ExecutionPhase.EXECUTING
            job.start_time = datetime_to_db(start_time)

    @retry_async_transaction
    async def mark_queued(
        self, service: str, job_id: str, message_id: str
    ) -> None:
        """Mark a job as queued for processing.

        This is called by the web frontend after queuing the work. However,
        the worker may have gotten there first and have already updated the
        phase to executing, in which case we should not set it back to queued.

        Parameters
        ----------
        service
            Service that owns the job.
        job_id
            Identifier of the job.
        message_id
            Message ID representing the job in some queuing system.

        Raises
        ------
        UnknownJobError
            Raised if the job was not found or has a non-matching service.
        """
        async with self._session.begin():
            job = await self._get_job(job_id, service)
            job.message_id = message_id
            if job.phase in (ExecutionPhase.PENDING, ExecutionPhase.HELD):
                job.phase = ExecutionPhase.QUEUED

    async def update_destruction(
        self, job_id: str, destruction: datetime
    ) -> None:
        """Update the destruction time of a job.

        Parameters
        ----------
        job_id
            Identifier of the job.
        destruction
            New destruction time.
        """
        destruction = destruction.replace(microsecond=0)
        async with self._session.begin():
            job = await self._get_job(job_id)
            job.destruction_time = datetime_to_db(destruction)

    async def update_execution_duration(
        self, job_id: str, execution_duration: timedelta
    ) -> None:
        """Update the destruction time of a job.

        Parameters
        ----------
        job_id
            Identifier of the job.
        execution_duration
            New execution duration.
        """
        async with self._session.begin():
            job = await self._get_job(job_id)
            job.execution_duration = int(execution_duration.total_seconds())

    async def _get_job(self, job_id: str) -> SQLJob:
        """Retrieve a job from the database by job ID."""
        stmt = select(SQLJob).where(SQLJob.id == int(job_id))
        job = (await self._session.execute(stmt)).scalar_one_or_none()
        if not job:
            raise UnknownJobError(job_id)
        return job
