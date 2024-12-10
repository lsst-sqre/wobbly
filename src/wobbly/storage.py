"""Storage layer for the UWS implementation."""

from __future__ import annotations

from datetime import UTC, datetime

from safir.database import (
    PaginatedList,
    PaginatedQueryRunner,
    datetime_to_db,
    retry_async_transaction,
)
from safir.datetime import current_datetime
from safir.uws import (
    JobCreate,
    JobError,
    JobResult,
    JobUpdateMetadata,
    SerializedJob,
)
from sqlalchemy import delete, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import async_scoped_session
from vo_models.uws.types import ExecutionPhase
from vo_models.vosi.availability import Availability

from .exceptions import UnknownJobError
from .models import JobCursor, JobIdentifier, JobSearch
from .schema import Job as SQLJob
from .schema import JobError as SQLJobError
from .schema import JobResult as SQLJobResult

__all__ = ["JobStore"]


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
        self._paginated_runner = PaginatedQueryRunner(SerializedJob, JobCursor)

    async def add(
        self, service: str, owner: str, job_data: JobCreate
    ) -> SerializedJob:
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
        SerializedJob
            Full job record of the newly-created job.
        """
        duration = None
        if job_data.execution_duration:
            duration = int(job_data.execution_duration.total_seconds())
        destruction_time = job_data.destruction_time.replace(microsecond=0)
        job = SQLJob(
            service=service,
            owner=owner,
            phase=ExecutionPhase.PENDING,
            run_id=job_data.run_id,
            json_parameters=job_data.json_parameters,
            creation_time=datetime_to_db(current_datetime()),
            destruction_time=datetime_to_db(destruction_time),
            execution_duration=duration,
            errors=[],
            results=[],
        )
        async with self._session.begin():
            self._session.add(job)
            await self._session.flush()
            return SerializedJob.model_validate(job, from_attributes=True)

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

    async def delete(self, job_id: JobIdentifier) -> bool:
        """Delete a job by ID.

        Parameters
        ----------
        job_id
            Identifier of job to dleete.

        Returns
        -------
        bool
            `True` if a job with that ID was found and deleted, `False`
            otherwise.
        """
        stmt = delete(SQLJob).where(
            SQLJob.service == job_id.service, SQLJob.id == int(job_id.id)
        )
        if job_id.owner:
            stmt = stmt.where(SQLJob.owner == job_id.owner)
        async with self._session.begin():
            result = await self._session.execute(stmt)
            return result.rowcount >= 1

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
        async with self._session.begin():
            job = await self._get_job(job_id)
            return SerializedJob.model_validate(job, from_attributes=True)

    async def list_expired(self) -> list[SerializedJob]:
        """List jobs that have passed their destruction time.

        Excludes jobs that are already marked as archived.

        Returns
        -------
        list of SerializedJob
            List of expired jobs that are not currently archived.
        """
        now = datetime_to_db(datetime.now(tz=UTC))
        stmt = select(SQLJob).where(
            SQLJob.destruction_time <= now,
            SQLJob.phase != ExecutionPhase.ARCHIVED,
        )
        async with self._session.begin():
            jobs = await self._session.scalars(stmt)
            return [
                SerializedJob.model_validate(j, from_attributes=True)
                for j in jobs
            ]

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
        stmt = select(SQLJob)
        if service:
            stmt = stmt.where(SQLJob.service == service)
        if user:
            stmt = stmt.where(SQLJob.owner == user)
        if search.phases:
            stmt = stmt.where(SQLJob.phase.in_(search.phases))
        if search.since:
            stmt = stmt.where(
                SQLJob.creation_time > datetime_to_db(search.since)
            )
        async with self._session.begin():
            return await self._paginated_runner.query_object(
                self._session, stmt, cursor=search.cursor, limit=search.limit
            )

    async def list_services(self) -> list[str]:
        """List the services that have any jobs stored.

        Returns
        -------
        list of str
            List of service names.
        """
        stmt = select(SQLJob.service).order_by(SQLJob.service)
        async with self._session.begin():
            return list(await self._session.scalars(stmt.distinct()))

    async def list_users(self, service: str | None) -> list[str]:
        """List the users who have jobs for a given service.

        Parameters
        ----------
        service
            Name of the service, or `None` to include jobs from any service.

        Returns
        -------
        list of str
            List of user names.
        """
        stmt = select(SQLJob.owner).order_by(SQLJob.owner)
        if service:
            stmt = stmt.where(SQLJob.service == service)
        async with self._session.begin():
            return list(await self._session.scalars(stmt.distinct()))

    @retry_async_transaction
    async def mark_aborted(self, job_id: JobIdentifier) -> SerializedJob:
        """Mark a job as aborted.

        If the job has an associated start time, set or change the end time to
        the current time.

        Parameters
        ----------
        job_id
            Identifier of the job.

        Returns
        -------
        SerializedJob
            Job after the transition has been applied.

        Raises
        ------
        UnknownJobError
            Raised if the job was not found.
        """
        async with self._session.begin():
            job = await self._get_job(job_id)
            job.phase = ExecutionPhase.ABORTED
            if job.start_time:
                job.end_time = datetime_to_db(current_datetime())
            return SerializedJob.model_validate(job, from_attributes=True)

    @retry_async_transaction
    async def mark_archived(self, job_id: JobIdentifier) -> SerializedJob:
        """Mark a job as archived.

        Parameters
        ----------
        job_id
            Identifier of the job.

        Returns
        -------
        SerializedJob
            Job after the transition has been applied.

        Raises
        ------
        UnknownJobError
            Raised if the job was not found.
        """
        async with self._session.begin():
            job = await self._get_job(job_id)
            job.phase = ExecutionPhase.ARCHIVED
            return SerializedJob.model_validate(job, from_attributes=True)

    @retry_async_transaction
    async def mark_completed(
        self, job_id: JobIdentifier, results: list[JobResult]
    ) -> SerializedJob:
        """Mark a job as completed.

        Set the job end time to the current time.

        If the job phase is already ``ABORTED``, do not change the phase, but
        do store the job results. This can happen if there's a race between
        aborting the job and completion of the job. The UWS standard says that
        any results generated before the abort should be retained.

        Parameters
        ----------
        job_id
            Identifier of the job.
        results
            Results of the job.

        Returns
        -------
        SerializedJob
            Job after the transition has been applied.

        Raises
        ------
        UnknownJobError
            Raised if the job was not found.
        """
        async with self._session.begin():
            job = await self._get_job(job_id)
            job.end_time = datetime_to_db(current_datetime())
            if not job.start_time:
                job.start_time = job.end_time
            if job.phase != ExecutionPhase.ABORTED:
                job.phase = ExecutionPhase.COMPLETED
            for sequence, result in enumerate(results, start=1):
                sql_result = SQLJobResult(
                    job_id=job.id,
                    id=result.id,
                    sequence=sequence,
                    url=result.url,
                    size=result.size,
                    mime_type=result.mime_type,
                )
                self._session.add(sql_result)
            await self._session.refresh(job, ["results"])
            return SerializedJob.model_validate(job, from_attributes=True)

    @retry_async_transaction
    async def mark_failed(
        self, job_id: JobIdentifier, errors: list[JobError]
    ) -> SerializedJob:
        """Mark a job as failed with an error.

        Set the job end time to the current time.

        If the job phase is already ``ABORTED``, do not change it to
        ``ERROR``, but do store the error information in case the user queries
        it and finds it useful. This can happen if there is a race condition
        between the job failing and the job being aborted by the user.

        Parameters
        ----------
        job_id
            Identifier of the job.
        errors
            Errors that caused the job failure.

        Returns
        -------
        SerializedJob
            Job after the transition has been applied.

        Raises
        ------
        UnknownJobError
            Raised if the job was not found.
        """
        async with self._session.begin():
            job = await self._get_job(job_id)
            job.end_time = datetime_to_db(current_datetime())
            if not job.start_time:
                job.start_time = job.end_time
            if job.phase != ExecutionPhase.ABORTED:
                job.phase = ExecutionPhase.ERROR
            for error in errors:
                sql_error = SQLJobError(
                    job_id=job.id,
                    type=error.type,
                    code=error.code,
                    message=error.message,
                    detail=error.detail,
                )
                self._session.add(sql_error)
            await self._session.refresh(job, ["errors"])
            return SerializedJob.model_validate(job, from_attributes=True)

    @retry_async_transaction
    async def mark_executing(
        self, job_id: JobIdentifier, start_time: datetime
    ) -> SerializedJob:
        """Mark a job as executing.

        Parameters
        ----------
        job_id
            Identifier of the job.
        start_time
            Time at which the job started executing. The job record will be
            updated accordingly.

        Returns
        -------
        SerializedJob
            Job after the transition has been applied.

        Raises
        ------
        UnknownJobError
            Raised if the job was not found.
        """
        start_time = start_time.replace(microsecond=0)
        async with self._session.begin():
            job = await self._get_job(job_id)
            if job.phase in (ExecutionPhase.PENDING, ExecutionPhase.QUEUED):
                job.phase = ExecutionPhase.EXECUTING
            job.start_time = datetime_to_db(start_time)
            return SerializedJob.model_validate(job, from_attributes=True)

    @retry_async_transaction
    async def mark_queued(
        self, job_id: JobIdentifier, message_id: str | None
    ) -> SerializedJob:
        """Mark a job as queued for processing.

        This is called by the web frontend after queuing the work. However,
        the worker may have gotten there first and have already updated the
        phase to executing, in which case we should not set it back to queued.

        Parameters
        ----------
        job_id
            Identifier of the job.
        message_id
            Message ID representing the job in some queuing system.

        Returns
        -------
        SerializedJob
            Job after the transition has been applied.

        Raises
        ------
        UnknownJobError
            Raised if the job was not found.
        """
        async with self._session.begin():
            job = await self._get_job(job_id)
            if message_id:
                job.message_id = message_id
            if job.phase in (ExecutionPhase.PENDING, ExecutionPhase.HELD):
                job.phase = ExecutionPhase.QUEUED
            return SerializedJob.model_validate(job, from_attributes=True)

    @retry_async_transaction
    async def update(
        self, job_id: JobIdentifier, job_update: JobUpdateMetadata
    ) -> SerializedJob:
        """Update some portion of the job.

        Parameters
        ----------
        job_id
            Identifier of the job.
        job_update
            Update to apply.

        Returns
        -------
        SerializedJob
            The modified job record.

        Raises
        ------
        UnknownJobError
            Raised if the job was not found.
        """
        async with self._session.begin():
            job = await self._get_job(job_id)
            if job_update.destruction_time:
                time = job_update.destruction_time.replace(microsecond=0)
                job.destruction_time = datetime_to_db(time)
            if job_update.execution_duration:
                duration = int(job_update.execution_duration.total_seconds())
                job.execution_duration = duration
            return SerializedJob.model_validate(job, from_attributes=True)

    async def _get_job(self, job_id: JobIdentifier) -> SQLJob:
        """Retrieve a job from the database by job ID."""
        stmt = select(SQLJob).where(
            SQLJob.id == int(job_id.id), SQLJob.service == job_id.service
        )
        if job_id.owner:
            stmt = stmt.where(SQLJob.owner == job_id.owner)
        job = (await self._session.execute(stmt)).scalar_one_or_none()
        if not job:
            raise UnknownJobError(job_id.id)
        return job
