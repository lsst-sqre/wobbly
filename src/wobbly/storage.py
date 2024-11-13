"""Storage layer for the UWS implementation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from safir.database import (
    datetime_from_db,
    datetime_to_db,
    retry_async_transaction,
)
from safir.datetime import current_datetime
from sqlalchemy import delete, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import async_scoped_session
from vo_models.uws.types import ExecutionPhase
from vo_models.vosi.availability import Availability

from .exceptions import UnknownJobError
from .models import (
    Job,
    JobCreate,
    JobError,
    JobIdentifier,
    JobResult,
    JobUpdateMetadata,
)
from .schema import Job as SQLJob
from .schema import JobResult as SQLJobResult

__all__ = ["JobStore"]


def _convert_job(job: SQLJob) -> Job:
    """Convert the SQL representation of a job to its model.

    The internal representation of a job uses a model that is kept separate
    from the database schema so that the conversion can be done explicitly and
    the API isolated from the SQLAlchemy database models. This internal helper
    function converts from the database representation to the model.
    """
    execution_duration = None
    if job.execution_duration:
        execution_duration = timedelta(seconds=job.execution_duration)
    error = None
    if job.error_code and job.error_type and job.error_message:
        error = JobError(
            type=job.error_type,
            code=job.error_code,
            message=job.error_message,
            detail=job.error_detail,
        )
    return Job(
        id=str(job.id),
        service=job.service,
        owner=job.owner,
        phase=job.phase,
        message_id=job.message_id,
        run_id=job.run_id,
        parameters=job.parameters,
        creation_time=datetime_from_db(job.creation_time),
        start_time=datetime_from_db(job.start_time),
        end_time=datetime_from_db(job.end_time),
        destruction_time=datetime_from_db(job.destruction_time),
        execution_duration=execution_duration,
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
        duration = None
        if job_data.execution_duration:
            duration = int(job_data.execution_duration.total_seconds())
        destruction_time = job_data.destruction_time.replace(microsecond=0)
        job = SQLJob(
            service=service,
            owner=owner,
            phase=ExecutionPhase.PENDING,
            run_id=job_data.run_id,
            parameters=job_data.parameters,
            creation_time=datetime_to_db(current_datetime()),
            destruction_time=datetime_to_db(destruction_time),
            execution_duration=duration,
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
        async with self._session.begin():
            job = await self._get_job(job_id)
            return _convert_job(job)

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
            return [_convert_job(j) for j in jobs.scalars()]

    async def list_jobs(
        self,
        service: str,
        user: str | None = None,
        *,
        phases: set[ExecutionPhase] | None = None,
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
            return [_convert_job(j) for j in jobs.scalars()]

    async def list_services(self) -> list[str]:
        """List the services that have any jobs stored.

        Returns
        -------
        list of str
            List of service names.
        """
        stmt = select(SQLJob.service).distinct()
        async with self._session.begin():
            services = await self._session.execute(stmt)
            return list(services.scalars())

    async def list_users(self, service: str) -> list[str]:
        """List the users who have jobs for a given service.

        Parameters
        ----------
        service
            Name of the service.

        Returns
        -------
        list of str
            List of user names.
        """
        stmt = select(SQLJob.owner).where(SQLJob.service == service).distinct()
        async with self._session.begin():
            users = await self._session.execute(stmt)
            return list(users.scalars())

    @retry_async_transaction
    async def mark_aborted(self, job_id: JobIdentifier) -> Job:
        """Mark a job as aborted.

        If the job has an associated start time, set or change the end time to
        the current time.

        Parameters
        ----------
        job_id
            Identifier of the job.

        Returns
        -------
        Job
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
            return _convert_job(job)

    @retry_async_transaction
    async def mark_archived(self, job_id: JobIdentifier) -> Job:
        """Mark a job as archived.

        Parameters
        ----------
        job_id
            Identifier of the job.

        Returns
        -------
        Job
            Job after the transition has been applied.

        Raises
        ------
        UnknownJobError
            Raised if the job was not found.
        """
        async with self._session.begin():
            job = await self._get_job(job_id)
            job.phase = ExecutionPhase.ARCHIVED
            return _convert_job(job)

    @retry_async_transaction
    async def mark_completed(
        self, job_id: JobIdentifier, results: list[JobResult]
    ) -> Job:
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
        Job
            Job after the transition has been applied.

        Raises
        ------
        UnknownJobError
            Raised if the job was not found.
        """
        async with self._session.begin():
            job = await self._get_job(job_id)
            job.end_time = datetime_to_db(current_datetime())
            if job.phase != ExecutionPhase.ABORTED:
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
            await self._session.refresh(job, ["results"])
            return _convert_job(job)

    @retry_async_transaction
    async def mark_failed(self, job_id: JobIdentifier, error: JobError) -> Job:
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
        error
            Error that caused the job failure.

        Returns
        -------
        Job
            Job after the transition has been applied.

        Raises
        ------
        UnknownJobError
            Raised if the job was not found.
        """
        async with self._session.begin():
            job = await self._get_job(job_id)
            job.end_time = datetime_to_db(current_datetime())
            if job.phase != ExecutionPhase.ABORTED:
                job.phase = ExecutionPhase.ERROR
            job.error_type = error.type
            job.error_code = error.code
            job.error_message = error.message
            job.error_detail = error.detail
            return _convert_job(job)

    @retry_async_transaction
    async def mark_executing(
        self, job_id: JobIdentifier, start_time: datetime
    ) -> Job:
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
        Job
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
            return _convert_job(job)

    @retry_async_transaction
    async def mark_queued(
        self, job_id: JobIdentifier, message_id: str | None
    ) -> Job:
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
        Job
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
            return _convert_job(job)

    @retry_async_transaction
    async def update(
        self, job_id: JobIdentifier, job_update: JobUpdateMetadata
    ) -> Job:
        """Update some portion of the job.

        Parameters
        ----------
        job_id
            Identifier of the job.
        job_update
            Update to apply.

        Returns
        -------
        Job
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
            return _convert_job(job)

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
