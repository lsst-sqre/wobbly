"""Tests for the service API to Wobbly."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, cast
from unittest.mock import ANY
from urllib.parse import parse_qs, urlparse

import pytest
from httpx import AsyncClient
from safir.database import PaginationLinkData, datetime_to_db
from safir.dependencies.db_session import db_session_dependency
from safir.metrics import NOT_NONE, MockEventPublisher
from sqlalchemy import select
from vo_models.uws.types import ErrorType

from wobbly.dependencies.context import context_dependency
from wobbly.schema import Job as SQLJob


@pytest.mark.asyncio
async def test_create(client: AsyncClient) -> None:
    headers = {
        "X-Auth-Request-Service": "some-service",
        "X-Auth-Request-User": "user",
    }
    r = await client.get("/wobbly/jobs", headers=headers)
    assert r.status_code == 200
    assert r.json() == []

    destruction = datetime.now(tz=UTC) + timedelta(days=30)
    r = await client.post(
        "/wobbly/jobs",
        json={
            "json_parameters": {"foo": "bar", "baz": "other"},
            "destruction_time": destruction.isoformat(),
        },
        headers=headers,
    )
    assert r.status_code == 201
    assert r.headers["Location"] == "https://example.com/wobbly/jobs/1"
    job = r.json()
    assert job == {
        "id": "1",
        "service": "some-service",
        "owner": "user",
        "phase": "PENDING",
        "json_parameters": {"foo": "bar", "baz": "other"},
        "creation_time": ANY,
        "destruction_time": destruction.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    now = datetime.now(tz=UTC)
    creation = datetime.fromisoformat(job["creation_time"])
    assert now - timedelta(seconds=5) <= creation <= now

    r = await client.get("/wobbly/jobs/1", headers=headers)
    assert r.status_code == 200
    assert r.json() == job

    r = await client.get("/wobbly/jobs", headers=headers)
    assert r.status_code == 200
    assert r.json() == [job]

    r = await client.post(
        "/wobbly/jobs",
        json={
            "json_parameters": {"foo": "bar", "baz": "other"},
            "run_id": "big-job",
            "destruction_time": destruction.isoformat(),
            "execution_duration": 600,
        },
        headers={
            "X-Auth-Request-Service": "some-service",
            "X-Auth-Request-User": "other-user",
        },
    )
    assert r.status_code == 201
    assert r.headers["Location"] == "https://example.com/wobbly/jobs/2"
    other_job = r.json()
    assert other_job == {
        "id": "2",
        "service": "some-service",
        "owner": "other-user",
        "phase": "PENDING",
        "run_id": "big-job",
        "json_parameters": {"foo": "bar", "baz": "other"},
        "creation_time": ANY,
        "destruction_time": destruction.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "execution_duration": 600,
    }

    # Job listings are limited by user.
    r = await client.get("/wobbly/jobs", headers=headers)
    assert r.status_code == 200
    assert r.json() == [job]
    r = await client.get(
        "/wobbly/jobs",
        headers={
            "X-Auth-Request-Service": "some-service",
            "X-Auth-Request-User": "other-user",
        },
    )
    assert r.status_code == 200
    assert r.json() == [other_job]

    # Job listings are also limited by service.
    r = await client.get(
        "/wobbly/jobs",
        headers={
            "X-Auth-Request-Service": "other-service",
            "X-Auth-Request-User": "user",
        },
    )
    assert r.status_code == 200
    assert r.json() == []


@pytest.mark.asyncio
async def test_completed(client: AsyncClient) -> None:
    headers = {
        "X-Auth-Request-Service": "some-service",
        "X-Auth-Request-User": "user",
    }
    destruction = datetime.now(tz=UTC) + timedelta(days=30)
    r = await client.post(
        "/wobbly/jobs",
        json={
            "json_parameters": {"foo": "bar", "baz": "other"},
            "destruction_time": destruction.isoformat(),
        },
        headers=headers,
    )
    assert r.status_code == 201
    assert r.json()["phase"] == "PENDING"
    url = r.headers["Location"]

    r = await client.patch(
        url,
        json={"phase": "QUEUED", "message_id": "some-message-id"},
        headers=headers,
    )
    assert r.status_code == 200
    job = r.json()
    assert job["phase"] == "QUEUED"
    assert job["message_id"] == "some-message-id"
    assert "start_time" not in job
    r = await client.get("/wobbly/jobs/1", headers=headers)
    assert r.status_code == 200
    assert r.json() == job

    now = datetime.now(tz=UTC)
    r = await client.patch(
        url,
        json={"phase": "EXECUTING", "start_time": now.isoformat()},
        headers=headers,
    )
    assert r.status_code == 200
    job = r.json()
    assert job["phase"] == "EXECUTING"
    assert job["start_time"] == now.strftime("%Y-%m-%dT%H:%M:%SZ")
    assert "results" not in job
    r = await client.get(url, headers=headers)
    assert r.status_code == 200
    assert r.json() == job

    results = [
        {"id": "image", "url": "https://example.com/image"},
        {
            "id": "map",
            "url": "https://example.com/map",
            "size": 124513,
            "mime_type": "application/fits",
        },
    ]
    r = await client.patch(
        url, json={"phase": "COMPLETED", "results": results}, headers=headers
    )
    assert r.status_code == 200
    job = r.json()
    assert job["phase"] == "COMPLETED"
    end_time = datetime.fromisoformat(job["end_time"])
    now = datetime.now(tz=UTC)
    assert now - timedelta(seconds=5) <= end_time <= now
    assert job["results"] == results

    # Check that the correct metrics events were published.
    manager = context_dependency._events
    assert isinstance(manager.created, MockEventPublisher)
    manager.created.published.assert_published_all(
        [{"service": "some-service", "username": "user"}]
    )
    assert isinstance(manager.queued, MockEventPublisher)
    manager.queued.published.assert_published_all(
        [{"service": "some-service", "username": "user"}]
    )
    assert isinstance(manager.completed, MockEventPublisher)
    manager.completed.published.assert_published_all(
        [{"service": "some-service", "username": "user", "elapsed": NOT_NONE}]
    )


@pytest.mark.asyncio
async def test_failed(client: AsyncClient) -> None:
    headers = {
        "X-Auth-Request-Service": "some-service",
        "X-Auth-Request-User": "user",
    }
    destruction = datetime.now(tz=UTC) + timedelta(days=30)
    r = await client.post(
        "/wobbly/jobs",
        json={
            "json_parameters": {"foo": "bar", "baz": "other"},
            "destruction_time": destruction.isoformat(),
        },
        headers=headers,
    )
    assert r.status_code == 201
    assert r.json()["phase"] == "PENDING"
    url = r.headers["Location"]
    r = await client.patch(
        url,
        json={"phase": "QUEUED", "message_id": "some-message-id"},
        headers=headers,
    )
    assert r.status_code == 200
    now = datetime.now(tz=UTC)
    r = await client.patch(
        url,
        json={"phase": "EXECUTING", "start_time": now.isoformat()},
        headers=headers,
    )
    assert r.status_code == 200

    error = {
        "type": ErrorType.TRANSIENT,
        "code": "SomeError",
        "message": "Error message",
        "detail": "Some more details",
    }
    r = await client.patch(
        url, json={"phase": "ERROR", "errors": [error]}, headers=headers
    )
    assert r.status_code == 200
    job = r.json()
    assert job["phase"] == "ERROR"
    assert job["errors"] == [error]
    end_time = datetime.fromisoformat(job["end_time"])
    now = datetime.now(tz=UTC)
    assert now - timedelta(seconds=5) <= end_time <= now

    # Check that the correct metrics events were published.
    manager = context_dependency._events
    assert isinstance(manager.created, MockEventPublisher)
    manager.created.published.assert_published_all(
        [{"service": "some-service", "username": "user"}]
    )
    assert isinstance(manager.queued, MockEventPublisher)
    manager.queued.published.assert_published_all(
        [{"service": "some-service", "username": "user"}]
    )
    assert isinstance(manager.failed, MockEventPublisher)
    manager.failed.published.assert_published_all(
        [
            {
                "service": "some-service",
                "username": "user",
                "error_code": "SomeError",
                "elapsed": NOT_NONE,
            }
        ]
    )


@pytest.mark.asyncio
async def test_aborted(client: AsyncClient) -> None:
    headers = {
        "X-Auth-Request-Service": "some-service",
        "X-Auth-Request-User": "user",
    }
    destruction = datetime.now(tz=UTC) + timedelta(days=30)
    r = await client.post(
        "/wobbly/jobs",
        json={
            "json_parameters": {"foo": "bar", "baz": "other"},
            "destruction_time": destruction.isoformat(),
        },
        headers=headers,
    )
    assert r.status_code == 201
    assert r.json()["phase"] == "PENDING"
    url = r.headers["Location"]

    r = await client.patch(url, json={"phase": "ABORTED"}, headers=headers)
    assert r.status_code == 200
    job = r.json()
    assert job["phase"] == "ABORTED"
    assert "end_time" not in job

    # Check that the correct metrics events were published.
    manager = context_dependency._events
    assert isinstance(manager.created, MockEventPublisher)
    manager.created.published.assert_published_all(
        [{"service": "some-service", "username": "user"}]
    )
    assert isinstance(manager.aborted, MockEventPublisher)
    manager.aborted.published.assert_published_all(
        [{"service": "some-service", "username": "user"}]
    )


@pytest.mark.asyncio
async def test_update(client: AsyncClient) -> None:
    headers = {
        "X-Auth-Request-Service": "some-service",
        "X-Auth-Request-User": "user",
    }
    destruction = datetime.now(tz=UTC) + timedelta(days=30)
    r = await client.post(
        "/wobbly/jobs",
        json={
            "json_parameters": {"foo": "bar", "baz": "other"},
            "destruction_time": destruction.isoformat(),
        },
        headers=headers,
    )
    assert r.status_code == 201
    job = r.json()
    url = r.headers["Location"]

    destruction = datetime.now(tz=UTC) + timedelta(days=60)
    r = await client.patch(
        url,
        json={
            "destruction_time": destruction.isoformat(),
            "execution_duration": 300.5,
        },
        headers=headers,
    )
    assert r.status_code == 200
    job["destruction_time"] = destruction.strftime("%Y-%m-%dT%H:%M:%SZ")
    job["execution_duration"] = 300
    assert r.json() == job
    r = await client.get(url, headers=headers)
    assert r.status_code == 200
    assert r.json() == job


@pytest.mark.asyncio
async def test_errors(client: AsyncClient) -> None:
    destruction = datetime.now(tz=UTC) + timedelta(days=30)
    headers = {
        "X-Auth-Request-Service": "some-service",
        "X-Auth-Request-User": "user",
    }
    r = await client.post(
        "/wobbly/jobs",
        json={
            "json_parameters": {},
            "destruction_time": destruction.isoformat(),
        },
        headers=headers,
    )
    assert r.status_code == 201
    r = await client.get("/wobbly/jobs/1", headers=headers)
    assert r.status_code == 200

    # Methods require both service and user to be set.
    for method, url, kwargs in (
        (client.get, "/wobbly/jobs", {}),
        (
            client.post,
            "/wobbly/jobs",
            {
                "json": {
                    "json_parameters": {},
                    "destruction_time": destruction.isoformat(),
                }
            },
        ),
        (client.get, "/wobbly/jobs/1", {}),
        (client.delete, "/wobbly/jobs/1", {}),
        (client.patch, "/wobbly/jobs/1", {"json": {"phase": "ABORTED"}}),
    ):
        r = await method(url, **kwargs)
        assert r.status_code == 422
        r = await method(
            url,
            headers={"X-Auth-Request-Service": "some-service"},
            **kwargs,
        )
        assert r.status_code == 422
        r = await method(
            url,
            headers={"X-Auth-Request-User": "some-user"},
            **kwargs,
        )
        assert r.status_code == 422

    # Access to existing jobs is limited by both service and user.
    for method, url, kwargs in (
        (client.get, "/wobbly/jobs/1", {}),
        (client.delete, "/wobbly/jobs/1", {}),
        (client.patch, "/wobbly/jobs/1", {"json": {"phase": "ABORTED"}}),
    ):
        r = await method(
            url,
            headers={
                "X-Auth-Request-Service": "some-service",
                "X-Auth-Request-User": "other-user",
            },
            **cast("dict[str, Any]", kwargs),
        )
        assert r.status_code == 404
        r = await method(
            url,
            headers={
                "X-Auth-Request-Service": "other-service",
                "X-Auth-Request-User": "user",
            },
            **cast("dict[str, Any]", kwargs),
        )
        assert r.status_code == 404

    # Weird limits on searches not allowed.
    r = await client.get("/wobbly/jobs", params={"limit": 0}, headers=headers)
    assert r.status_code == 422
    r = await client.get("/wobbly/jobs", params={"limit": -1}, headers=headers)
    assert r.status_code == 422

    # Unsupported updates are rejected.
    r = await client.patch(
        "/wobbly/jobs/1", json={"phase": "SUSPENDED"}, headers=headers
    )
    assert r.status_code == 422


async def create_jobs(
    client: AsyncClient, headers: dict[str, str], count: int
) -> list[dict[str, Any]]:
    """Create some test jobs and return the JSON representation."""
    now = datetime.now(tz=UTC)
    destruction = now + timedelta(days=30)
    jobs = []
    for n in range(count):
        r = await client.post(
            "/wobbly/jobs",
            json={
                "json_parameters": {"id": n},
                "destruction_time": destruction.isoformat(),
            },
            headers=headers,
        )
        assert r.status_code == 201
        jobs.append(r.json())
    return jobs


@pytest.mark.asyncio
async def test_pagination(client: AsyncClient) -> None:
    headers = {
        "X-Auth-Request-Service": "some-service",
        "X-Auth-Request-User": "user",
    }
    await create_jobs(client, headers, 10)
    expected = list(range(10))
    expected.reverse()

    # Simple job list without pagination.
    r = await client.get("/wobbly/jobs", headers=headers)
    assert r.status_code == 200
    assert [j["json_parameters"]["id"] for j in r.json()] == expected
    assert "Link" not in r.headers

    # Limit larger than the nubmer of jobs should return all jobs.
    r = await client.get("/wobbly/jobs", params={"limit": 20}, headers=headers)
    assert r.status_code == 200
    assert [j["json_parameters"]["id"] for j in r.json()] == expected
    link_data = PaginationLinkData.from_header(r.headers["Link"])
    assert not link_data.next_url
    assert not link_data.prev_url

    # Paginated queries.
    r = await client.get("/wobbly/jobs", params={"limit": 5}, headers=headers)
    assert r.status_code == 200
    assert [j["json_parameters"]["id"] for j in r.json()] == expected[:5]
    link_data = PaginationLinkData.from_header(r.headers["Link"])
    assert not link_data.prev_url
    assert link_data.first_url == "https://example.com/wobbly/jobs?limit=5"
    assert link_data.next_url
    r = await client.get(link_data.next_url, headers=headers)
    assert r.status_code == 200
    assert [j["json_parameters"]["id"] for j in r.json()] == expected[5:]
    link_data = PaginationLinkData.from_header(r.headers["Link"])
    assert not link_data.next_url
    assert link_data.first_url == "https://example.com/wobbly/jobs?limit=5"
    assert link_data.prev_url
    prev_url_params = parse_qs(urlparse(link_data.prev_url).query)
    params = {k: v[0] for k, v in prev_url_params.items()}
    params["limit"] = "1"
    r = await client.get(link_data.prev_url, params=params, headers=headers)
    assert r.status_code == 200
    assert [j["json_parameters"]["id"] for j in r.json()] == [expected[4]]


@pytest.mark.asyncio
async def test_pagination_phase(client: AsyncClient) -> None:
    headers = {
        "X-Auth-Request-Service": "some-service",
        "X-Auth-Request-User": "user",
    }
    await create_jobs(client, headers, 10)
    expected = list(range(10))
    expected.reverse()

    # Change the phase of one job.
    r = await client.patch(
        "/wobbly/jobs/1",
        json={"phase": "QUEUED", "message_id": "some-message-id"},
        headers=headers,
    )
    assert r.status_code == 200

    # Paginated queries by phase.
    r = await client.get("/wobbly/jobs", params={"limit": 5}, headers=headers)
    assert r.status_code == 200
    assert [j["json_parameters"]["id"] for j in r.json()] == expected[:5]
    r = await client.get(
        "/wobbly/jobs", params={"limit": 5, "phase": "QUEUED"}, headers=headers
    )
    assert r.status_code == 200
    assert [j["json_parameters"]["id"] for j in r.json()] == [expected[9]]
    link_data = PaginationLinkData.from_header(r.headers["Link"])
    assert not link_data.next_url
    assert not link_data.prev_url

    # Unpaginated query by phase.
    r = await client.get(
        "/wobbly/jobs", params={"phase": "PENDING"}, headers=headers
    )
    assert r.status_code == 200
    assert [j["json_parameters"]["id"] for j in r.json()] == expected[:9]
    assert "Link" not in r.headers

    # Paginated query with empty results.
    r = await client.get(
        "/wobbly/jobs",
        params={"phase": "ABORTED", "limit": 10},
        headers=headers,
    )
    assert r.status_code == 200
    assert r.json() == []
    link_data = PaginationLinkData.from_header(r.headers["Link"])
    assert not link_data.next_url
    assert not link_data.prev_url


@pytest.mark.asyncio
async def test_pagination_since(client: AsyncClient) -> None:
    headers = {
        "X-Auth-Request-Service": "some-service",
        "X-Auth-Request-User": "user",
    }
    await create_jobs(client, headers, 10)
    now = datetime.now(tz=UTC)
    expected = list(range(10))
    expected.reverse()

    # Tweak the creation time of one job so that there's something
    # interesting to query.
    stmt = select(SQLJob).where(SQLJob.id == 2)
    async for session in db_session_dependency():
        async with session.begin():
            result = await session.execute(stmt)
            job = result.scalar_one()
            new_creation = datetime.now(tz=UTC) - timedelta(minutes=5)
            job.creation_time = datetime_to_db(new_creation)

    # Search by since.
    r = await client.get(
        "/wobbly/jobs",
        params={"since": (now - timedelta(seconds=5)).isoformat()},
        headers=headers,
    )
    assert r.status_code == 200
    since_expected = [*expected[:8], expected[9]]
    assert [j["json_parameters"]["id"] for j in r.json()] == since_expected

    # Search with a since parameter that cannot be satisfied.
    r = await client.get(
        "/wobbly/jobs",
        params={"since": (now + timedelta(minutes=5)).isoformat()},
        headers=headers,
    )
    assert r.status_code == 200
    assert r.json() == []


@pytest.mark.asyncio
async def test_pagination_empty(client: AsyncClient) -> None:
    headers = {
        "X-Auth-Request-Service": "some-service",
        "X-Auth-Request-User": "user",
    }
    r = await client.get("/wobbly/jobs", params={"limit": 1}, headers=headers)
    assert r.status_code == 200
    assert r.json() == []
    link_data = PaginationLinkData.from_header(r.headers["Link"])
    assert not link_data.next_url
    assert not link_data.prev_url
