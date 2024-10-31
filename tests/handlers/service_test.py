"""Tests for the service API to Wobbly."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, cast
from unittest.mock import ANY

import pytest
from httpx import AsyncClient
from vo_models.uws.types import ErrorType


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
            "parameters": {"foo": "bar", "baz": "other"},
            "destruction_time": destruction.isoformat(),
        },
        headers=headers,
    )
    assert r.status_code == 201
    assert r.headers["Location"] == "https://example.com/wobbly/jobs/1"
    job = r.json()
    assert job == {
        "id": "1",
        "owner": "user",
        "phase": "PENDING",
        "parameters": {"foo": "bar", "baz": "other"},
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
            "parameters": ["FOO BAR", "BAZ OTHER"],
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
        "owner": "other-user",
        "phase": "PENDING",
        "run_id": "big-job",
        "parameters": ["FOO BAR", "BAZ OTHER"],
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
            "parameters": {"foo": "bar", "baz": "other"},
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
            "parameters": {"foo": "bar", "baz": "other"},
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
        url, json={"phase": "ERROR", "error": error}, headers=headers
    )
    assert r.status_code == 200
    job = r.json()
    assert job["phase"] == "ERROR"
    assert job["error"] == error
    end_time = datetime.fromisoformat(job["end_time"])
    now = datetime.now(tz=UTC)
    assert now - timedelta(seconds=5) <= end_time <= now


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
            "parameters": {"foo": "bar", "baz": "other"},
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
            "parameters": {"foo": "bar", "baz": "other"},
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
    r = await client.post(
        "/wobbly/jobs",
        json={
            "parameters": [],
            "destruction_time": destruction.isoformat(),
        },
        headers={
            "X-Auth-Request-Service": "some-service",
            "X-Auth-Request-User": "user",
        },
    )
    assert r.status_code == 201
    r = await client.get(
        "/wobbly/jobs/1",
        headers={
            "X-Auth-Request-Service": "some-service",
            "X-Auth-Request-User": "user",
        },
    )
    assert r.status_code == 200

    # Methods require both service and user to be set.
    for method, url, kwargs in (
        (client.get, "/wobbly/jobs", {}),
        (
            client.post,
            "/wobbly/jobs",
            {
                "json": {
                    "parameters": [],
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
            **cast(dict[str, Any], kwargs),
        )
        assert r.status_code == 404
        r = await method(
            url,
            headers={
                "X-Auth-Request-Service": "other-service",
                "X-Auth-Request-User": "user",
            },
            **cast(dict[str, Any], kwargs),
        )
        assert r.status_code == 404
