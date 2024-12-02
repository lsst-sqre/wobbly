"""Tests for Wobbly administrative API."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_admin(client: AsyncClient) -> None:
    destruction = datetime.now(tz=UTC) + timedelta(days=30)
    r = await client.post(
        "/wobbly/jobs",
        json={
            "parameters": {"foo": "bar", "baz": "other"},
            "destruction_time": destruction.isoformat(),
        },
        headers={
            "X-Auth-Request-Service": "some-service",
            "X-Auth-Request-User": "user",
        },
    )
    assert r.status_code == 201
    job_one = r.json()
    r = await client.post(
        "/wobbly/jobs",
        json={
            "parameters": {"foo": "bar", "baz": "other"},
            "destruction_time": destruction.isoformat(),
        },
        headers={
            "X-Auth-Request-Service": "other-service",
            "X-Auth-Request-User": "other-user",
        },
    )
    assert r.status_code == 201
    job_two = r.json()

    r = await client.get("/wobbly/admin/services")
    assert r.status_code == 200
    assert r.json() == ["other-service", "some-service"]

    r = await client.get("/wobbly/admin/services/some-service/users")
    assert r.status_code == 200
    assert r.json() == ["user"]

    r = await client.get("/wobbly/admin/services/some-service/users/user/jobs")
    assert r.status_code == 200
    assert r.json() == [job_one]

    r = await client.get(
        "/wobbly/admin/services/some-service/users/user/jobs/1"
    )
    assert r.status_code == 200
    assert r.json() == job_one

    r = await client.get("/wobbly/admin/services/bad-service/users")
    assert r.status_code == 200
    assert r.json() == []
    r = await client.get(
        "/wobbly/admin/services/other-service/users/user/jobs"
    )
    assert r.status_code == 200
    assert r.json() == []
    r = await client.get(
        "/wobbly/admin/services/other-service/users/user/jobs/1"
    )
    assert r.status_code == 404
    r = await client.get(
        "/wobbly/admin/services/some-service/users/bad-user/jobs"
    )
    assert r.status_code == 200
    assert r.json() == []
    r = await client.get(
        "/wobbly/admin/services/some-service/users/bad-user/jobs/1"
    )
    assert r.status_code == 404

    r = await client.get("/wobbly/admin/users")
    assert r.status_code == 200
    assert r.json() == ["other-user", "user"]

    r = await client.get("/wobbly/admin/users/other-user/jobs")
    assert r.status_code == 200
    assert r.json() == [job_two]

    r = await client.get("/wobbly/admin/users/bad-user/jobs")
    assert r.status_code == 200
    assert r.json() == []

    r = await client.get("/wobbly/admin/jobs")
    assert r.status_code == 200
    assert r.json() == [job_two, job_one]
