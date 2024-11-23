"""Extra type definitions for Wobbly."""

from __future__ import annotations

from typing import Any, TypeAlias

__all__ = ["JobParameters"]

JobParameters: TypeAlias = dict[str, Any] | list[str]
"""Possible types of job parameters.

This can either be a serialized parameters model (the `dict` case), or a list
of old-style input parameters, which are stored as simple strings.
"""
