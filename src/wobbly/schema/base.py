"""Declarative base for the UWS database schema."""

from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase

__all__ = ["SchemaBase"]


class SchemaBase(DeclarativeBase):
    """SQLAlchemy declarative base for the UWS database schema."""
