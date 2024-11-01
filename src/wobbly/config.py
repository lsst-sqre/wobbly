"""Configuration definition."""

from __future__ import annotations

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from safir.logging import LogLevel, Profile
from safir.pydantic import EnvAsyncPostgresDsn

__all__ = ["Config", "config"]


class Config(BaseSettings):
    """Configuration for wobbly."""

    database_url: EnvAsyncPostgresDsn = Field(
        ...,
        title="PostgreSQL DSN",
        description="DSN of PostgreSQL database for UWS job tracking",
    )

    database_password: SecretStr | None = Field(
        None, title="Password for UWS job database"
    )

    log_level: LogLevel = Field(
        LogLevel.INFO, title="Log level of the application's logger"
    )

    name: str = Field("wobbly", title="Name of application")

    path_prefix: str = Field("/wobbly", title="URL prefix for application")

    profile: Profile = Field(
        Profile.development, title="Application logging profile"
    )

    model_config = SettingsConfigDict(
        env_prefix="WOBBLY_", case_sensitive=False
    )


config = Config()
"""Configuration for wobbly."""
