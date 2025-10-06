"""Configuration definition."""

from __future__ import annotations

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from safir.logging import LogLevel, Profile, configure_logging
from safir.metrics import MetricsConfiguration, metrics_configuration_factory
from safir.pydantic import EnvAsyncPostgresDsn

__all__ = ["Config", "config"]


class Config(BaseSettings):
    """Configuration for wobbly."""

    model_config = SettingsConfigDict(
        env_prefix="WOBBLY_", case_sensitive=False
    )

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

    log_profile: Profile = Field(
        Profile.development, title="Application logging profile"
    )

    metrics: MetricsConfiguration = Field(
        default_factory=metrics_configuration_factory,
        title="Metrics configuration",
    )

    name: str = Field("wobbly", title="Name of application")

    path_prefix: str = Field("/wobbly", title="URL prefix for application")

    slack_webhook: SecretStr | None = Field(
        None,
        title="Slack webhook for alerts",
        description="If set, alerts will be posted to this Slack webhook",
    )


config = Config()
"""Configuration for wobbly."""


# Ensure this is always run so that command-line tools can rely on it as well.
configure_logging(
    profile=config.log_profile, log_level=config.log_level, name="wobbly"
)
