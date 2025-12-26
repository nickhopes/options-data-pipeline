"""Configuration management using Pydantic Settings."""

import os
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """Database connection settings."""

    model_config = SettingsConfigDict(env_prefix="DB_")

    host: str = Field(default="62.171.161.104", description="Database host")
    port: int = Field(default=5432, description="Database port")
    name: str = Field(default="hexdb", description="Database name")
    user: str = Field(default="hexuser", description="Database user")
    password: str = Field(default="", description="Database password")

    @property
    def connection_string(self) -> str:
        """Get PostgreSQL connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

    @property
    def async_connection_string(self) -> str:
        """Get async PostgreSQL connection string."""
        return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


class PrefectSettings(BaseSettings):
    """Prefect configuration settings."""

    model_config = SettingsConfigDict(env_prefix="PREFECT_")

    api_url: str = Field(
        default="http://localhost:4200/api",
        description="Prefect API URL"
    )


class LoggingSettings(BaseSettings):
    """Logging configuration."""

    level: str = Field(default="INFO", alias="LOG_LEVEL")
    format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log format"
    )


class Settings(BaseSettings):
    """Main application settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # Sub-settings
    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    prefect: PrefectSettings = Field(default_factory=PrefectSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)

    # Application settings
    environment: str = Field(default="production", alias="ENVIRONMENT")


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Convenience function for database config dict (compatible with existing scripts)
def get_db_config() -> dict:
    """Get database configuration as a dictionary (for psycopg2)."""
    settings = get_settings()
    return {
        "host": settings.db.host,
        "port": settings.db.port,
        "database": settings.db.name,
        "user": settings.db.user,
        "password": settings.db.password,
    }
