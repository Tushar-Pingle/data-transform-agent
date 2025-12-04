"""
Configuration settings for Data Transform Agent.

Uses Pydantic Settings for environment variable management
with validation and type safety.
"""

from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator


class DatabricksSettings(BaseSettings):
    """Databricks connection settings."""
    
    model_config = SettingsConfigDict(
        env_prefix="DATABRICKS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
    
    host: str = Field(
        ...,
        description="Databricks workspace hostname (without https://)",
        examples=["community.cloud.databricks.com"],
    )
    token: str = Field(
        ...,
        description="Personal access token for authentication",
    )
    warehouse_id: str = Field(
        ...,
        description="SQL Warehouse ID for query execution",
    )
    catalog: str = Field(
        default="main",
        description="Default catalog to use",
    )
    
    @field_validator("host")
    @classmethod
    def clean_host(cls, v: str) -> str:
        """Remove https:// prefix if present."""
        return v.replace("https://", "").replace("http://", "").rstrip("/")
    
    @property
    def http_path(self) -> str:
        """Generate the HTTP path for SQL Warehouse connection."""
        return f"/sql/1.0/warehouses/{self.warehouse_id}"


class AnthropicSettings(BaseSettings):
    """Anthropic (Claude) API settings."""
    
    model_config = SettingsConfigDict(
        env_prefix="ANTHROPIC_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
    
    api_key: str = Field(
        ...,
        description="Anthropic API key",
    )
    model: str = Field(
        default="claude-sonnet-4-20250514",
        description="Model to use for SQL generation",
    )
    max_tokens: int = Field(
        default=4096,
        description="Maximum tokens in response",
    )


class AppSettings(BaseSettings):
    """Application-level settings."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
    
    log_level: str = Field(
        default="INFO",
        description="Logging level",
    )
    default_dry_run: bool = Field(
        default=True,
        description="Enable dry-run mode by default",
    )
    default_target_layer: str = Field(
        default="silver",
        description="Default target layer for transforms",
    )


# Convenience functions
def get_databricks_settings() -> DatabricksSettings:
    """Get Databricks settings from environment."""
    return DatabricksSettings()


def get_anthropic_settings() -> AnthropicSettings:
    """Get Anthropic settings from environment."""
    return AnthropicSettings()


def get_app_settings() -> AppSettings:
    """Get application settings from environment."""
    return AppSettings()