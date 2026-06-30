"""Custom exceptions for datasource-kit."""

from __future__ import annotations

__all__ = [
    "DatasourceKitError",
    "ProfileError",
    "ProviderError",
    "RegistryError",
    "RuntimeStepError",
    "ValidationError",
]


class DatasourceKitError(Exception):
    """Base exception for all datasource-kit errors."""


class ProfileError(DatasourceKitError):
    """Raised when a source profile is invalid or references an unknown provider."""


class ValidationError(DatasourceKitError):
    """Raised when a pure-data shape is missing a load-bearing field."""


class RegistryError(DatasourceKitError):
    """Raised when provider registration or lookup fails."""


class ProviderError(DatasourceKitError):
    """Raised when a provider cannot satisfy the requested pipeline step."""


class RuntimeStepError(DatasourceKitError):
    """Raised when the ingest runtime fails a pipeline step."""
