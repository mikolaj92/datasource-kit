"""Custom exceptions for datasource-kit."""

from __future__ import annotations

__all__ = ["DatasourceKitError", "ProfileError"]


class DatasourceKitError(Exception):
    """Base exception for all datasource-kit errors."""


class ProfileError(DatasourceKitError):
    """Raised when a source profile is invalid or references an unknown provider."""
