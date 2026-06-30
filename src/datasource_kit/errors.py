"""Typed error hierarchy for datasource-kit."""

from __future__ import annotations

__all__ = ["ProfileError"]


class ProfileError(Exception):
    """Raised when a profile folder cannot be loaded or fails validation."""
