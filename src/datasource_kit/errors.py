"""Custom exceptions for datasource-kit."""

from __future__ import annotations

__all__ = [
    "DatasourceKitError",
    "ProfileError",
    "ProviderError",
    "RegistryError",
    "RuntimeStepError",
    "SourceError",
    "TransportError",
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


class SourceError(DatasourceKitError):
    """A Fetcher or Enumerator call against the upstream source failed.

    Generic base for any error that originates from the datasource itself
    (as opposed to kit-internal errors like :class:`ProfileError`).
    Consumer code should subclass this for domain-specific source errors;
    subclasses may add arbitrary typed attributes via their own
    ``__init__`` without interfering with the base.
    """


class TransportError(SourceError):
    """The connectivity or protocol layer of a source call failed.

    Covers timeouts, DNS failures, TLS errors, connection resets, and
    similar transport-level problems.  A more specific subset of
    :class:`SourceError` for cases where the failure is clearly at the
    network/transport layer rather than a logical error from the source.
    """
