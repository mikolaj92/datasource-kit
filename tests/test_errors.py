"""Tests for datasource_kit.errors – SourceError / TransportError hierarchy."""

from __future__ import annotations

import pytest

from datasource_kit.errors import (
    DatasourceKitError,
    ProfileError,
    SourceError,
    TransportError,
)


# ------------------------------------------------------------------
# Hierarchy & isinstance checks
# ------------------------------------------------------------------


class TestSourceError:
    """SourceError is a DatasourceKitError usable for generic source failures."""

    def test_is_subclass_of_kit_error(self) -> None:
        assert issubclass(SourceError, DatasourceKitError)

    def test_is_not_subclass_of_profile_error(self) -> None:
        assert not issubclass(SourceError, ProfileError)

    def test_raise_and_catch_as_kit_error(self) -> None:
        with pytest.raises(DatasourceKitError):
            raise SourceError("upstream unreachable")

    def test_message_preserved(self) -> None:
        err = SourceError("bad response from API")
        assert str(err) == "bad response from API"
        assert err.args == ("bad response from API",)


class TestTransportError:
    """TransportError is the connectivity/protocol subset of SourceError."""

    def test_is_subclass_of_source_error(self) -> None:
        assert issubclass(TransportError, SourceError)

    def test_is_subclass_of_kit_error(self) -> None:
        assert issubclass(TransportError, DatasourceKitError)

    def test_catch_as_source_error(self) -> None:
        with pytest.raises(SourceError):
            raise TransportError("connection reset")

    def test_catch_as_kit_error(self) -> None:
        with pytest.raises(DatasourceKitError):
            raise TransportError("timeout")

    def test_message_preserved(self) -> None:
        err = TransportError("DNS resolution failed")
        assert str(err) == "DNS resolution failed"


# ------------------------------------------------------------------
# Consumer subclassing (the main use-case from the issue)
# ------------------------------------------------------------------


class TestConsumerSubclassing:
    """Consumers should be able to subclass and add structured context."""

    def test_plain_subclass(self) -> None:
        class CBOSATransportError(TransportError):
            pass

        err = CBOSATransportError("CBOSA timeout")
        assert isinstance(err, TransportError)
        assert isinstance(err, SourceError)
        assert isinstance(err, DatasourceKitError)
        assert str(err) == "CBOSA timeout"

    def test_subclass_with_extra_fields(self) -> None:
        """Mirrors the SAOSDetailTimeoutError pattern from the issue."""

        class SAOSDetailTimeoutError(TransportError):
            def __init__(
                self,
                judgment_id: str,
                attempts: int,
                timeout: float,
            ) -> None:
                self.judgment_id = judgment_id
                self.attempts = attempts
                self.timeout = timeout
                super().__init__(
                    f"SAOS detail {judgment_id} timed out after "
                    f"{attempts} attempts ({timeout}s)"
                )

        err = SAOSDetailTimeoutError("II-123/45", attempts=3, timeout=30.0)

        # Structured fields are accessible
        assert err.judgment_id == "II-123/45"
        assert err.attempts == 3
        assert err.timeout == 30.0

        # Still catchable via the hierarchy
        assert isinstance(err, TransportError)
        assert isinstance(err, SourceError)
        assert isinstance(err, DatasourceKitError)
        assert "II-123/45" in str(err)

    def test_source_error_subclass(self) -> None:
        """A non-transport source error (logical failure from the source)."""

        class ISAPValidationError(SourceError):
            pass

        err = ISAPValidationError("invalid record schema")
        assert isinstance(err, SourceError)
        assert isinstance(err, DatasourceKitError)
        assert not isinstance(err, TransportError)


# ------------------------------------------------------------------
# Package-level re-export
# ------------------------------------------------------------------


def test_importable_from_package() -> None:
    import datasource_kit

    assert datasource_kit.SourceError is SourceError
    assert datasource_kit.TransportError is TransportError
