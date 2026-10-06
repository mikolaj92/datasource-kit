"""Optional domain-blind publication seam; no scheduling, grants or retries.

An operation ID correlates intent and outcome, not an idempotency guarantee.
Unknown outcomes require consumer/provider reconciliation before any retry.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class PublishRequest:
    operation_id: str
    target: object
    payload: object

    def __post_init__(self) -> None:
        if not self.operation_id.strip():
            raise ValueError("operation_id must be non-empty")


@dataclass(frozen=True, slots=True)
class PublishReceipt:
    operation_id: str
    status: Literal["confirmed", "rejected", "unknown"]
    external_id: str | None = None
    details: object = None

    def __post_init__(self) -> None:
        if not self.operation_id.strip():
            raise ValueError("operation_id must be non-empty")
        if self.status not in {"confirmed", "rejected", "unknown"}:
            raise ValueError("unsupported publication status")


@runtime_checkable
class Publisher(Protocol):
    """Optional capability, independent of all read protocols."""

    def publish(self, request: PublishRequest) -> PublishReceipt: ...
