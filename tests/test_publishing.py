from dataclasses import FrozenInstanceError

import pytest

from datasource_kit.publishing import PublishRequest, PublishReceipt, Publisher


@pytest.mark.parametrize("status", ["ok", "", "pending"])
def test_receipt_rejects_ambiguous_status(status):
    with pytest.raises(ValueError):
        PublishReceipt(operation_id="op-1", status=status)


@pytest.mark.parametrize("operation_id", ["", "  "])
def test_request_requires_correlation_id(operation_id):
    with pytest.raises(ValueError):
        PublishRequest(operation_id=operation_id, target="target", payload={})


def test_optional_publisher_is_structural_and_keeps_payload_opaque():
    payload = object()
    request = PublishRequest(operation_id="op-1", target="destination", payload=payload)

    class Source:
        def publish(self, request: PublishRequest) -> PublishReceipt:
            assert request.payload is payload
            return PublishReceipt(operation_id=request.operation_id, status="unknown")

    source = Source()
    assert isinstance(source, Publisher)
    assert not isinstance(object(), Publisher)
    receipt = source.publish(request)
    assert receipt.status == "unknown"
    assert receipt.external_id is None
    with pytest.raises(FrozenInstanceError):
        receipt.status = "confirmed"
