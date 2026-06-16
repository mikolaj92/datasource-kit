from __future__ import annotations

import pytest

from datasource_kit import Registry


class _Source:
    def __init__(self, name: str) -> None:
        self.name = name


def test_register_by_name_attribute_and_get():
    reg: Registry[_Source] = Registry()
    src = _Source("clp")
    reg.register(src)
    assert reg.get("clp") is src
    assert "clp" in reg
    assert len(reg) == 1


def test_register_with_explicit_name():
    reg: Registry[object] = Registry()
    obj = object()
    reg.register(obj, name="manual")
    assert reg.get("manual") is obj


def test_duplicate_registration_rejected():
    reg: Registry[_Source] = Registry()
    reg.register(_Source("dup"))
    with pytest.raises(KeyError):
        reg.register(_Source("dup"))


def test_missing_name_rejected():
    reg: Registry[object] = Registry()
    with pytest.raises(ValueError):
        reg.register(object())


def test_get_unknown_raises():
    reg: Registry[_Source] = Registry()
    with pytest.raises(KeyError):
        reg.get("nope")


def test_names_and_items_sorted_and_iter():
    reg: Registry[_Source] = Registry()
    reg.register(_Source("b"))
    reg.register(_Source("a"))
    assert reg.names() == ["a", "b"]
    assert [n for n, _ in reg.items()] == ["a", "b"]
    assert [s.name for s in reg] == ["a", "b"]
