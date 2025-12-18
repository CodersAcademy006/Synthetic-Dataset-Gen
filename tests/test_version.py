import pytest
from engine import version

def test_resolve_version_with_run_id():
    v = version.resolve_version("dataset1", run_id="abc123")
    assert v == "abc123"

def test_resolve_version_without_run_id(monkeypatch):
    # Patch datetime to deterministic value
    from datetime import datetime
    class FixedDatetime(datetime):
        @classmethod
        def utcnow(cls):
            return cls(2025, 1, 1, 0, 0, 0)
    monkeypatch.setattr(version, "datetime", FixedDatetime)
    v = version.resolve_version("dataset1")
    assert v == "2025-01-01T00-00-00Z"

def test_resolve_version_errors():
    with pytest.raises(ValueError):
        version.resolve_version("")
    with pytest.raises(ValueError):
        version.resolve_version("dataset1", run_id="")
