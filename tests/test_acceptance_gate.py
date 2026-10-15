import pytest

from dividend_events.acceptance import audit_collection
from dividend_events.storage import write_json


def campaign(root):
    message = dict(provider="prime", agency="5", message_id="one", title="Решения совета директоров")
    write_json(root / "discovery.json", dict(groups={"123": []}))
    write_json(root / "issuers/123.json", dict(inn="123", status="indexed", messages=[message]))
    return dict(inn="123", message=message, checked_at="2026-10-15T12:00:00+00:00")


def test_gate_rejects_missing_search_and_missing_document(tmp_path):
    row = campaign(tmp_path)
    with pytest.raises(ValueError, match="Unattempted document"):
        audit_collection(tmp_path)
    write_json(tmp_path / "messages/5-one.json", dict(row, source_id="source:one"))
    write_json(tmp_path / "discovery.json", dict(groups={"123": [], "456": []}))
    with pytest.raises(ValueError, match="Unattempted issuer searches"):
        audit_collection(tmp_path)


def test_gate_does_not_count_old_circuit_skip_as_attempt(tmp_path):
    row = campaign(tmp_path)
    write_json(tmp_path / "messages/5-one.json", dict(row, review=dict(
        status="source_unavailable", reason="host circuit open after repeated HTTP 503")))
    with pytest.raises(ValueError, match="not retrieved or explicitly attempted"):
        audit_collection(tmp_path)
    write_json(tmp_path / "messages/5-one.json", dict(row, review=dict(
        status="source_unavailable", reason="source_unavailable: HTTP 503; source:receipt")))
    report = audit_collection(tmp_path)
    assert report["unavailable_documents"] == 1 and report["downloaded_documents"] == 0


def test_gate_rejects_wrong_checkpoint_identity(tmp_path):
    row = campaign(tmp_path)
    write_json(tmp_path / "messages/5-one.json", dict(row, inn="456", source_id="source:one"))
    with pytest.raises(ValueError, match="identity mismatch"):
        audit_collection(tmp_path)
    write_json(tmp_path / "messages/5-one.json", dict(row, source_id="source:one"))
    assert audit_collection(tmp_path)["downloaded_documents"] == 1


@pytest.mark.parametrize("recover", [True, False])
def test_timeout_retry_preserves_failure_and_does_not_retry_access_denials(tmp_path, monkeypatch, recover):
    from dividend_events import prime
    from dividend_events.acceptance import retry_timeouts
    from dividend_events.storage import read_json
    row = campaign(tmp_path)
    row["message"]["url"] = "https://example.org/one"
    row["review"] = dict(status="source_unavailable", reason="Read timed out. (read timeout=20)")
    write_json(tmp_path / "discovery.json", dict(groups={"123": []}, policy={}))
    write_json(tmp_path / "messages/5-one.json", row)
    denied = dict(row, review=dict(status="source_unavailable", reason="HTTP 403"))
    write_json(tmp_path / "messages/5-denied.json", denied)
    calls = []

    class Store:
        def fetch(self, url, kind):
            calls.append(url)
            if not recover:
                raise TimeoutError("Read timed out again")
            return b"body", {"source_id": "source:recovered"}

    monkeypatch.setattr(prime, "parse", lambda *args: ([], {"status": "not_dividend"}))
    result = retry_timeouts(tmp_path, Store())
    assert result == dict(selected=1, recovered=int(recover), unavailable=int(not recover))
    assert len(calls) == 1
    assert read_json(next((tmp_path / "retry-history").glob("*.json"))) == row
    assert read_json(tmp_path / "messages/5-denied.json") == denied
    current = read_json(tmp_path / "messages/5-one.json")
    assert bool(current.get("source_id")) == recover
    if recover:
        assert retry_timeouts(tmp_path, Store())["selected"] == 0
