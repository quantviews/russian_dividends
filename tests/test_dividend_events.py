"""Causality and evidence invariants; no live network or production CSV mutations."""

from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path

import polars as pl
import pytest

from dividend_events.collect import parse_aggregator, import_reviewed
from dividend_events.model import make_event, public_boundary, merge_revisions
from dividend_events.official import parse_official_history
from dividend_events.package import build_package, validate_package, detect_conflicts
from dividend_events.reader import read_as_of, normalized_amount
from dividend_events.schema import SCHEMAS
from dividend_events.storage import EvidenceStore, digest
from dividend_events.universe import collect_universe


@pytest.fixture
def security():
    return dict(security_id="security:test-ordinary", issuer_id="issuer:test", ticker="TEST",
                share_class="ordinary", identity_status="identified", aliases_json='[{"ticker":"OLD","valid_to":"2024-01-01"}]')


@pytest.fixture
def store(tmp_path):
    return EvidenceStore(tmp_path / "store", offline=True)


def event(store, security, key="2023:annual", event_type="recommendation", status="proposed",
          published="2023-03-01T10:00:00+03:00", observed="2023-03-02T12:00:00+03:00", previous=None, **extra):
    source = store.save(f"official {published} {extra} {status}".encode(), "https://issuer.test/" + digest(str(extra).encode()),
                        "official_document", observed_at=observed)
    fields = dict(event_type=event_type, status=status, amount="10", currency="RUB", amount_basis="gross",
                  reporting_period="2023 full year", evidence="official", verification_status="verified",
                  published_at=published, publication_precision="second", publication_timezone="Europe/Moscow",
                  publication_evidence="synthetic fixture timestamp", edition_evidence="synthetic immutable fixture",
                  historical_availability="proven", share_basis_date="2023-01-01")
    fields.update(extra)
    return make_event(fields, source, security, key, previous)


def package(tmp_path, store, securities, events, actions=()):
    tables = {name: [] for name in SCHEMAS}
    tables.update(securities=securities, events=events, sources=store.sources(),
                  coverage=[dict(security_id=s["security_id"], ticker=s["ticker"], status="partial") for s in securities],
                  corporate_actions=list(actions), conflicts=detect_conflicts(events, actions))
    return build_package(tmp_path / "packages", tables, store, {"limitations": ["synthetic test fixtures"]}, Path(__file__).parents[1])


def test_recommendation_approval_late_correction_preserves_early_answer(tmp_path, store, security):
    proposed = event(store, security)
    approved = event(store, security, event_type="approval", status="approved", published="2023-04-01T12:00:00+03:00",
                     observed="2023-04-01T12:10:00+03:00", previous=proposed["event_id"])
    correction = event(store, security, event_type="amendment", status="approved", published="2023-05-01T12:00:00+03:00",
                       observed="2023-05-02T12:00:00+03:00", previous=approved["event_id"], amount="12",
                       change_type="amount_and_record_date", record_date="2023-05-15", record_date_origin="official")
    before = package(tmp_path, store, [security], [proposed, approved])
    after = package(tmp_path, store, [security], [proposed, approved, correction])
    assert read_as_of(before, "2023-04-20T00:00:00Z").to_dicts() == read_as_of(after, "2023-04-20T00:00:00Z").to_dicts()
    assert read_as_of(after, "2023-03-01T06:59:59Z").is_empty()
    assert read_as_of(after, "2023-03-01T07:00:00Z")["status"].to_list() == ["proposed"]
    assert read_as_of(after, "2023-06-01T00:00:00Z")["amount"].to_list() == ["12"]


def test_public_vs_observed_and_day_only(tmp_path, store, security):
    e = event(store, security, published="2023-03-01", publication_precision="day", observed="2026-10-15T00:00:00Z")
    p = package(tmp_path, store, [security], [e])
    assert read_as_of(p, "2023-03-01T23:59:59+03:00").is_empty()
    assert len(read_as_of(p, "2023-03-02T00:00:00+03:00")) == 1
    assert read_as_of(p, "2023-03-02T00:00:00+03:00", "observed_as_of").is_empty()
    assert len(read_as_of(p, "2026-10-15T01:00:00Z", "observed_as_of")) == 1
    from dividend_events.reader import read_many_as_of
    before, after = read_many_as_of(p, [{"at":"2023-03-01T23:59:59+03:00"}, {"at":"2023-03-02T00:00:00+03:00"}])
    assert before.is_empty() and len(after) == 1
    with pytest.raises(ValueError, match="timezone"):
        read_as_of(p, "2023-03-02T00:00:00")


def test_unknown_edition_excluded_from_public_but_visible_after_observation(tmp_path, store, security):
    e = event(store, security, historical_availability="unproven", edition_evidence=None)
    p = package(tmp_path, store, [security], [e])
    assert read_as_of(p, "2026-10-15T00:00:00Z").is_empty()
    assert len(read_as_of(p, "2026-10-15T00:00:00Z", "observed_as_of")) == 1
    with pytest.raises(ValueError, match="edition"):
        event(store, security, edition_evidence=None)


def test_cancellation_distinct_from_no_dividend_and_payment_deadline(tmp_path, store, security):
    proposal = event(store, security)
    cancel = event(store, security, event_type="cancellation", status="cancelled", previous=proposal["event_id"],
                   published="2023-04-01T00:00:00Z")
    zero = event(store, security, key="2024:annual", event_type="no_dividend", status="no_dividend", amount="0")
    terms = event(store, security, key="2025:annual", event_type="payment_terms", status="approved", payment_to="2023-04-30")
    p = package(tmp_path, store, [security], [proposal, cancel, zero, terms])
    rows = read_as_of(p, "2026-10-15T00:00:00Z").to_dicts()
    assert {e["status"] for e in rows} == {"cancelled", "no_dividend", "approved"}
    assert all(e["actual_payment_date"] is None for e in rows)
    with pytest.raises(ValueError, match="reference"):
        event(store, security, event_type="cancellation", status="cancelled")


def test_partial_payment_and_unknown_amount(tmp_path, store, security):
    e = event(store, security, event_type="payment", status="paid", actual_payment_date="2023-05-01",
              actual_payment_amount="5", partial_payment=True, recipient_category="nominal_holder")
    p = package(tmp_path, store, [security], [e])
    assert read_as_of(p, "2026-10-15T00:00:00Z")["partial_payment"].to_list() == [True]
    with pytest.raises(ValueError, match="Paid"):
        event(store, security, event_type="payment", status="paid")
    unknown = event(store, security, amount=None, amount_unknown_reason="not_disclosed")
    assert unknown["amount"] is None
    with pytest.raises(ValueError, match="reason"):
        event(store, security, amount=None)


def test_same_record_date_different_periods_and_classes(tmp_path, store, security):
    pref = {**security, "security_id": "security:test-preferred", "ticker": "TESTP", "share_class": "preferred", "listing_status": "inactive"}
    events = [event(store, security, key="2023:H1", record_date="2023-07-01", record_date_origin="official"),
              event(store, security, key="2022:FY", record_date="2023-07-01", record_date_origin="official"),
              event(store, pref, key="2023:H1", record_date="2023-07-01", record_date_origin="official")]
    p = package(tmp_path, store, [security, pref], events)
    assert len(read_as_of(p, "2026-10-15T00:00:00Z")) == 3
    assert len({e["dividend_id"] for e in events}) == 3
    assert len(pl.read_parquet(p / "coverage.parquet")) == 2


def test_conflicts_preserved_and_never_silently_pick_source(tmp_path, store, security):
    a = event(store, security)
    b = event(store, security, amount="20")
    p = package(tmp_path, store, [security], [a, b])
    assert len(read_as_of(p, "2026-10-15T00:00:00Z")) == 2
    assert pl.read_parquet(p / "conflicts.parquet")["field"].to_list() == ["amount"]


def test_normal_lifecycle_and_decimal_format_are_not_conflicts(store, security):
    proposed=event(store,security,amount="10.00")
    approved=event(store,security,amount="10",event_type="approval",status="approved")
    assert detect_conflicts([proposed,approved],[])==[]


def test_repeat_document_idempotence_and_package_tampering(tmp_path, store, security):
    a = event(store, security)
    b = event(store, security)
    assert a == b
    p = package(tmp_path, store, [security], [a])
    assert package(tmp_path, store, [security], [a]) == p
    (p / a["raw_path"]).write_bytes(b"changed")
    with pytest.raises(ValueError, match="hash"):
        validate_package(p)


def test_split_separate_normalization_and_no_future_action(store, security):
    e = event(store, security, amount="100")
    actions = [dict(action_id="split", security_id=e["security_id"], action_date="2024-01-01", kind="split", ratio="10",
                    verification_status="verified", public_available_at="2023-12-01T00:00:00Z")]
    result = normalized_amount(e, actions, "2024-01-02", "2024-01-02T00:00:00Z")
    assert Decimal(result["amount"]) == 10 and e["amount"] == "100"
    with pytest.raises(ValueError, match="point-in-time"):
        normalized_amount(e, actions, "2024-01-02", "2023-01-01T00:00:00Z")


def test_aggregator_never_invents_publication_or_approval(tmp_path, store, security):
    body = '<table><tr><th>Период выплаты</th><th>Дивиденд</th><th>Префы</th></tr><tr><td>2023 год закрытие реестра 10.07.2024</td><td>1 234,56 руб.</td><td>неизвестно</td></tr></table>'.encode()
    source = store.save(body, "https://aggregator.test/", "aggregator_reference")
    rows = parse_aggregator(body, source, security)
    assert rows[0]["amount"] == "1234.56" and rows[0]["published_at"] is None
    p = package(tmp_path, store, [security], rows)
    assert read_as_of(p, "2099-01-01T00:00:00Z", "observed_as_of").is_empty()
    assert parse_aggregator(body, source, security, preferred=True)[0]["amount"] is None


def test_rights_dates_require_calendar(store, security):
    with pytest.raises(ValueError, match="calendar"):
        event(store, security, ex_date="2023-07-01", ex_date_origin="derived")


def test_cross_issue_revision_rejected(tmp_path, store, security):
    a = event(store, security)
    b = event(store, security, key="different", previous=a["event_id"], amount="20")
    with pytest.raises(ValueError, match="crosses"):
        package(tmp_path, store, [security], [a, b])


def test_unknown_publication_is_not_decision_date(tmp_path, store, security):
    e = event(store, security, published=None, publication_precision="unknown", historical_availability="unproven",
              publication_evidence=None, edition_evidence=None, decision_at="2023-01-01")
    p = package(tmp_path, store, [security], [e])
    assert read_as_of(p, "2099-01-01T00:00:00Z").is_empty()


def test_changed_document_versions_and_repeat_ingest(store, security):
    a = event(store, security)
    # Same source URL, different content; corrected publication and observation occur later.
    source = store.save(b"corrected source content", a["source_url"], "official_document", observed_at="2023-06-01T00:00:00Z")
    fields = dict(event_type="recommendation", status="proposed", amount="15", amount_basis="gross",
                  historical_availability="unproven", verification_status="verified", evidence="corrected")
    b = make_event(fields, source, security, "2023:annual")
    merged = merge_revisions([a], [b])
    assert len(merged) == 2 and merged[1]["supersedes_id"] == a["event_id"]
    assert merge_revisions(merged, [b]) == merged


def test_official_history_is_observed_fact_not_archived_publication(tmp_path, store, security):
    security = {**security, "isin": "RU0007288411"}
    html = '''<z-table :columns="['Дата принятия решения']"><z-table-row>
    <z-table-col>2023</z-table-col><z-table-col>-</z-table-col><z-table-col>28.06.2024</z-table-col>
    <z-table-col>-</z-table-col><z-table-col>0</z-table-col><z-table-col>0</z-table-col>
    </z-table-row><z-table-row><z-table-col>9М 2023</z-table-col><z-table-col>26.12.2023</z-table-col>
    <z-table-col>07.12.2023</z-table-col><z-table-col>07.02.2024</z-table-col><z-table-col>915,33</z-table-col>
    <z-table-col>139920</z-table-col></z-table-row></z-table>'''.encode()
    source = store.save(html, "https://issuer.test/history", "official_document", observed_at="2026-10-15T00:00:00Z")
    rows = parse_official_history(html, source, security, "nornickel_history_v1")
    p = package(tmp_path, store, [security], rows)
    assert {r["status"] for r in rows} == {"approved", "no_dividend"}
    assert read_as_of(p, "2024-12-31T00:00:00Z").is_empty()
    assert len(read_as_of(p, "2026-10-15T00:00:00Z", "observed_as_of")) == 2
    assert all(r["actual_payment_date"] is None for r in rows)


def test_minute_precision_is_conservative(store, security):
    e = event(store, security, published="2023-03-01T10:15:00+03:00", publication_precision="minute")
    assert e["public_available_at"] == "2023-03-01T07:16:00+00:00"


def test_wrong_coverage_and_duplicate_keys_rejected(tmp_path, store, security):
    e = event(store, security)
    with pytest.raises(ValueError, match="Duplicate"):
        package(tmp_path, store, [security], [e, e])


def test_universe_keeps_orphans_delisted_aliases_and_preferred(tmp_path, store, monkeypatch):
    project = tmp_path / "project"
    (project / "metadata").mkdir(parents=True)
    (project / "data/raw").mkdir(parents=True)
    (project / "metadata/ticker_mappings.json").write_text('{"NEW":"issuer"}', encoding="utf-8")
    (project / "data/ORPHAN.csv").write_text("closing_date,year,period_type,dividend_value\n2023-01-01,2022,full year,1\n")
    columns = ["secid", "isin", "regnumber", "type", "is_traded", "emitent_id"]
    def fetch(url, kind):
        inactive = "is_trading=0" in url
        data = [["OLD", "RUtest", "reg", "common_share", 0, 1]] if inactive else [
            ["NEW", "RUtest", "reg", "common_share", 1, 1],
            ["NEWP", "RUtestP", "regP", "preferred_share", 1, 1],
            ["FUND", "RUfund", "fund", "exchange_ppif", 1, 2]]
        if "start=0&" not in url:
            data = []
        body = json.dumps({"securities": {"columns": columns, "data": data}}).encode()
        return body, store.save(body, url, kind)
    monkeypatch.setattr(store, "fetch", fetch)
    securities, classification, discovery = collect_universe(project, store)
    assert len(securities) == 3  # same issue OLD/NEW, preferred, and orphan
    by_ticker = {s["ticker"]: s for s in securities}
    assert by_ticker["ORPHAN"]["mapping_status"] == "absent"
    assert by_ticker["ORPHAN"]["mapping_missing_observed_at"] is not None
    assert by_ticker["NEWP"]["share_class"] == "preferred"
    assert "OLD" in by_ticker["NEW"]["aliases_json"]
    assert any(r["ticker"] == "FUND" and r["classification"] == "excluded" for r in classification)
    assert discovery["moex_pagination_complete"]


def test_http_403_saved_without_cookies_or_retry(tmp_path, monkeypatch):
    store = EvidenceStore(tmp_path / "http", delay=0, retries=2)
    calls = []
    class Response:
        status_code = 403
        content = b"Forbidden"
        headers = {"Content-Type": "text/html", "Set-Cookie": "secret", "Authorization": "secret"}
    def get(url, timeout):
        calls.append(url)
        return Response()
    monkeypatch.setattr(store.session, "get", get)
    with pytest.raises(PermissionError, match="403"):
        store.fetch("https://issuer.test/blocked", "official_document")
    assert len(calls) == 1
    source = store.sources()[0]
    assert "secret" not in source["http_metadata_json"]
    assert (store.root / source["raw_path"]).read_bytes() == b"Forbidden"
    store.offline = True
    with pytest.raises(PermissionError, match="cached HTTP 403"):
        store.fetch("https://issuer.test/blocked", "official_document")
    assert len(calls) == 1


def test_http_circuit_waits_then_requests_next_document(tmp_path, monkeypatch):
    from dividend_events import storage
    store = EvidenceStore(tmp_path / 'http', delay=0, retries=0)
    clock = [100.0]
    monkeypatch.setattr(storage.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(storage.time, 'sleep', lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    calls = []
    class Response:
        status_code = 200
        content = b'archived document'
        headers = {}
    def get(url, timeout):
        calls.append((url, clock[0]))
        return Response()
    monkeypatch.setattr(store.session, 'get', get)
    store.host_cooldowns['issuer.test'] = 220.0
    body, source = store.fetch('https://issuer.test/next', 'official_document')
    assert calls == [('https://issuer.test/next', 220.0)]
    assert body == Response.content and source['http_status'] == '200'


def test_numbers_in_non_amount_text_are_not_dividends(store, security):
    for text in ("Решение ожидается 25.05.2024", "10% от номинала", "В 2024 году решение не принято"):
        body = f"<table><tr><td>2023 год</td><td>{text}</td></tr><tr><th>Дивиденды</th></tr></table>".encode()
        source = store.save(body, "https://aggregator.test/", "aggregator_reference")
        row = parse_aggregator(body, source, security)[0]
        assert row["amount"] is None and row["amount_unknown_reason"]
