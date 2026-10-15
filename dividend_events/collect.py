"""Evidence collectors. Aggregator rows are references, never historical announcements."""

import json
from pathlib import Path
import re
from datetime import datetime
from decimal import Decimal
from urllib.parse import quote

from bs4 import BeautifulSoup

from .model import make_event
from .storage import read_json, write_json, identifier, now
from .universe import ISS, block

BASE = "https://xn--80aeiahhn9aobclif2kuc.xn--p1ai"


def aliases(security):
    return sorted({security["ticker"]} | {a["ticker"] for a in json.loads(security["aliases_json"]) if "ticker" in a})


def parse_aggregator(body, source, security, preferred=False):
    soup = BeautifulSoup(body, "html.parser")
    events = []
    table = next((t for t in soup.find_all("table") if any(w in t.get_text(" ").lower() for w in ("период", "дивиденд", "выплат"))), None)
    if table is None:
        raise ValueError("dividend_table_not_found")
    occurrences = {}
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        column = 2 if preferred else 1
        if len(cells) <= column:
            continue
        period = " ".join(cells[0].stripped_strings)
        text = " ".join(cells[column].stripped_strings)
        if "ИТОГО" in period.upper() or "Период выплаты" in period:
            continue
        dates = re.findall(r"\b(\d{1,2}\.\d{2}\.\d{4})\b", period)
        record_date = None
        if dates:
            try:
                record_date = datetime.strptime(dates[0], "%d.%m.%Y").date().isoformat()
            except ValueError:
                pass
        reporting_period = period
        for found in dates:
            reporting_period = reporting_period.replace(found, "")
        if not re.search(r"\b(?:19|20)\d{2}\b", reporting_period):
            continue
        no_dividend = "РЕШЕНИЕ ДИВИДЕНДЫ НЕ ВЫПЛАЧИВАТЬ" in text.upper()
        numeric = r"\d+(?:[ \u00a0\u202f]\d{3})*(?:[,.]\d+)?"
        match = re.search(r"(?<![\d.,])(" + numeric + r")\s*(?:руб\.?|рублей|рубля|₽)", text, re.I)
        if not match:
            match = re.fullmatch(r"\s*(" + numeric + r")\s*", text)
        amount = "0" if no_dividend else str(Decimal(re.sub(r"\s", "", match[1]).replace(",", "."))) if match else None
        # A reference's identity includes the complete period label and occurrence, never record_date alone.
        logical = ["aggregator_reference", period, preferred]
        occurrence = occurrences.get(period, 0)
        occurrences[period] = occurrence + 1
        logical.append(occurrence)
        fields = dict(event_type="reference", status="unverified", reporting_period=reporting_period.strip(),
            amount=amount, amount_unknown_reason=None if amount is not None else "not_parseable_from_source",
            currency="RUB" if "руб" in text.lower() else None, amount_basis="unknown",
            original_text=period + " | " + text, evidence=str(tr), record_date=record_date,
            record_date_origin="aggregator" if record_date else None,
            publication_precision="unknown", historical_availability="unproven",
            verification_status="reference_only", gap_reason="mutable_aggregator_has_no_publication_or_edition_proof",
            share_basis_text="as_displayed_by_aggregator; historical_share_basis_unverified")
        events.append(make_event(fields, source, security, logical))
    return events


def collect_security(security, mapping, store, start, end, official_config):
    state = {"security_id": security["security_id"], "ticker": security["ticker"], "status": "not_checked",
             "requested_from": start, "requested_to": end, "checked_at": now()}
    events, source_ids, reasons, statuses = [], [], [], {}
    ticker = security["ticker"]
    # Every issue receives a source check. An absent table is not zero dividends.
    try:
        body, source = store.fetch(f"{ISS}/securities/{quote(ticker, safe='')}/dividends.json?iss.meta=off", "moex_dividends")
        source_ids.append(source["source_id"])
        rows = block(json.loads(body), "dividends")
        statuses["moex_dividends"] = "rows_require_review" if rows else "no_rows_not_proof_of_no_dividend"
        reasons.append("moex_dividend_rows_not_historical_publication_evidence")
    except Exception as exc:
        statuses["moex_dividends"] = "source_unavailable"
        reasons.append(f"moex_dividends:{type(exc).__name__}:{exc}")

    candidates = []
    for alias in aliases(security):
        if alias in mapping:
            candidates.append((mapping[alias], False))
        elif security.get("share_class") == "preferred" and alias.endswith("P") and alias[:-1] in mapping:
            candidates.append((mapping[alias[:-1]], True))
        elif security.get("identity_status") == "unresolved" and alias.endswith("P") and alias[:-1] in mapping:
            # Candidate retained as unverified, never used to assert issue identity.
            candidates.append((mapping[alias[:-1]], True))
    for path, preferred in sorted(set(candidates)):
        try:
            body, source = store.fetch(BASE + "/" + path.strip("/") + "/", "aggregator_reference")
            source_ids.append(source["source_id"])
            found = parse_aggregator(body, source, security, preferred)
            events.extend(found)
            statuses["aggregator"] = "reference_only" if found else "no_events_found"
        except Exception as exc:
            statuses["aggregator"] = "source_unavailable"
            reasons.append(f"aggregator:{type(exc).__name__}:{exc}")
    statuses.setdefault("aggregator", "unsupported_security")

    configured = [item for item in official_config if item.get("security_id") == security["security_id"]
                  or item.get("ticker") in aliases(security)]
    for item in configured:
        try:
            body, source = store.fetch(item["url"], "official_document")
            source_ids.append(source["source_id"])
            if item.get("adapter"):
                from .official import parse_official_history
                official_events = parse_official_history(body, source, security, item["adapter"])
                events.extend(official_events)
                statuses[item["url"]] = "current_facts_verified_historical_edition_unproven"
            else:
                statuses[item["url"]] = "document_saved_requires_review"
        except Exception as exc:
            statuses[item["url"]] = "source_unavailable"
            reasons.append(f"official:{type(exc).__name__}:{exc}")
    if not configured:
        statuses["official_disclosure"] = "not_checked"
        reasons.append("no_verified_official_document_locator_for_security")
    reasons += ["historical_archive_coverage_not_proven", "jurisdiction_and_share_basis_require_verification"]
    dates = sorted(e["record_date"] for e in events if e.get("record_date"))
    counts = {}
    for event in events:
        counts[event["event_type"]] = counts.get(event["event_type"], 0) + 1
    state.update(status="partial" if events or source_ids and configured else "source_unavailable",
        available_from=dates[0] if dates else None, available_to=dates[-1] if dates else None,
        source_ids_json=json.dumps(sorted(set(source_ids))), event_counts_json=json.dumps(counts),
        missing_fields_json=json.dumps(["published_at", "edition_evidence", "actual_payment_date", "share_basis_date"]),
        reasons_json=json.dumps(reasons, ensure_ascii=False), source_statuses_json=json.dumps(statuses, ensure_ascii=False))
    return events, state


def collect_all(securities, mapping, store, start, end, run_dir, official_config=()):
    events, coverage = [], []
    for index, security in enumerate(securities, 1):
        checkpoint = Path(run_dir) / "checkpoints" / (security["security_id"].replace(":", "_") + ".json")
        if checkpoint.exists():
            saved = read_json(checkpoint)
            found, state = saved["events"], saved["coverage"]
        else:
            try:
                found, state = collect_security(security, mapping, store, start, end, official_config)
            except Exception as exc:
                found = []
                state = dict(security_id=security["security_id"], ticker=security["ticker"], status="source_unavailable",
                    requested_from=start, requested_to=end, checked_at=now(), reasons_json=json.dumps([str(exc)]),
                    source_ids_json="[]", event_counts_json="{}", missing_fields_json="[]", source_statuses_json="{}")
            write_json(checkpoint, {"events": found, "coverage": state})
        events.extend(found)
        coverage.append(state)
        if index % 25 == 0 or index == len(securities):
            print(f"Checked {index}/{len(securities)} issues; {len(events)} reference events", flush=True)
    return events, coverage


def import_reviewed(path, securities, store, existing):
    """Explicit human/parser-reviewed facts with raw evidence; never infer historic availability."""
    registry = {s["security_id"]: s for s in securities}
    events = {e["event_id"]: e for e in existing}
    for record in read_json(path):
        security = registry[record["security_id"]]
        if record.get("raw_file"):
            source = store.save(Path(record["raw_file"]).read_bytes(), record["url"], "reviewed_official_document")
        else:
            _, source = store.fetch(record["url"], "reviewed_official_document")
        fields = record["fields"]
        if fields.get("verification_status") != "verified" or not fields.get("evidence"):
            raise ValueError("Reviewed import requires verified status and evidence excerpt")
        body = (store.root / source["raw_path"]).read_bytes()
        # Text or HTML can be mechanically checked. Binary documents need separately supplied extracted evidence.
        plain = BeautifulSoup(body, "html.parser").get_text(" ", strip=True)
        if " ".join(fields["evidence"].split()) not in " ".join(plain.split()):
            raise ValueError("Evidence excerpt is absent from the saved document")
        event = make_event(fields, source, security, record["dividend_key"], record.get("supersedes_id"))
        if event["supersedes_id"] and event["supersedes_id"] not in events:
            raise ValueError("Missing superseded event")
        events[event["event_id"]] = event
    return list(events.values())
