"""Event identities, append-only revisions and conservative publication boundaries."""

from datetime import datetime, date, time, timedelta, timezone
from decimal import Decimal
import json
from zoneinfo import ZoneInfo

from .schema import EVENT_STATUSES, PARSER_VERSION, SCHEMAS
from .storage import identifier


def timestamp(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def public_boundary(event):
    if event.get("historical_availability") != "proven":
        return None
    if not event.get("publication_evidence") or not event.get("edition_evidence"):
        raise ValueError("Historical availability requires publication and edition evidence")
    value = event.get("published_at")
    if not value:
        raise ValueError("Proven publication requires published_at")
    precision = event.get("publication_precision")
    if precision == "day":
        zone = event.get("publication_timezone")
        if not zone:
            raise ValueError("Day-only publication requires an IANA timezone")
        return datetime.combine(date.fromisoformat(value) + timedelta(days=1), time(), ZoneInfo(zone)).astimezone(timezone.utc).isoformat()
    if precision in {"second", "minute", "hour"}:
        parsed = timestamp(value)
        if precision == "minute":
            parsed = parsed.replace(second=0, microsecond=0) + timedelta(minutes=1)
        elif precision == "hour":
            parsed = parsed.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        return parsed.isoformat()
    raise ValueError("Unsupported publication precision; use day, hour, minute, second or unknown without proof")


def make_event(fields, source, security, logical_key, previous=None):
    if set(fields) - set(SCHEMAS["events"]):
        raise ValueError("Unknown event fields: " + str(sorted(set(fields) - set(SCHEMAS["events"]))))
    row = {key: None for key in SCHEMAS["events"]}
    row.update(fields)
    row.update(security_id=security["security_id"], issuer_id=security.get("issuer_id"),
               ticker=fields.get("ticker", security["ticker"]),
               parser_version=PARSER_VERSION, observed_at=source["observed_at"])
    for key in ("source_id", "source_url", "document_id", "raw_path", "sha256"):
        row[key] = source[key]
    row["dividend_id"] = identifier("dividend", [security["security_id"], logical_key])
    row["event_id"] = identifier("event", [row["dividend_id"], source["source_id"], fields, previous])
    row["revision_id"] = row["event_id"]
    row["supersedes_id"] = previous
    row["public_available_at"] = public_boundary(row)
    validate_event(row)
    return row


def validate_event(row):
    if row["event_type"] not in EVENT_STATUSES or row["status"] not in EVENT_STATUSES[row["event_type"]]:
        raise ValueError("Incompatible event type/status")
    timestamp(row["observed_at"])
    if row.get("amount") is None and not row.get("amount_unknown_reason"):
        raise ValueError("Unknown amount requires a reason")
    for field in ("amount", "actual_payment_amount"):
        if row.get(field) is not None:
            value = Decimal(row[field])
            if not value.is_finite() or value < 0:
                raise ValueError(f"Invalid {field}")
    if row["event_type"] == "no_dividend" and row.get("amount") != "0":
        raise ValueError("Explicit no_dividend requires confirmed zero")
    if row["event_type"] == "payment":
        if not row.get("evidence") or (not row.get("actual_payment_date") and not (
            row.get("actual_payment_amount") is not None and "actual_payment_date_not_disclosed" in (row.get("gap_reason") or "").split(";"))):
            raise ValueError("Paid requires actual payment evidence and date, or an explicitly disclosed paid amount with a date gap")
    if row["event_type"] == "cancellation" and not row.get("supersedes_id"):
        raise ValueError("Cancellation must reference the cancelled proposal/approval")
    if row.get("verification_status") not in {"verified", "reference_only", "needs_review"}:
        raise ValueError("Invalid verification status")
    if row.get("historical_availability") not in {"proven", "unproven"}:
        raise ValueError("Invalid historical availability")
    if row["event_type"] == "reference" and (row["verification_status"] != "reference_only" or row["historical_availability"] != "unproven"):
        raise ValueError("Reference observations cannot be promoted to verified historical events")
    if row.get("amount_basis") not in {"gross", "net", "unknown"}:
        raise ValueError("Amount basis must be gross, net or unknown")
    for field in ("record_date", "last_buy_date", "ex_date", "payment_from", "payment_to", "actual_payment_date", "share_basis_date"):
        if row.get(field):
            date.fromisoformat(row[field])
    for field in ("record_date", "last_buy_date", "ex_date"):
        if row.get(field) and not row.get(field + "_origin"):
            raise ValueError(f"{field} requires provenance")
        if row.get(field + "_origin") == "derived" and (not row.get("calendar_version") or not row.get("settlement_rule")):
            raise ValueError("Derived rights dates require pinned calendar and settlement rule")
    if public_boundary(row) != row.get("public_available_at"):
        raise ValueError("Invalid public availability boundary")


def merge_revisions(previous, incoming):
    """Link only unambiguous same-source, same-episode revisions; keep other branches visible."""
    def archive_identity(row):
        try:
            proof = json.loads(row.get("edition_evidence") or "null")
        except (ValueError, TypeError):
            return None
        if isinstance(proof, dict) and proof.get("kind") == "regulated_news_archive":
            return proof.get("provider", "skrin"), proof.get("agency"), proof.get("message_id")
        return None
    merged = {e["event_id"]: e for e in previous}
    for raw in incoming:
        if raw["event_id"] in merged:
            continue
        event = dict(raw)
        superseded = {e["supersedes_id"] for e in merged.values() if e.get("supersedes_id")}
        archive = archive_identity(event)
        archive_previous = [e for e in merged.values() if archive and archive_identity(e) == archive
                            and e['security_id'] == event['security_id']]
        changed_archive = any(e['sha256'] != event['sha256'] and timestamp(e['observed_at']) < timestamp(event['observed_at'])
                              for e in archive_previous)
        if changed_archive:
            gaps = set((event.get("gap_reason") or "").split(";")) - {""}
            gaps.add("archive_immutability_violation")
            event.update(historical_availability="unproven", public_available_at=None,
                gap_reason=";".join(sorted(gaps)), change_type="archive_content_revision")
        candidates = [e for e in merged.values() if e["dividend_id"] == event["dividend_id"]
                      and ((archive and archive_identity(e) == archive) or
                           (e["event_type"] == event["event_type"] and e["source_url"] == event["source_url"]))
                      and e["event_id"] not in superseded]
        if not candidates and changed_archive:
            candidates = [e for e in archive_previous if e['event_type'] == event['event_type'] and e['event_id'] not in superseded]
        if not event.get("supersedes_id") and len(candidates) == 1:
            parent = candidates[0]
            if parent["sha256"] != event["sha256"] and timestamp(event["observed_at"]) > timestamp(parent["observed_at"]):
                event["supersedes_id"] = parent["event_id"]
                if archive:
                    event["dividend_id"] = parent["dividend_id"]
                event["change_type"] = event.get("change_type") or "source_revision"
        merged[event["event_id"]] = event
    return list(merged.values())
