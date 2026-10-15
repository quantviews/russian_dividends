"""Pinned packages, comprehensive integrity validation, and conservative CSV reconciliation."""

from collections import Counter
import json
from pathlib import Path
import shutil
import subprocess
from decimal import Decimal, InvalidOperation

import polars as pl

from .model import validate_event, timestamp
from .schema import SCHEMAS, SCHEMA_VERSION, PARSER_VERSION, frame
from .storage import canonical, digest, identifier, now, read_json, write_json


def reconcile(securities, sources, events, store):
    from .collect import aliases
    registry = {}
    for s in securities:
        for ticker in aliases(s):
            registry.setdefault(ticker, []).append(s)
    rows = []
    for source in sources:
        if source["source_kind"] != "legacy_csv":
            continue
        from urllib.parse import unquote, urlsplit
        original = unquote(urlsplit(source["source_url"]).path)
        ticker = Path(original).stem.upper()
        for index, row in enumerate(pl.read_csv(store.root / source["raw_path"], infer_schema=False).to_dicts(), 1):
            security = registry.get(ticker, [])
            sid = security[0]["security_id"] if len(security) == 1 else None
            amount = row.get("dividend_value", row.get("dividend_value_post_split"))
            period = " ".join(str(row.get(k) or "") for k in ("year", "period_type")).strip()
            matches = [e for e in events if e["security_id"] == sid and e["record_date"] == row.get("closing_date")
                       and e["reporting_period"] == period and e["status"] in {"approved", "paid", "no_dividend"}
                       and e["verification_status"] == "verified"]
            reason = "no_verified_event_with_matching_period_and_share_basis"
            result = "no_confirmation"
            # Legacy CSV does not encode basis dates; normalized values cannot be compared to declarations.
            if matches and "/raw/" in original.replace("\\", "/"):
                compatible = [e for e in matches if e.get("share_basis_text") == "legacy_raw_basis_verified"]
                if compatible:
                    try:
                        result = "matched" if any(Decimal(e["amount"]) == Decimal(amount) for e in compatible if e["amount"] is not None) else "differs"
                        reason = "reviewed_period_and_original_basis"
                    except (InvalidOperation, TypeError):
                        reason = "unknown_amount"
            rows.append(dict(security_id=sid, ticker=ticker, csv_path=source["source_url"], csv_row=str(index),
                record_date=row.get("closing_date"), reporting_period=period, csv_amount=amount,
                result=result, event_ids_json=json.dumps([e["event_id"] for e in matches]), reason=reason))
    return rows


def detect_conflicts(events, actions):
    groups, conflicts = {}, []
    superseded = {e["supersedes_id"] for e in events if e.get("supersedes_id")}
    for e in events:
        if e["event_id"] not in superseded and e["verification_status"] == "verified":
            groups.setdefault(e["dividend_id"], []).append(e)
    for did, group in groups.items():
        for field in ("amount", "record_date", "status", "currency"):
            values = sorted({e[field] for e in group if e.get(field) is not None})
            if field == "amount":
                values = sorted({format(Decimal(value).normalize(), "f") for value in values})
            if field == "status" and not (set(values) & {"cancelled", "no_dividend"} and set(values) & {"approved", "paid"}):
                continue  # Normal recommendation/approval/payment stages are not a source conflict.
            if len(values) > 1:
                conflicts.append(dict(conflict_id=identifier("conflict", [did, field, values]), security_id=group[0]["security_id"],
                    dividend_id=did, event_ids_json=json.dumps([e["event_id"] for e in group]), field=field,
                    values_json=json.dumps(values), status="unresolved", resolution=None, evidence="parallel_unsuperseded_versions"))
    grouped = {}
    for a in actions:
        grouped.setdefault(a["ticker"], []).append(a)
    for ticker, group in grouped.items():
        if len({a["source_id"] for a in group}) > 1:
            by_source = {}
            for a in group:
                ratio = Decimal(a["ratio"])
                # moexutils auto/price/shares uses the price divisor: reverse
                # 5000:1 is 0.0002 there, but ratio=5000, kind=reverse here.
                factor = ratio if a["kind"] == "reverse" else 1 / ratio if a["kind"] in {"split", "auto", "price", "shares"} else None
                by_source.setdefault(a["source_id"], []).append((a["action_date"], format(factor.normalize(), "f") if factor is not None else a["kind"]+":"+a["ratio"]))
            if len({json.dumps(sorted(v)) for v in by_source.values()}) > 1:
                conflicts.append(dict(conflict_id=identifier("conflict", [ticker, by_source]), security_id=group[0]["security_id"],
                    dividend_id=None, event_ids_json="[]", field="corporate_actions", values_json=json.dumps(by_source),
                    status="unresolved", resolution=None, evidence="dividends_and_moexutils_registry_disagree_or_differ_in_depth"))
    return conflicts


def corporate_actions(project, moexutils, securities, store):
    from .collect import aliases
    registry = {ticker: s["security_id"] for s in securities for ticker in aliases(s)}
    rows = []
    path = Path(project) / "metadata/splits.json"
    if path.exists():
        source = store.local(path, "dividends_splits")
        for ticker, actions in read_json(path).items():
            for action in actions:
                rows.append(dict(ticker=ticker, security_id=registry.get(ticker), action_date=action["date"],
                    kind=action.get("kind", "split"), ratio=str(action["ratio"]), source_id=source["source_id"]))
    if moexutils:
        path = Path(moexutils) / "metadata/splits.csv"
        if path.exists():
            source = store.local(path, "moexutils_splits")
            for action in pl.read_csv(path, infer_schema=False).to_dicts():
                rows.append(dict(ticker=action["ticker"], security_id=registry.get(action["ticker"]),
                    action_date=action["date"], kind=action["kind"], ratio=action["ratio"], source_id=source["source_id"]))
    for row in rows:
        row.update(action_id=identifier("action", row), verification_status="unverified",
                   basis_policy="preserve_source_convention; do_not_apply_until_verified")
    return rows


def build_package(destination, tables, store, metadata, project):
    destination = Path(destination)
    # Package identity includes every table and pinned settings; repeated export is idempotent.
    content = {name: sorted(rows, key=lambda r: canonical(r)) for name, rows in tables.items()}
    code_hashes = {p.name: digest(p.read_bytes()) for p in Path(__file__).parent.glob("*.py")}
    package_id = "dividends-" + digest(canonical({"tables": content, "metadata": metadata, "schema": SCHEMA_VERSION,
                                               "code": code_hashes}))[:24]
    target = destination / package_id
    if target.exists():
        validate_package(target)
        return target
    staging = destination / ("." + package_id + ".building")
    if staging.exists():
        # A previous interrupted build is disposable; only remove this exact validated staging directory.
        if staging.resolve().parent != destination.resolve() or not staging.name.endswith(".building"):
            raise ValueError("Unsafe staging path")
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    for table in SCHEMAS:
        frame(table, content.get(table, [])).write_parquet(staging / f"{table}.parquet")
    for source in content["sources"]:
        src = (store.root / source["raw_path"]).resolve()
        dst = (staging / source["raw_path"]).resolve()
        if not src.is_relative_to(store.root.resolve()) or not dst.is_relative_to(staging.resolve()):
            raise ValueError("Unsafe source evidence path")
        dst.parent.mkdir(parents=True, exist_ok=True)
        if not dst.exists():
            shutil.copyfile(src, dst)
    write_json(staging / "schema.json", {"schema_version": SCHEMA_VERSION,
        "tables": {name: {k: str(v) for k, v in schema.items()} for name, schema in SCHEMAS.items()}})
    summary = dict(package_id=package_id, schema_version=SCHEMA_VERSION,
        rows={name: len(content.get(name, [])) for name in SCHEMAS},
        coverage=dict(Counter(r["status"] for r in content["coverage"])),
        event_types=dict(Counter(e["event_type"] for e in content["events"])),
        strict_public_events=sum(e.get("public_available_at") is not None and e["verification_status"] == "verified" for e in content["events"]),
        reconciliation=dict(Counter(r["result"] for r in content["reconciliation"])),
        history_complete=False, limitations=metadata.get("limitations", []))
    write_json(staging / "summary.json", summary)
    (staging / "summary.sha256").write_text(digest((staging / "summary.json").read_bytes()) + "  summary.json\n", encoding="ascii")
    artifacts = {p.relative_to(staging).as_posix(): digest(p.read_bytes()) for p in sorted(staging.rglob("*")) if p.is_file()}
    try:
        version = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=project, text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        version = "unknown"
    write_json(staging / "manifest.json", dict(schema_version=SCHEMA_VERSION, package_id=package_id,
        built_at=now(), code_version=version, code_sha256=code_hashes, parser_version=PARSER_VERSION,
        time_policy="day-only => start of following local day; unknown edition excluded from public_as_of",
        artifacts=artifacts, **metadata))
    (staging / "manifest.sha256").write_text(digest((staging / "manifest.json").read_bytes()) + "  manifest.json\n", encoding="ascii")
    validate_package(staging)
    staging.rename(target)
    return target


def validate_package(package):
    root = Path(package).resolve()
    manifest = read_json(root / "manifest.json")
    if manifest["schema_version"] != SCHEMA_VERSION:
        raise ValueError("Unsupported schema version")
    for name in ("manifest", "summary"):
        expected = (root / f"{name}.sha256").read_text(encoding="ascii").split()[0]
        if digest((root / f"{name}.json").read_bytes()) != expected:
            raise ValueError(f"{name} hash mismatch")
    for relative, sha in manifest["artifacts"].items():
        path = (root / relative).resolve()
        if not path.is_relative_to(root) or not path.is_file() or digest(path.read_bytes()) != sha:
            raise ValueError(f"Artifact hash mismatch or unsafe path: {relative}")
    tables = {}
    for name, schema in SCHEMAS.items():
        if f"{name}.parquet" not in manifest["artifacts"]:
            raise ValueError("Missing table in manifest")
        df = pl.read_parquet(root / f"{name}.parquet")
        if dict(df.schema) != schema:
            raise ValueError(f"Schema mismatch: {name}")
        tables[name] = df.to_dicts()
    for name, key in (("securities", "security_id"), ("events", "event_id"), ("sources", "source_id"), ("coverage", "security_id"), ("conflicts", "conflict_id")):
        values = [r[key] for r in tables[name]]
        if None in values or len(values) != len(set(values)):
            raise ValueError(f"Duplicate/null key: {name}.{key}")
    securities = {r["security_id"] for r in tables["securities"]}
    if {r["security_id"] for r in tables["coverage"]} != securities:
        raise ValueError("Coverage must include every security exactly once")
    sources = {r["source_id"]: r for r in tables["sources"]}
    for source in sources.values():
        if manifest["artifacts"].get(source["raw_path"]) != source["sha256"]:
            raise ValueError("Source hash missing or inconsistent")
        timestamp(source["observed_at"])
    events = {r["event_id"]: r for r in tables["events"]}
    for row in tables["coverage"]:
        if row["status"] not in {"not_checked", "partial", "source_unavailable", "no_events_found"}:
            raise ValueError("Invalid coverage status")
        if any(sid not in sources for sid in json.loads(row.get("source_ids_json") or "[]")):
            raise ValueError("Broken coverage/source link")
    for row in tables["corporate_actions"]:
        if row["source_id"] not in sources or row["security_id"] is not None and row["security_id"] not in securities:
            raise ValueError("Broken corporate action link")
        ratio=Decimal(row["ratio"])
        if not ratio.is_finite() or ratio <= 0 or row["kind"] not in {"split","reverse","auto","price","shares"}:
            raise ValueError("Invalid corporate action convention/ratio")
        from datetime import date
        date.fromisoformat(row["action_date"])
    evidence_cache = {}
    for row in events.values():
        validate_event(row)
        if row["security_id"] not in securities or row["source_id"] not in sources:
            raise ValueError("Broken event foreign key")
        source = sources[row["source_id"]]
        if any(row[k] != source[k] for k in ("sha256", "raw_path", "source_url", "document_id", "observed_at")):
            raise ValueError("Event/source evidence mismatch")
        from .disclosure import validate_archive_evidence
        validate_archive_evidence(row, sources, root, evidence_cache)
        previous = row["supersedes_id"]
        seen = {row["event_id"]}
        while previous:
            if previous in seen or previous not in events:
                raise ValueError("Cycle or missing superseded event")
            seen.add(previous)
            parent = events[previous]
            if parent["dividend_id"] != row["dividend_id"] or parent["security_id"] != row["security_id"]:
                raise ValueError("Revision crosses dividend/security identity")
            if row["public_available_at"] and parent["public_available_at"] and timestamp(row["public_available_at"]) < timestamp(parent["public_available_at"]):
                raise ValueError("Correction publication precedes superseded event")
            previous = parent["supersedes_id"]
    return {"valid": True, "package_id": manifest["package_id"], "rows": {k: len(v) for k, v in tables.items()}}
