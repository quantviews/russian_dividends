"""python -m dividend_events --help"""

import argparse
from datetime import date
import json
import os
from pathlib import Path
import re

import polars as pl

from .collect import collect_all, import_reviewed
from .package import build_package, corporate_actions, detect_conflicts, reconcile, validate_package
from .reader import read_as_of
from .model import merge_revisions
from .storage import EvidenceStore, digest, canonical, read_json, write_json
from .universe import collect_universe


def run(args):
    project = Path(args.project).resolve()
    root = Path(args.root).resolve()
    if not args.run_id or Path(args.run_id).name != args.run_id or args.run_id in {".", ".."}:
        raise ValueError("run-id must be one directory name")
    date.fromisoformat(args.start)
    date.fromisoformat(args.end)
    if args.start > args.end:
        raise ValueError("start must precede end")
    run_dir = root / "runs" / args.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    config = read_json(args.sources) if args.sources else []
    settings = dict(project=str(project), moexutils=str(Path(args.moexutils).resolve()) if args.moexutils else None,
        start=args.start, end=args.end, sources=config, previous=str(Path(args.previous).resolve()) if args.previous else None,
        extra_registry_digest=digest(Path(args.registry).read_bytes()) if args.registry else None)
    settings["reviewed_digest"] = digest(Path(args.reviewed).read_bytes()) if args.reviewed else None
    store = EvidenceStore(root / "evidence", delay=args.delay, timeout=args.timeout, retries=args.retries,
                          offline=args.offline, refresh=args.command == "update")
    previous_events, previous_source_ids = [], []
    if args.previous:
        previous = Path(args.previous)
        validate_package(previous)
        previous_events = pl.read_parquet(previous / "events.parquet").to_dicts()
        for source in pl.read_parquet(previous / "sources.parquet").to_dicts():
            previous_source_ids.append(source["source_id"])
            raw = previous / source["raw_path"]
            dst = store.root / source["raw_path"]
            dst.parent.mkdir(parents=True, exist_ok=True)
            if not dst.exists():
                dst.write_bytes(raw.read_bytes())
            receipt = store.root / "receipts" / (source["source_id"].split(":")[1] + ".json")
            if receipt.exists() and read_json(receipt) != source:
                raise ValueError("Receipt differs from pinned parent; use a fresh --root to retain original observation")
            if not receipt.exists():
                write_json(receipt, source)
    frozen = run_dir / "universe.json"
    if frozen.exists():
        snapshot = read_json(frozen)
        if snapshot["settings"] != settings:
            raise ValueError("Run settings changed; use a new run-id")
        securities, classification, discovery = snapshot["securities"], snapshot["classification"], snapshot["discovery"]
    else:
        securities, classification, discovery = collect_universe(project, store, args.moexutils, args.registry)
        if args.previous:
            validate_package(args.previous)
            current = {s["security_id"]: s for s in securities}
            for old in pl.read_parquet(Path(args.previous) / "securities.parquet").to_dicts():
                current.setdefault(old["security_id"], old)
            securities = list(current.values())
        snapshot = dict(settings=settings, securities=securities, classification=classification, discovery=discovery)
        write_json(frozen, snapshot)
    print(f"Frozen universe: {len(securities)} issues; {discovery['local_and_moex_tickers']} ticker observations", flush=True)
    events, coverage = collect_all(securities, discovery["mapping"], store, args.start, args.end, run_dir, config)
    events = merge_revisions(previous_events, events)
    if args.reviewed:
        events = import_reviewed(args.reviewed, securities, store, events)
    events = list({e["event_id"]: e for e in events}.values())
    actions_path = run_dir / "corporate_actions.json"
    if actions_path.exists():
        actions = read_json(actions_path)
    else:
        actions = corporate_actions(project, args.moexutils, securities, store)
        write_json(actions_path, actions)
    needed = set(discovery["input_source_ids"]) | set(previous_source_ids)
    needed.update(e["source_id"] for e in events)
    needed.update(a["source_id"] for a in actions)
    for row in coverage:
        related = [e for e in events if e["security_id"] == row["security_id"]]
        counts = {}
        for e in related:
            counts[e["event_type"]] = counts.get(e["event_type"], 0) + 1
        row["event_counts_json"] = json.dumps(counts)
        if any(e["verification_status"] == "verified" for e in related):
            row["status"] = "partial"
        ids = set(json.loads(row["source_ids_json"])) | {e["source_id"] for e in related}
        ids.update(re.findall(r"source:[a-f0-9]{32}", row["reasons_json"]))
        row["source_ids_json"] = json.dumps(sorted(ids))
        needed.update(ids)
    needed.update(sid for s in securities for sid in json.loads(s.get("source_ids_json") or "[]"))
    sources = [s for s in store.sources() if s["source_id"] in needed]
    dates = sorted(e["record_date"] for e in events if e.get("record_date"))
    tables = dict(securities=securities, events=events, sources=sources, coverage=coverage,
        conflicts=detect_conflicts(events, actions), corporate_actions=actions,
        reconciliation=reconcile(securities, sources, events, store), classification=classification)
    metadata = dict(requested_from=args.start, requested_to=args.end,
        snapshot_at=args.end + "T23:59:59.999999+03:00", universe_sha256=digest(canonical(securities)),
        registry_discovery=discovery, parent_package=settings["previous"],
        actual_record_date_from=dates[0] if dates else None, actual_record_date_to=dates[-1] if dates else None,
        limitations=["Official historical disclosure availability is not established for all securities",
            "Aggregator reference events are excluded from both strict reading modes",
            "Legacy CSV does not prove publication time, payment or original share basis",
            "ISIN jurisdiction, receipt ratios and unknown aliases require primary-source verification",
            "Corporate action registry conflicts are retained; unverified adjustments are not applied"])
    path = build_package(root / "packages", tables, store, metadata, project)
    write_json(run_dir / "result.json", {"package": str(path), "validation": validate_package(path)})
    print(json.dumps({"package": str(path), "summary": read_json(path / "summary.json")}, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(description="Immutable, evidence-backed dividend event packages")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("backfill", "update"):
        p = sub.add_parser(name)
        p.add_argument("--project", default=".")
        p.add_argument("--root", default=os.environ.get("DIVIDENDS_EVENTS_ROOT", "artifacts/dividend-events"))
        p.add_argument("--run-id", required=True)
        p.add_argument("--start", default="2023-01-01")
        p.add_argument("--end", default="2026-10-05")
        p.add_argument("--moexutils")
        p.add_argument("--registry", help="JSON additional verified issue identifiers")
        p.add_argument("--sources", help="JSON official document locators")
        p.add_argument("--reviewed", help="JSON verified facts with evidence excerpts")
        p.add_argument("--previous", help="Pinned previous package, required for update")
        p.add_argument("--offline", action="store_true")
        p.add_argument("--delay", type=float, default=2)
        p.add_argument("--timeout", type=float, default=25)
        p.add_argument("--retries", type=int, default=2)
    p = sub.add_parser("validate")
    p.add_argument("package")
    p = sub.add_parser("export")
    p.add_argument("package")
    p.add_argument("--at", required=True)
    p.add_argument("--mode", choices=["public_as_of", "observed_as_of"], default="public_as_of")
    p.add_argument("--output", required=True, help="New Parquet or JSON file; legacy CSV never overwritten")
    p.add_argument("--all-events", action="store_true")
    args = parser.parse_args()
    if args.command in {"backfill", "update"}:
        if args.command == "update" and not args.previous:
            parser.error("update requires --previous; history must not be discarded")
        run(args)
    elif args.command == "validate":
        print(json.dumps(validate_package(args.package), indent=2))
    else:
        output = Path(args.output)
        data = read_as_of(args.package, args.at, args.mode, state=not args.all_events)
        if output.exists():
            identical = output.suffix == ".json" and read_json(output) == data.to_dicts()
            if output.suffix == ".parquet":
                identical = pl.read_parquet(output).equals(data)
            if not identical:
                raise FileExistsError("Refusing to overwrite different export: " + str(output))
            print(f"Identical export already exists: {output}")
            return
        output.parent.mkdir(parents=True, exist_ok=True)
        if output.suffix == ".parquet":
            data.write_parquet(output)
        elif output.suffix == ".json":
            write_json(output, data.to_dicts())
        else:
            raise ValueError("Export suffix must be .parquet or .json")
        print(f"Exported {len(data)} events to {output}")


if __name__ == "__main__":
    main()
