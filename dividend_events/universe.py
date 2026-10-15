"""Union of local history, moexutils registries and both active/inactive MOEX records."""

import json
from pathlib import Path
from urllib.parse import urlencode

import polars as pl

from .storage import identifier, digest, now

ISS = "https://iss.moex.com/iss"


def block(payload, name):
    value = payload.get(name)
    if not isinstance(value, dict) or "columns" not in value or "data" not in value:
        raise ValueError(f"missing_table:{name}")
    return [dict(zip(value["columns"], row, strict=True)) for row in value["data"]]


def collect_universe(project, store, moexutils=None, extra_registry=None):
    project = Path(project)
    origins, registry, classification, issues, inputs = {}, [], [], [], []

    def add(ticker, origin):
        if ticker:
            origins.setdefault(str(ticker).upper(), set()).add(origin)

    mapping_path = project / "metadata/ticker_mappings.json"
    mapping = json.loads(mapping_path.read_text(encoding="utf-8")) if mapping_path.exists() else {}
    if mapping_path.exists():
        inputs.append(store.local(mapping_path, "local_mapping")["source_id"])
    for ticker in mapping:
        add(ticker, "mapping")
    for folder in (project / "data", project / "data/raw"):
        for path in sorted(folder.glob("*.csv")):
            add(path.stem, str(folder.relative_to(project)))
            inputs.append(store.local(path, "legacy_csv")["source_id"])

    aliases = {}
    if moexutils:
        root = Path(moexutils)
        path = root / "combined_stocks.csv"
        if path.exists():
            inputs.append(store.local(path, "moexutils_universe")["source_id"])
            for ticker in pl.scan_csv(path).select("ticker").unique().collect()["ticker"]:
                add(ticker, "moexutils_history")
        for name in ("renames.csv", "delisted.csv", "splits.csv"):
            path = root / "metadata" / name
            if not path.exists():
                issues.append(f"missing_moexutils:{name}")
                continue
            source = store.local(path, "moexutils_" + name.removesuffix(".csv"))
            inputs.append(source["source_id"])
            for row in pl.read_csv(path, infer_schema=False).to_dicts():
                if name == "renames.csv":
                    for key in ("old", "new"):
                        add(row[key], "moexutils_renames")
                        aliases.setdefault(row[key], []).append({**row, "source_id": source["source_id"],
                                                                 "identity_link": "unverified"})
                else:
                    add(row["ticker"], "moexutils_" + name)

    for active in (1, 0):
        start, seen = 0, set()
        try:
            while True:
                url = ISS + "/securities.json?" + urlencode({"engine": "stock", "market": "shares",
                    "is_trading": active, "limit": 100, "start": start, "iss.meta": "off"})
                body, source = store.fetch(url, "moex_registry")
                inputs.append(source["source_id"])
                rows = block(json.loads(body), "securities")
                if not rows:
                    break
                fingerprint = digest(body)
                if fingerprint in seen:
                    raise ValueError("pagination_did_not_advance")
                seen.add(fingerprint)
                for row in rows:
                    row["_source_id"] = source["source_id"]
                    kind = row.get("type", "")
                    equity = kind in {"common_share", "preferred_share", "depositary_receipt"}
                    ticker = row["secid"]
                    classification.append(dict(ticker=ticker, instrument_type=kind,
                        classification="included" if equity or ticker in origins else "excluded",
                        reason="equity_or_receipt" if equity else "local_universe_needs_review" if ticker in origins else "outside_equity_scope",
                        source_id=source["source_id"]))
                    if equity or ticker in origins:
                        registry.append(row)
                        add(ticker, "moex_active" if active else "moex_inactive")
                start += len(rows)
        except Exception as exc:
            issues.append(f"moex_registry_{active}:{type(exc).__name__}:{exc}")

    if extra_registry:
        source = store.local(extra_registry, "verified_extra_registry")
        inputs.append(source["source_id"])
        for row in json.loads(Path(extra_registry).read_text(encoding="utf-8")):
            if not row.get("isin") and not row.get("regnumber"):
                raise ValueError("Extra disclosures require verified ISIN or registration number")
            registry.append({**row, "_source_id": source["source_id"]})
            add(row["secid"], "extra_registry")

    # Registration number matters: equal ISINs in old ISS rows do not necessarily identify an issue.
    indexed = {}
    for row in registry:
        indexed.setdefault(row["secid"], []).append(row)
    securities = []
    for ticker in sorted(origins):
        matches = indexed.get(ticker, [{}])
        if ticker not in indexed:
            classification.append(dict(ticker=ticker, instrument_type="unknown", classification="needs_review",
                reason="local_ticker_has_no_verified_moex_issue_identity", source_id=None))
        unique = {}
        for row in matches:
            key = (row.get("isin"), row.get("regnumber"))
            unique.setdefault(key, row)
        for row in unique.values():
            isin, regnumber = row.get("isin"), row.get("regnumber")
            sid = identifier("security", [isin, regnumber]) if isin or regnumber else identifier("unresolved", ticker)
            kind = row.get("type", "unknown")
            securities.append(dict(security_id=sid, issuer_id="moex:" + str(row["emitent_id"]) if row.get("emitent_id") else None,
                ticker=ticker, isin=isin, regnumber=regnumber, name=row.get("name"),
                share_class={"common_share": "ordinary", "preferred_share": "preferred"}.get(kind, "unknown"),
                instrument_type=kind, jurisdiction=None, currency=None, depositary_ratio=None,
                identity_status="identified" if isin or regnumber else "unresolved",
                aliases_json=json.dumps(aliases.get(ticker, []), ensure_ascii=False),
                source_ids_json=json.dumps([row["_source_id"]] if row.get("_source_id") else []),
                origin_json=json.dumps(sorted(origins[ticker])),
                mapping_status="present" if ticker in mapping or ticker.endswith("P") and ticker[:-1] in mapping else "absent",
                mapping_missing_observed_at=None if ticker in mapping or ticker.endswith("P") and ticker[:-1] in mapping else now(),
                listing_status="active" if row.get("is_traded") == 1 else "inactive" if row.get("is_traded") == 0 else "unknown"))
    # Collapse real aliases sharing issue identifiers; retain all ticker observations as aliases.
    merged = {}
    for row in securities:
        sid = row["security_id"]
        if sid not in merged:
            merged[sid] = row
            row["aliases_json"] = json.dumps(json.loads(row["aliases_json"]) + [{"ticker": row["ticker"], "valid_from": None, "valid_to": None}])
        else:
            kept = merged[sid]
            kept["aliases_json"] = json.dumps(json.loads(kept["aliases_json"]) + json.loads(row["aliases_json"]) +
                                               [{"ticker": row["ticker"], "valid_from": None, "valid_to": None}])
            kept["source_ids_json"] = json.dumps(sorted(set(json.loads(kept["source_ids_json"]) + json.loads(row["source_ids_json"]))))
            kept["origin_json"] = json.dumps(sorted(set(json.loads(kept["origin_json"]) + json.loads(row["origin_json"]))))
            if row["listing_status"] == "active":
                for field in ("ticker", "listing_status", "name", "issuer_id"):
                    kept[field] = row[field]
    return list(merged.values()), classification, {"issues": issues, "input_source_ids": sorted(set(inputs)),
        "mapping": mapping, "local_and_moex_tickers": len(origins), "moex_pagination_complete": not any(i.startswith("moex_registry") for i in issues)}
