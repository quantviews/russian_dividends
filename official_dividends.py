"""Reviewed official additions to the CSV projection; raw aggregator data stay intact."""
from datetime import date
from decimal import Decimal
import json
import hashlib
from io import BytesIO
from pathlib import Path

import polars as pl

SUPPLEMENTS_PATH = Path(__file__).resolve().parent / "metadata/official_dividends.json"
KEY = ["closing_date", "year", "period_type"]


def merge_official(frame, ticker, path=SUPPLEMENTS_PATH):
    if path is None:
        return frame
    registry = json.loads(Path(path).read_text(encoding="utf-8"))
    if registry["schema_version"] != 1:
        raise ValueError("Unsupported official dividend supplement schema")
    seen = set()
    for row in registry["entries"]:
        if row["ticker"] != ticker:
            continue
        key = tuple(str(row[k]) for k in KEY)
        if key in seen:
            raise ValueError("Duplicate official dividend key")
        seen.add(key)
        amount = Decimal(row["dividend_value"])
        if (row["status"] != "approved" or row["basis_verified"] is not True
                or row["amount_basis"] != "original_per_share" or row["currency"] != "RUB"
                or not amount.is_finite() or amount <= 0
                or not row.get("isin") or not row.get("regnumber") or not row.get("event_id")
                or len(row.get("source_sha256", "")) != 64):
            raise ValueError("Unverified official dividend supplement")
        date.fromisoformat(row["closing_date"])
        date.fromisoformat(row["basis_reviewed_at"][:10])
        match = pl.all_horizontal([pl.col(k).cast(pl.String) == str(row[k]) for k in KEY])
        old = frame.filter(match)
        if old.height > 1:
            raise ValueError(f"{ticker}: ambiguous duplicate raw dividend key {key}")
        if any(Decimal(str(v)) != amount for v in old["dividend_value"].to_list()):
            raise ValueError(f"{ticker}: raw dividend conflicts with official supplement {key}")
        addition = pl.DataFrame([{k: row[k] for k in KEY} | {"dividend_value": float(amount)}]).cast(frame.schema)
        frame = pl.concat([frame.filter(~match.fill_null(False)), addition])
    return frame.sort(["closing_date", "year"], descending=True, nulls_last=True, maintain_order=True) if seen else frame


def verified_csv_extension(baseline, current, ticker):
    """Acceptance exception for evidence-backed additions; never for changed raw CSV."""
    registry = json.loads(SUPPLEMENTS_PATH.read_text(encoding="utf-8"))
    entries = [r for r in registry['entries'] if r['ticker'] == ticker]
    if not entries or any(r.get('normalization_factor') != '1' for r in entries):
        return False
    project = SUPPLEMENTS_PATH.parent.parent
    for row in entries:
        manifest_path = (project/row['manifest_path']).resolve()
        if not manifest_path.is_relative_to(project.resolve()):
            return False
        if hashlib.sha256(manifest_path.read_bytes()).hexdigest() != row['manifest_sha256']:
            return False
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        for relative, sha in manifest['artifacts'].items():
            p = (manifest_path.parent/relative).resolve()
            if not p.is_relative_to(manifest_path.parent) or hashlib.sha256(p.read_bytes()).hexdigest() != sha:
                return False
        facts = pl.read_parquet(manifest_path.parent/'events.parquet').filter(pl.col('event_id') == row['event_id']).to_dicts()
        if len(facts) != 1:
            return False
        e = facts[0]
        if (e['ticker'], e['status'], e['amount'], e['record_date'], e['sha256']) != (
                ticker, 'approved', row['dividend_value'], row['closing_date'], row['source_sha256']):
            return False
    schema = {'closing_date': pl.String, 'year': pl.Int64, 'dividend_value': pl.Float64}
    original = pl.read_csv(BytesIO(baseline), schema_overrides=schema)
    actual = pl.read_csv(BytesIO(current), schema_overrides=schema)
    expected = merge_official(original, ticker)
    return expected.equals(actual)
