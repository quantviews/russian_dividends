"""
Apply stock-split adjustments to raw dividend CSVs.

Reads:  data/raw/*.csv  (as declared on the source site)
Writes: data/*.csv      (per-share amounts on current share basis)

For each corporate action after a row's closing_date:
  - kind "split"   (1→R): divide dividend by R
  - kind "reverse" (R→1): multiply dividend by R
"""

from __future__ import annotations

import json
import os
from typing import Dict, List, Optional, Tuple

import polars as pl
from datetime import date
from official_dividends import merge_official, SUPPLEMENTS_PATH

RAW_DIR = "data/raw"
OUT_DIR = "data"
SPLITS_PATH = "metadata/splits.json"
ANOMALIES_PATH = "metadata/split_anomalies.json"
JUMP_THRESHOLD = 50.0


def load_splits(path: str = SPLITS_PATH) -> Dict[str, List[dict]]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def adjustment_factor(splits: List[dict], as_of: Optional[date]) -> float:
    """
    Multiplier to convert a historical (as-declared) dividend onto the
    current share basis.

    - forward split (kind omitted / "split"), ratio R (1→R shares): factor *= 1/R
    - reverse split (kind "reverse"), ratio R (R→1 shares): factor *= R
    """
    factor = 1.0
    for split in splits:
        split_date = date.fromisoformat(split["date"])
        # Missing date → treat as pre-event (historical placeholder rows)
        if as_of is None or as_of < split_date:
            r = float(split["ratio"])
            kind = split.get("kind", "split")
            if kind == "reverse":
                factor *= r
            else:
                factor /= r
    return factor


def normalize_value_column(df: pl.DataFrame) -> pl.DataFrame:
    if "dividend_value_post_split" in df.columns and "dividend_value" not in df.columns:
        return df.rename({"dividend_value_post_split": "dividend_value"})
    if "dividend_value_post_split" in df.columns:
        return df.drop("dividend_value_post_split")
    return df


def adjust_dataframe(df: pl.DataFrame, splits: List[dict]) -> pl.DataFrame:
    df = normalize_value_column(df)
    if df.is_empty() or not splits:
        return df
    dates = pl.col("closing_date").cast(pl.String).str.to_date(strict=False)
    factor = pl.lit(1.0)
    for split in splits:
        ratio = float(split["ratio"])
        multiplier = ratio if split.get("kind", "split") == "reverse" else 1 / ratio
        factor = factor * pl.when(dates.is_null() | (dates < date.fromisoformat(split["date"]))).then(multiplier).otherwise(1.0)
    return df.with_columns((pl.col("dividend_value").cast(pl.Float64) * factor).round(6))


def detect_jumps(df: pl.DataFrame, ticker: str, threshold: float = JUMP_THRESHOLD) -> List[dict]:
    if df.is_empty() or "dividend_value" not in df.columns:
        return []
    rows = (df.with_columns(pl.col("closing_date").cast(pl.String).str.to_date(strict=False))
            .filter(pl.col("dividend_value").cast(pl.Float64) > 0)
            .sort(["closing_date", "year"], descending=True, nulls_last=True).to_dicts())
    anomalies = []
    for newer, older in zip(rows, rows[1:]):
        a, b = float(newer["dividend_value"]), float(older["dividend_value"])
        jump = max(a / b, b / a)
        d1, d2 = newer["closing_date"], older["closing_date"]
        if jump < threshold or d1 is not None and d1 == d2:
            continue
        anomalies.append(dict(ticker=ticker, newer_date=str(d1) if d1 else None,
            newer_value=a, older_date=str(d2) if d2 else None, older_value=b, jump_ratio=round(jump, 2)))
    return anomalies


def process_file(
    ticker: str,
    raw_path: str,
    out_path: str,
    splits_map: Dict[str, List[dict]],
    supplements_path=SUPPLEMENTS_PATH,
) -> Tuple[bool, List[dict], List[dict]]:
    df_raw = pl.read_csv(raw_path, schema_overrides={"closing_date": pl.String, "year": pl.Int64, "dividend_value": pl.Float64})
    df_raw = normalize_value_column(df_raw)
    df_raw = merge_official(df_raw, ticker, supplements_path)

    raw_anomalies = detect_jumps(df_raw, ticker)
    splits = splits_map.get(ticker, [])
    df_adj = adjust_dataframe(df_raw, splits)

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    df_adj.write_csv(out_path)

    adj_anomalies = detect_jumps(df_adj, ticker)
    return bool(splits), raw_anomalies, adj_anomalies


def adjust_all(
    raw_dir: str = RAW_DIR,
    out_dir: str = OUT_DIR,
    splits_path: str = SPLITS_PATH,
) -> dict:
    splits_map = load_splits(splits_path)
    os.makedirs(out_dir, exist_ok=True)

    if not os.path.isdir(raw_dir):
        raise FileNotFoundError(f"Raw directory not found: {raw_dir}")

    summary = {
        "adjusted_tickers": [],
        "passthrough_tickers": [],
        "raw_anomalies": [],
        "remaining_anomalies": [],
        "files_processed": 0,
    }

    for name in sorted(os.listdir(raw_dir)):
        if not name.endswith(".csv"):
            continue
        ticker = name[:-4].upper()
        raw_path = os.path.join(raw_dir, name)
        out_path = os.path.join(out_dir, name)

        had_splits, raw_anoms, adj_anoms = process_file(
            ticker, raw_path, out_path, splits_map
        )
        summary["files_processed"] += 1
        if had_splits:
            summary["adjusted_tickers"].append(ticker)
        else:
            summary["passthrough_tickers"].append(ticker)
        summary["raw_anomalies"].extend(raw_anoms)
        summary["remaining_anomalies"].extend(adj_anoms)

    with open(ANOMALIES_PATH, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    return summary


def main():
    summary = adjust_all()
    print(f"Processed: {summary['files_processed']}")
    print(f"Split-adjusted: {', '.join(summary['adjusted_tickers']) or '(none)'}")
    print(f"Raw jump anomalies: {len(summary['raw_anomalies'])}")
    print(f"Remaining after adjust: {len(summary['remaining_anomalies'])}")
    if summary["remaining_anomalies"]:
        print("\nPossible missing splits (jumps still present):")
        for a in summary["remaining_anomalies"]:
            print(
                f"  {a['ticker']}: {a['newer_value']} ({a['newer_date']}) vs "
                f"{a['older_value']} ({a['older_date']}) — {a['jump_ratio']}x"
            )
    print(f"\nDetails: {ANOMALIES_PATH}")


if __name__ == "__main__":
    main()
