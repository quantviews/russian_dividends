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

import pandas as pd

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


def adjustment_factor(splits: List[dict], as_of: Optional[pd.Timestamp]) -> float:
    """
    Multiplier to convert a historical (as-declared) dividend onto the
    current share basis.

    - forward split (kind omitted / "split"), ratio R (1→R shares): factor *= 1/R
    - reverse split (kind "reverse"), ratio R (R→1 shares): factor *= R
    """
    factor = 1.0
    for split in splits:
        split_date = pd.Timestamp(split["date"])
        # Missing date → treat as pre-event (historical placeholder rows)
        if as_of is None or pd.isna(as_of) or as_of < split_date:
            r = float(split["ratio"])
            kind = split.get("kind", "split")
            if kind == "reverse":
                factor *= r
            else:
                factor /= r
    return factor


def normalize_value_column(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "dividend_value_post_split" in df.columns and "dividend_value" not in df.columns:
        df = df.rename(columns={"dividend_value_post_split": "dividend_value"})
    elif "dividend_value_post_split" in df.columns:
        df = df.drop(columns=["dividend_value_post_split"])
    return df


def adjust_dataframe(df: pd.DataFrame, splits: List[dict]) -> pd.DataFrame:
    df = normalize_value_column(df)
    if df.empty or not splits:
        return df

    df["closing_date"] = pd.to_datetime(df["closing_date"], errors="coerce")
    factors = df["closing_date"].apply(lambda d: adjustment_factor(splits, d))
    df["dividend_value"] = (df["dividend_value"].astype(float) * factors).round(6)
    return df


def detect_jumps(df: pd.DataFrame, ticker: str, threshold: float = JUMP_THRESHOLD) -> List[dict]:
    """Flag consecutive non-zero dividends whose ratio exceeds threshold."""
    if df.empty or "dividend_value" not in df.columns:
        return []

    work = df.copy()
    work["closing_date"] = pd.to_datetime(work["closing_date"], errors="coerce")
    work = work.sort_values(["closing_date", "year"], ascending=False, na_position="last")
    work = work[work["dividend_value"].astype(float) > 0].reset_index(drop=True)

    anomalies = []
    for i in range(len(work) - 1):
        a = float(work.loc[i, "dividend_value"])
        b = float(work.loc[i + 1, "dividend_value"])
        if b <= 0:
            continue
        jump = max(a / b, b / a)
        if jump >= threshold:
            # Same-day pairs are usually special vs regular, not splits
            d1 = work.loc[i, "closing_date"]
            d2 = work.loc[i + 1, "closing_date"]
            if pd.notna(d1) and pd.notna(d2) and d1 == d2:
                continue
            anomalies.append({
                "ticker": ticker,
                "newer_date": str(work.loc[i, "closing_date"].date())
                if pd.notna(work.loc[i, "closing_date"]) else None,
                "newer_value": a,
                "older_date": str(work.loc[i + 1, "closing_date"].date())
                if pd.notna(work.loc[i + 1, "closing_date"]) else None,
                "older_value": b,
                "jump_ratio": round(jump, 2),
            })
    return anomalies


def process_file(
    ticker: str,
    raw_path: str,
    out_path: str,
    splits_map: Dict[str, List[dict]],
) -> Tuple[bool, List[dict], List[dict]]:
    df_raw = pd.read_csv(raw_path)
    df_raw = normalize_value_column(df_raw)

    raw_anomalies = detect_jumps(df_raw, ticker)
    splits = splits_map.get(ticker, [])
    df_adj = adjust_dataframe(df_raw, splits)

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    df_adj.to_csv(out_path, index=False)

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
