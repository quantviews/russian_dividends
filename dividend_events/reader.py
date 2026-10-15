"""Offline point-in-time reads. Callers must name an immutable package directory."""

import json
from pathlib import Path

from .model import timestamp
from .schema import frame


def read_as_of(package, at, mode="public_as_of", *, state=True):
    from .package import validate_package
    validate_package(package)
    import polars as pl
    if mode not in {"public_as_of", "observed_as_of"}:
        raise ValueError("Mode must be public_as_of or observed_as_of")
    boundary = timestamp(at)
    rows = pl.read_parquet(Path(package) / "events.parquet").to_dicts()
    return select_as_of(rows, boundary, mode, state=state)


def read_many_as_of(package, requests):
    """Validate once and answer a batch from one in-memory immutable event snapshot."""
    from .package import validate_package
    import polars as pl
    validate_package(package)
    rows = pl.read_parquet(Path(package) / "events.parquet").to_dicts()
    return [select_as_of(rows, timestamp(q["at"]), q.get("mode", "public_as_of"), state=q.get("state", True)) for q in requests]


def select_as_of(rows, boundary, mode, *, state=True):
    if mode not in {"public_as_of", "observed_as_of"}:
        raise ValueError("Mode must be public_as_of or observed_as_of")
    eligible = {}
    for row in rows:
        if row["verification_status"] != "verified":
            continue
        value = row["public_available_at"] if mode == "public_as_of" else row["observed_at"]
        if value is not None and timestamp(value) <= boundary:
            eligible[row["event_id"]] = row
    # Filter first, then resolve versions: a late correction cannot erase early history.
    if state:
        superseded = {r["supersedes_id"] for r in eligible.values() if r["supersedes_id"]}
        # Walk ancestors so an omitted intermediate revision does not resurrect an obsolete state.
        all_rows = {r["event_id"]: r for r in rows}
        for key in list(superseded):
            seen = set()
            while key in all_rows and key not in seen:
                seen.add(key)
                key = all_rows[key]["supersedes_id"]
                if key:
                    superseded.add(key)
        eligible = {k: v for k, v in eligible.items() if k not in superseded}
    return frame("events", sorted(eligible.values(), key=lambda r: (r["dividend_id"], r["event_id"])))


def normalized_amount(event, actions, target_basis_date, known_at, *, mode="public_as_of"):
    """Return a separate normalization with provenance; never modify the announced amount."""
    from datetime import date
    from decimal import Decimal
    if mode not in {"public_as_of", "observed_as_of"}:
        raise ValueError("Invalid time mode")
    target = date.fromisoformat(target_basis_date)
    basis = event.get("share_basis_date")
    if not basis or event.get("amount") is None:
        raise ValueError("Original share basis and amount must be known")
    basis = date.fromisoformat(basis)
    if target < basis:
        raise ValueError("Target basis must not precede original basis")
    cutoff = timestamp(known_at)
    factor, used = Decimal(1), []
    for action in actions:
        if action["security_id"] != event["security_id"]:
            continue
        when = date.fromisoformat(action["action_date"])
        if basis < when <= target:
            availability = action.get("public_available_at" if mode == "public_as_of" else "observed_at")
            if action["verification_status"] != "verified" or not availability or timestamp(availability) > cutoff:
                raise ValueError("Corporate action lacks verified point-in-time evidence")
            ratio = Decimal(action["ratio"])
            if ratio <= 0 or action["kind"] not in {"split", "reverse"}:
                raise ValueError("Invalid corporate action")
            factor *= 1 / ratio if action["kind"] == "split" else ratio
            used.append(action["action_id"])
    return {"amount": str(Decimal(event["amount"]) * factor), "factor": str(factor),
            "target_basis_date": target_basis_date, "action_ids": used}
