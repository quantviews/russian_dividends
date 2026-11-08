import json

import polars as pl
import pytest

from adjust_splits import process_file, adjust_dataframe
from official_dividends import merge_official
from test_disclosure_archives import body, parse, security


def test_meeting_kopecks_cyrillic_registration_and_attached_year_suffix(tmp_path):
    ordinary = dict(security(), isin="RU0009100945", regnumber="10300436B")
    additional = dict(ordinary, security_id="additional", ticker="additional", regnumber="10300436B013D")
    preferred = dict(security("preferred"), isin="RU000A0JP0U9", regnumber="20100436B")
    content = body('Выплатить дивиденды по результатам полугодия 2026 года: '
        'по обыкновенным акциям 19 руб. 17 коп. на 1 обыкновенную акцию; '
        'по привилегированным акциям 0 руб. 22 коп. на 1 привилегированную акцию. '
        'Утвердить дату, на которую определяются лица, имеющие право на получение дивидендов – 05 октября 2026г. '
        'РЕШЕНИЕ ПРИНЯТО. Государственный регистрационный номер выпуска акций: 10300436В;',
        title="Решения общих собраний участников (акционеров)")
    events, _, _ = parse(tmp_path, content, [ordinary, additional, preferred], title="Решения общих собраний участников (акционеров)")
    assert {(e["security_id"], e["amount"], e["record_date"]) for e in events} == {
        (ordinary["security_id"], "19.17", "2026-10-05"), (preferred["security_id"], "0.22", "2026-10-05")}
    assert all(e["status"] == "approved" and e["actual_payment_date"] is None for e in events)
    changed = content.decode("cp1251").replace("10300436В;", "10300436В013D;").encode("cp1251")
    events, _, _ = parse(tmp_path, changed, [ordinary, additional, preferred], title="Решения общих собраний участников (акционеров)")
    assert "additional" in {e["security_id"] for e in events}
    assert ordinary["security_id"] not in {e["security_id"] for e in events}


def registry(tmp_path):
    path = tmp_path / "official.json"
    path.write_text(json.dumps(dict(schema_version=1, entries=[dict(ticker="TEST", closing_date="2026-10-05",
        year=2026, period_type="half year", dividend_value="19.17", status="approved", basis_verified=True,
        amount_basis="original_per_share", currency="RUB", isin="RU0009100945", regnumber="10300436B",
        event_id="event:test", source_sha256="a"*64, basis_reviewed_at="2026-11-08T12:00:00Z")])) ,encoding="utf-8")
    return path


def test_rebuild_preserves_raw_and_official_row_and_never_duplicates(tmp_path):
    raw = tmp_path / "raw.csv"
    raw.write_text("closing_date,year,period_type,dividend_value\n2026-05-12,2025,full year,26.23\n")
    original = raw.read_bytes()
    out = tmp_path / "out.csv"
    path = registry(tmp_path)
    process_file("TEST", str(raw), str(out), {}, path)
    first = out.read_bytes()
    process_file("TEST", str(raw), str(out), {}, path)
    assert out.read_bytes() == first and raw.read_bytes() == original
    df = pl.read_csv(out)
    assert df.height == 2
    assert merge_official(df, "TEST", path).height == 2
    assert merge_official(df.head(0), "OTHER", path).height == 0
    conflicting = df.with_columns(pl.when(pl.col("closing_date") == "2026-10-05")
        .then(0.0).otherwise(pl.col("dividend_value")).alias("dividend_value"))
    with pytest.raises(ValueError, match="conflicts"):
        merge_official(conflicting, "TEST", path)


def test_official_rows_are_adjusted_once_on_original_share_basis(tmp_path):
    frame = pl.DataFrame(dict(closing_date=["2026-10-05", "2026-11-01"], year=[2026, 2026],
                             period_type=["half year", "9 months"], dividend_value=[19.17, 3.0]))
    adjusted = adjust_dataframe(frame, [dict(date="2026-11-01", ratio=10, kind="split")])
    assert adjusted["dividend_value"].to_list() == [1.917, 3.0]
