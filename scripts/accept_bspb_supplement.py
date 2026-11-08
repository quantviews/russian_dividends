"""Apply the reviewed CSV additions and accept the BSPB adjustment in moexutils."""
from datetime import date
from pathlib import Path
import sys

import polars as pl

PROJECT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT))
sys.path.insert(0, str(PROJECT.parent/'moexutils'))
from adjust_splits import load_splits, process_file
from dividend_events.storage import digest, read_json, write_json, now
from moexutils import stocks, lake

ROOT = PROJECT/'artifacts/bspb-2026-h1'


def main():
    registry = read_json(PROJECT/'metadata/official_dividends.json')
    entries = [r for r in registry['entries'] if r['supplement_id'] in {'BSPB-2026-h1-v1', 'BSPBP-2026-h1-v1'}]
    assert len(entries) == 2
    for row in entries:
        manifest_path = PROJECT/row['manifest_path']
        assert digest(manifest_path.read_bytes()) == row['manifest_sha256']
        for relative, sha in read_json(manifest_path)['artifacts'].items():
            assert digest((manifest_path.parent/relative).read_bytes()) == sha
    if (ROOT/'acceptance.json').exists():
        prior = read_json(ROOT/'acceptance.json')
        assert digest((PROJECT/'metadata/official_dividends.json').read_bytes()) == prior['registry_sha256']
        for ticker, sha in prior['csv_sha256'].items():
            assert digest((PROJECT/'data'/f'{ticker}.csv').read_bytes()) == sha
        for table in ('stocks', 'stocks_adjusted'):
            current = lake.query(f"SELECT * FROM {lake.ref(table)} WHERE ticker IN ('BSPB','BSPBP') ORDER BY ticker,date")
            assert current.equals(pl.read_parquet(ROOT/f'{table}-accepted.parquet'))
        print('Existing acceptance verified; no writes performed.', flush=True)
        return
    before_hashes = read_json(ROOT/'before-csv.json')
    for ticker in ('BSPB', 'BSPBP'):
        raw, out = PROJECT/'data/raw'/f'{ticker}.csv', PROJECT/'data'/f'{ticker}.csv'
        process_file(ticker, str(raw), str(out), load_splits(str(PROJECT/'metadata/splits.json')))
        first = out.read_bytes()
        process_file(ticker, str(raw), str(out), load_splits(str(PROJECT/'metadata/splits.json')))
        assert out.read_bytes() == first
    for relative, sha in before_hashes.items():
        if Path(relative).as_posix() not in {'data/BSPB.csv', 'data/BSPBP.csv'}:
            assert digest((PROJECT/relative).read_bytes()) == sha, relative
    for ticker, amount in [('BSPB', 19.17), ('BSPBP', .22)]:
        dividend = stocks.load_dividends(ticker).filter(pl.col('closing_date') == date(2026, 10, 5))
        assert dividend.height == 1 and dividend['dividend_value'][0] == amount
    before = pl.read_parquet(ROOT/'moex-stocks-before.parquet').sort('ticker', 'date')
    candidate = pl.read_parquet(ROOT/'moex-stocks-candidate.parquet').sort('ticker', 'date')
    assert before.drop('adj_close').equals(candidate.drop('adj_close'))
    assert before.filter(pl.col('date') >= date(2026, 10, 5)).equals(candidate.filter(pl.col('date') >= date(2026, 10, 5)))
    # Update only adj_close of the two requested securities, in both tables atomically.
    # No sync() here: sync removes keys outside the supplied frame.
    counts = {}
    with lake.session() as con:
        con.execute('BEGIN')
        try:
            for table in ('stocks', 'stocks_adjusted'):
                current = con.execute(f"SELECT * FROM lake.{table} WHERE ticker IN ('BSPB','BSPBP') ORDER BY ticker,date").pl()
                if table == 'stocks':
                    assert current.equals(before) or current.equals(candidate), 'Consumer changed concurrently'
                assert current.select('date', 'ticker').equals(candidate.select('date', 'ticker')), 'Derived table keys differ'
                target = (current.drop('adj_close').join(candidate.select('date', 'ticker', 'adj_close'),
                    on=['date', 'ticker'], how='left').select(current.columns).sort('ticker', 'date'))
                current.write_parquet(ROOT/f'{table}-before-apply.parquet')
                target.write_parquet(ROOT/f'{table}-accepted.parquet')
                delta = lake.changed_rows(current, target, ['date', 'ticker'])
                counts[table] = delta.height
                if delta.height:
                    con.register('_bspb_dividend_delta', delta.select('date', 'ticker', 'adj_close'))
                    con.execute(f'UPDATE lake.{table} AS t SET adj_close=s.adj_close FROM _bspb_dividend_delta AS s '
                                'WHERE t.date=s.date AND t.ticker=s.ticker')
                    con.unregister('_bspb_dividend_delta')
            con.execute('COMMIT')
        except Exception:
            con.execute('ROLLBACK')
            raise
    for table in ('stocks', 'stocks_adjusted'):
        after = lake.query(f"SELECT * FROM {lake.ref(table)} WHERE ticker IN ('BSPB','BSPBP') ORDER BY ticker,date")
        assert after.equals(pl.read_parquet(ROOT/f'{table}-accepted.parquet'))
    after = stocks.read_stocks(['BSPB','BSPBP'], merge_renames=False)
    repeated, skipped = stocks.adj_close(after, stocks.load_dividends('BSPB'), stocks.load_splits())
    assert not skipped and lake.changed_rows(after, repeated, ['date', 'ticker']).height == 0
    # Current pinned consumer has BSPB prices only; pref dividend import is checked separately.
    assert set(after['ticker'].unique()) == {'BSPB'}
    preview = read_json(ROOT/'consumer-preview.json')
    audit = dict(status='accepted', accepted_at=now(), scope='BSPB/BSPBP 2026-H1 supplements and BSPB price-gap correction',
        registry_sha256=digest((PROJECT/'metadata/official_dividends.json').read_bytes()),
        packages=[dict(package_id=r['package_id'], manifest_sha256=r['manifest_sha256']) for r in entries],
        csv_sha256={t: digest((PROJECT/'data'/f'{t}.csv').read_bytes()) for t in ('BSPB','BSPBP')},
        raw_and_unrelated_csv_unchanged=True, csv_rebuild_idempotent=True, consumer_recalculation_idempotent=True,
        changed_rows=counts, gap_check=preview, csv_equivalence=read_json(ROOT/'csv-equivalence.json'),
        limitations=['BSPBP has no price rows in the current moexutils snapshot; its CSV import is verified.',
                    'No actual investor payment date is asserted.',
                    'This scoped acceptance does not complete the full dividend-universe archive acceptance.'])
    write_json(ROOT/'acceptance.json', audit)
    print(audit, flush=True)


if __name__ == '__main__':
    main()
