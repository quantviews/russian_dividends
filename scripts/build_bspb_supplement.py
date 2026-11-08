"""Reproduce the reviewed BSPB/BSPBP 2026-H1 supplement from pinned evidence."""
import json
from pathlib import Path
import shutil

import polars as pl

from dividend_events.disclosure import parse_message, validate_archive_evidence
from dividend_events.model import validate_event
from dividend_events.storage import canonical, digest, read_json, write_json

PROJECT = Path(__file__).resolve().parent.parent
PARENT = PROJECT / 'artifacts/dividend-events/packages/dividends-72b865ea11b444dd44b012f4'
ROOT = PROJECT / 'artifacts/bspb-2026-h1'


def build():
    assert digest((PARENT/'manifest.json').read_bytes()) == '3ae60701a69c90f4346bd3e02c1fdc9cdc6b6ba6c80f0414b74677bd1409787b'
    old = pl.read_parquet(PARENT/'events.parquet').filter(
        (pl.col('ticker') == 'BSPBP') & (pl.col('event_type') == 'approval') &
        (pl.col('published_at') == '2026-09-25T15:24:03+03:00')).to_dicts()[0]
    sources = {s['source_id']: s for s in pl.read_parquet(PARENT/'sources.parquet').to_dicts()}
    proof = json.loads(old['edition_evidence'])
    source, policy = sources[old['source_id']], sources[proof['policy_source_id']]
    body = (PARENT/source['raw_path']).read_bytes()
    assert digest(body) == source['sha256'] == '5c8999fb48ce6ed530ede7bb6acaa23c4e31862353e2a82c00480a38f034e8d1'
    groups = read_json(PROJECT/'artifacts/dividend-events/archive-runs/skrin-20261015/discovery.json')['groups']['7831000027']
    message = dict(message_id='3807721', agency='3', title='Решения общих собраний участников (акционеров)')
    events, review = parse_message(body, source, message, groups, policy, '7831000027')
    assert {(e['ticker'], e['amount'], e['record_date'], e['decision_at'], e['reporting_period']) for e in events} == {
        ('BSPB', '19.17', '2026-10-05', '2026-09-24', '2026 half year'),
        ('BSPBP', '0.22', '2026-10-05', '2026-09-24', '2026 half year')}
    assert all(e['status'] == 'approved' and e['actual_payment_date'] is None for e in events)
    basis_sources = read_json(ROOT/'basis-sources.json')
    descriptions = {}
    for s in basis_sources:
        raw = (ROOT/'evidence'/s['raw_path']).read_bytes()
        assert digest(raw) == s['sha256']
        data = json.loads(raw)['description']['data']
        d = {r[0]: r[2] for r in data}
        descriptions[d['SECID']] = d
    securities = [s for s in groups if s['ticker'] in {'BSPB', 'BSPBP'}]
    for s in securities:
        d = descriptions[s['ticker']]
        assert (d['ISIN'], d['REGNUMBER'], d['FACEVALUE']) == (s['isin'], s['regnumber'], '1')
    # These registry observations support today's CSV basis, not backdated knowledge.
    split_paths = [PROJECT/'metadata/splits.json', PROJECT.parent/'moexutils/metadata/splits.csv']
    splits = read_json(split_paths[0])
    assert not any(splits.get(t) for t in ('BSPB', 'BSPBP'))
    external = pl.read_csv(split_paths[1])
    assert external.filter(pl.col('ticker').is_in(['BSPB', 'BSPBP'])).is_empty()
    basis = dict(reviewed_at=max(s['observed_at'] for s in basis_sources),
        declared_basis='one original registered share of the stated class; no tax deduction inferred',
        nominal_rub='1', normalization_factor='1',
        rationale='Meeting specifies per-share amounts. Cancellation of treasury shares is not a split. '
                  'Pinned split registries contain no BSPB/BSPBP actions; current MOEX descriptions match both issues.',
        limitation='Current MOEX descriptions and registry review do not establish historical publication of share-basis metadata.',
        split_registry_hashes={p.name: digest(p.read_bytes()) for p in split_paths})
    needed = {source['source_id'], policy['source_id']}
    for s in securities:
        needed.update(json.loads(s['source_ids_json']))
    original_sources = [sources[sid] for sid in sorted(needed)]
    metadata = dict(schema_version=1, scope='BSPB/BSPBP 2026-H1 official CSV supplement',
        parent_package=PARENT.name, source_ids=sorted(needed), basis_review=basis,
        parser_review=review, code_sha256={name: digest((PROJECT/name).read_bytes()) for name in
        ('dividend_events/disclosure.py', 'dividend_events/official.py', 'dividend_events/schema.py')})
    package_id = 'bspb-h1-2026-' + digest(canonical(dict(metadata=metadata, events=events, sources=original_sources+basis_sources)))[:24]
    target = ROOT/'packages'/package_id
    if not target.exists():
        target.mkdir(parents=True)
        for s in original_sources + basis_sources:
            origin = PARENT if s in original_sources else ROOT/'evidence'
            raw = (origin/s['raw_path']).read_bytes()
            assert digest(raw) == s['sha256']
            destination = target/s['raw_path']
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(raw)
        for p in split_paths:
            shutil.copyfile(p, target/('basis-'+p.name))
        pl.DataFrame(events).write_parquet(target/'events.parquet')
        pl.DataFrame(securities).write_parquet(target/'securities.parquet')
        pl.DataFrame(original_sources+basis_sources).write_parquet(target/'sources.parquet')
        write_json(target/'review.json', metadata)
        artifacts = {p.relative_to(target).as_posix(): digest(p.read_bytes()) for p in target.rglob('*') if p.is_file()}
        write_json(target/'manifest.json', dict(package_id=package_id, artifacts=artifacts, **metadata))
        (target/'manifest.sha256').write_text(digest((target/'manifest.json').read_bytes()), encoding='ascii')
    manifest = read_json(target/'manifest.json')
    assert digest((target/'manifest.json').read_bytes()) == (target/'manifest.sha256').read_text()
    for name, sha in manifest['artifacts'].items():
        assert digest((target/name).read_bytes()) == sha
    for event in events:
        validate_event(event)
        validate_archive_evidence(event, {s['source_id']: s for s in original_sources}, target)
    registry_path = PROJECT/'metadata/official_dividends.json'
    registry = read_json(registry_path)
    additions = []
    for e in events:
        sec = next(s for s in securities if s['ticker'] == e['ticker'])
        additions.append(dict(supplement_id=e['ticker']+'-2026-h1-v1', ticker=e['ticker'],
            closing_date=e['record_date'], year=2026, period_type='half year', dividend_value=e['amount'],
            currency='RUB', status='approved', isin=sec['isin'], regnumber=sec['regnumber'],
            amount_basis='original_per_share', basis_verified=True, basis_reviewed_at=basis['reviewed_at'],
            normalization_factor='1', event_id=e['event_id'], source_url=e['source_url'], source_sha256=e['sha256'],
            published_at=e['published_at'], observed_at=e['observed_at'], package_id=package_id,
            manifest_path=(target/'manifest.json').relative_to(PROJECT).as_posix(),
            manifest_sha256=digest((target/'manifest.json').read_bytes())))
    for row in additions:
        prior = [r for r in registry['entries'] if r['supplement_id'] == row['supplement_id']]
        if prior and prior != [row]:
            raise ValueError('Existing supplement differs; publish an explicit revision')
        if not prior:
            registry['entries'].append(row)
    write_json(registry_path, registry)
    print(target, flush=True)
    return target


if __name__ == '__main__':
    build()
