"""Immutable projections over the shared content-addressed disclosure store."""
from collections import Counter, defaultdict
import json
from pathlib import Path
import argparse
import polars as pl
from dividend_events.storage import canonical, digest, read_json, write_json
from dividend_events.disclosure import validate_archive_evidence
from dividend_events.model import timestamp, public_boundary
from . import SCHEMA_VERSION, PARSER_VERSION
from .documents import document
from .bonds import parse, link_executions


def rows_frame(rows, empty_columns):
    columns = sorted(set().union(*(r.keys() for r in rows))) if rows else empty_columns
    return pl.DataFrame([{k:r.get(k) for k in columns} for r in rows], schema={k:pl.String for k in columns})


def build(root, dividend_packages=(), runs=()):
    root = Path(root).resolve()
    evidence = root/'evidence'
    checkpoints, coverage = [], []
    for package in map(Path, dividend_packages):
        from dividend_events.package import validate_package
        validate_package(package)
        sources = pl.read_parquet(package/'sources.parquet').to_dicts()
        for source in sources:
            if source['source_kind'] != 'archive_collection_report':continue
            report = read_json(package/source['raw_path'])
            policy = report['discovery']['policy']
            checkpoints.extend((r,policy) for r in report['reviews'])
            coverage.append(dict(kind='dividend_archive_report', source_id=source['source_id'],
                package=str(package.resolve()), issuers=len(report.get('issuers', [])),
                scope='equity_issuer_universe_only; not a complete bond universe'))
    for run in map(Path, runs):
        discovery = read_json(run/'discovery.json')
        checkpoints.extend((read_json(p),discovery['policy']) for p in sorted((run/'messages').glob('*.json')))
        coverage.append(dict(kind='credit_priority_run', run=str(run.resolve()),
            selected=len(read_json(run/'selection.json')), scope='existing issuer search index; explicit title selection'))
    sources, docs, reviews = {}, {}, []
    for checkpoint, policy in checkpoints:
        sid = checkpoint.get('source_id')
        if not sid:
            reviews.append(dict(source_id=None, status='unavailable_or_not_downloaded', reason=checkpoint['review'].get('reason')))
            continue
        source = read_json(evidence/'receipts'/(sid.split(':')[1]+'.json'))
        sources[sid] = source
        sources[policy['source_id']] = policy
        try:
            doc = document((evidence/source['raw_path']).read_bytes(), source, checkpoint['message'], policy, checkpoint['inn'])
        except (ValueError, KeyError, AttributeError) as exc:
            reviews.append(dict(source_id=sid, status='needs_review', reason=str(exc)))
            continue
        old = docs.get(doc['edition_id'])
        if old is None or timestamp(doc['observed_at']) < timestamp(old['observed_at']):
            docs[doc['edition_id']] = doc
    by_key = defaultdict(list)
    for doc in docs.values():by_key[doc['document_key']].append(doc)
    for editions in by_key.values():
        for changed in sorted(editions, key=lambda d:timestamp(d['observed_at']))[1:]:
            changed.update(historical_availability='unproven', public_available_at=None)
    events = []
    for doc in docs.values():
        found, review = parse(doc)
        events.extend(found)
        reviews.append(review)
    events = link_executions(events)
    # Exact duplicate checkpoints are harmless; deterministic package and review identities.
    tables = dict(documents=sorted(docs.values(), key=lambda r:r['edition_id']),
        bond_events=sorted(events,key=lambda r:r['event_id']), sources=sorted(sources.values(),key=lambda r:r['source_id']),
        reviews=[json.loads(v) for v in sorted({canonical(r).decode() for r in reviews})])
    code = {p.name:digest(p.read_bytes()) for p in Path(__file__).parent.glob('*.py')}
    import dividend_events
    code.update({'dividend_events/'+p.name:digest(p.read_bytes()) for p in Path(dividend_events.__file__).parent.glob('*.py')})
    metadata = dict(schema_version=SCHEMA_VERSION, parser_version=PARSER_VERSION, evidence_root=str(evidence),
        coverage=coverage, code_sha256=code, history_complete=False)
    package_id = 'issuer-events-'+digest(canonical(dict(tables=tables, metadata=metadata)))[:24]
    target = root/'issuer-packages'/package_id
    if (target/'manifest.json').exists():
        validate(target)
        return target
    target.mkdir(parents=True, exist_ok=True)
    for name, rows in tables.items():
        rows_frame(rows, ['event_id'] if name=='bond_events' else ['source_id']).write_parquet(target/(name+'.parquet'))
    summary = dict(documents=len(docs), bond_events=len(events), event_types=dict(Counter(e['event_type'] for e in events)),
        linked_executions=sum(bool(e['follows_event_id']) for e in events), reviews=dict(Counter(r['status'] for r in reviews)),
        history_complete=False, coverage=coverage)
    write_json(target/'summary.json', summary)
    manifest = dict(metadata, package_id=package_id, artifacts={p.name:digest(p.read_bytes()) for p in target.iterdir() if p.is_file()})
    write_json(target/'manifest.json', manifest)
    (target/'manifest.sha256').write_text(digest((target/'manifest.json').read_bytes()), encoding='ascii')
    validate(target)
    return target


def validate(package):
    package = Path(package).resolve()
    manifest = read_json(package/'manifest.json')
    if manifest['schema_version'] != SCHEMA_VERSION or digest((package/'manifest.json').read_bytes()) != (package/'manifest.sha256').read_text().strip():
        raise ValueError('Invalid issuer archive manifest')
    for name, sha in manifest['artifacts'].items():
        path = (package/name).resolve()
        if not path.is_relative_to(package) or digest(path.read_bytes()) != sha:raise ValueError('Archive artifact hash mismatch')
    evidence = Path(manifest['evidence_root']).resolve()
    sources = {s['source_id']:s for s in pl.read_parquet(package/'sources.parquet').to_dicts()}
    for source in sources.values():
        path = (evidence/source['raw_path']).resolve()
        if not path.is_relative_to(evidence) or digest(path.read_bytes()) != source['sha256']:raise ValueError('Raw evidence hash mismatch')
    docs = pl.read_parquet(package/'documents.parquet').to_dicts()
    cache, replay = {}, []
    for doc in docs:
        source = sources[doc['source_id']]
        if any(doc[k] != source[k] for k in ('source_url','raw_path','sha256','observed_at')):raise ValueError('Document/source mismatch')
        validate_archive_evidence(doc, sources, evidence, cache)
        if public_boundary(doc) != doc['public_available_at']:raise ValueError('Incorrect public boundary')
        replay.extend(parse(doc)[0])
    actual = pl.read_parquet(package/'bond_events.parquet').to_dicts()
    expected = sorted(link_executions(replay),key=lambda e:e['event_id'])
    if actual != expected:raise ValueError('Bond facts differ from replayed evidence')
    if len({e['event_id'] for e in actual}) != len(actual):raise ValueError('Duplicate bond events')
    return manifest


def read_as_of(package, at, mode='public_as_of'):
    validate(package)
    if mode not in ('public_as_of','observed_as_of'):raise ValueError('Unknown information mode')
    cutoff = timestamp(at)
    key = 'public_available_at' if mode=='public_as_of' else 'observed_at'
    rows = pl.read_parquet(Path(package)/'bond_events.parquet').to_dicts()
    selected = [e for e in rows if e.get(key) and timestamp(e[key])<=cutoff
        and timestamp(e.get('public_available_at') or e['published_at'])<=cutoff]
    # A later payment cannot expose a parent/follow-up link before the parent is available.
    ids = {e['event_id'] for e in selected}
    return [dict(e, follows_event_id=e['follows_event_id'] if e['follows_event_id'] in ids else None) for e in selected]


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', default='artifacts/dividend-events')
    p.add_argument('--dividend-package', action='append', default=[])
    p.add_argument('--run', action='append', default=[])
    args = p.parse_args()
    result = build(args.root,args.dividend_package,args.run)
    print(result, flush=True)
    print(json.dumps(read_json(result/'summary.json'), ensure_ascii=False), flush=True)
