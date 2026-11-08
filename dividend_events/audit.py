"""Reproducible offline acceptance evidence for a pinned delivered package."""
import argparse
from datetime import timedelta
from pathlib import Path
from urllib.parse import unquote, urlsplit
import polars as pl
from .model import timestamp
from .package import validate_package
from .reader import read_many_as_of, select_as_of
from .storage import digest, read_json, write_json


def audit(package, baseline, output):
    package, baseline, output = Path(package), Path(baseline), Path(output)
    validation = validate_package(package)
    validate_package(baseline)
    events = pl.read_parquet(package / "events.parquet").to_dicts()
    examples = []
    for year in (2023, 2024, 2025, 2026):
        eligible = [e for e in events if e.get("public_available_at") and e["published_at"].startswith(str(year))
                    and e["event_type"] == "recommendation" and e["verification_status"] == "verified"]
        if not eligible:
            raise ValueError(f"No real verified recommendation example in {year}")
        chosen = sorted(eligible, key=lambda e:(e["ticker"] != "SBER",e["public_available_at"],e["event_id"]))[0]
        boundary = timestamp(chosen["public_available_at"])
        examples.append(dict(year=year,event_id=chosen["event_id"],ticker=chosen["ticker"],amount=chosen["amount"],
            source_url=chosen["source_url"],source_id=chosen["source_id"],sha256=chosen["sha256"],raw_path=chosen["raw_path"],
            published_at=chosen["published_at"],publication_precision=chosen["publication_precision"],
            observed_at=chosen["observed_at"],before=(boundary-timedelta(microseconds=1)).isoformat(),after=boundary.isoformat(),
            edition_evidence=chosen["edition_evidence"]))
    requests=[]
    for e in examples:
        requests.extend([dict(at=e["before"],state=False),dict(at=e["after"],state=False),
                         dict(at=e["after"],mode="observed_as_of",state=False)])
    frames=read_many_as_of(package,requests)
    amendments=sorted([e for e in events if e['event_type']=='amendment' and e.get('supersedes_id') and e.get('public_available_at')],key=lambda e:e['public_available_at'])
    if not amendments:
        raise ValueError('No real linked archive correction example')
    correction=amendments[0]
    cutoff=timestamp(correction['public_available_at'])
    before=select_as_of(events,cutoff-timedelta(microseconds=1),'public_as_of')
    earlier_events=[e for e in events if e['event_id']!=correction['event_id']]
    earlier=select_as_of(earlier_events,cutoff-timedelta(microseconds=1),'public_as_of')
    after=select_as_of(events,cutoff,'public_as_of')
    if (not before.equals(earlier) or correction['supersedes_id'] not in before['event_id'].to_list()
        or correction['event_id'] not in after['event_id'].to_list() or correction['supersedes_id'] in after['event_id'].to_list()):
        raise ValueError('Late archive correction altered the early answer')
    correction_example=dict(event_id=correction['event_id'],supersedes_id=correction['supersedes_id'],source_url=correction['source_url'],
        published_at=correction['published_at'],change_type=correction['change_type'],early_answer_unchanged=True)
    output.mkdir(parents=True,exist_ok=True)
    for index,e in enumerate(examples):
        before,after,observed=frames[index*3:index*3+3]
        eid=e["event_id"]
        e.update(before_includes_event=eid in before["event_id"].to_list(),
            after_includes_event=eid in after["event_id"].to_list(),
            observed_at_publication_includes_event=eid in observed["event_id"].to_list())
        if e["before_includes_event"] or not e["after_includes_event"] or e["observed_at_publication_includes_event"]:
            raise ValueError("Real archive causality check failed")
        for label,frame in (("before",before),("after",after),("observed",observed)):
            path=output / f"public-example-{e['year']}-{label}.parquet"
            if path.exists() and not pl.read_parquet(path).equals(frame):
                raise FileExistsError("Refusing to replace another audit export")
            if not path.exists():frame.write_parquet(path)
    compatibility=[]
    for s in pl.read_parquet(baseline / "sources.parquet").to_dicts():
        if s["source_kind"]!="legacy_csv":continue
        value=unquote(urlsplit(s["source_url"]).path)
        if len(value)>2 and value[0]=='/' and value[2]==':':value=value[1:]
        path=Path(value)
        same=path.is_file() and digest(path.read_bytes())==s["sha256"]
        official_extension = False
        if not same and path.is_file() and path.parent.name == "data":
            from official_dividends import verified_csv_extension
            official_extension = verified_csv_extension((baseline/s["raw_path"]).read_bytes(), path.read_bytes(), path.stem)
        compatibility.append(dict(path=str(path),sha256=s["sha256"],unchanged=same,
            official_extension_verified=official_extension))
    if not compatibility or not all(r["unchanged"] or r["official_extension_verified"] for r in compatibility):
        raise ValueError("Legacy CSV differs from pinned baseline")
    summary=read_json(package/'summary.json')
    snapshot_at = read_json(package/'manifest.json')['snapshot_at']
    snapshot = select_as_of(events, timestamp(snapshot_at), 'public_as_of')
    snapshot_ids = set(snapshot['event_id'].to_list())
    later = [e for e in events if e.get('public_available_at') and timestamp(e['public_available_at']) > timestamp(snapshot_at)]
    unproven = [e for e in events if not e.get('public_available_at')]
    if any(e['event_id'] in snapshot_ids for e in later + unproven):
        raise ValueError('Snapshot contains a future or historically unproven event')
    report=dict(package=str(package.resolve()),validation=validation,examples=examples,
        csv_compatibility=compatibility,summary=summary,late_correction=correction_example,
        requested_snapshot=dict(at=snapshot_at,events=snapshot.height,later_events_excluded=len(later),
            historically_unproven_excluded=len(unproven)),
        exports={p.name:digest(p.read_bytes()) for p in sorted(output.glob("*.parquet"))},
        evidence_policy="Regulated archive publication/edition rules; not an independent cryptographic timestamp")
    target=output/'acceptance-evidence.json'
    if target.exists() and read_json(target)!=report:
        raise FileExistsError("Refusing to replace another acceptance audit")
    write_json(target,report)
    print(f"Verified {len(examples)} real year examples and {len(compatibility)} unchanged CSV files: {target}")
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--package',required=True)
    parser.add_argument('--baseline',required=True)
    parser.add_argument('--output',required=True)
    args=parser.parse_args()
    audit(args.package,args.baseline,args.output)


if __name__=='__main__':main()
