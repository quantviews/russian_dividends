"""Offline acceptance examples from real bond disclosure editions."""
import argparse
from datetime import timedelta
from pathlib import Path
import polars as pl
from dividend_events.model import timestamp
from dividend_events.storage import read_json, write_json, digest
from .archive import validate


def audit(package, output):
    package, output = Path(package), Path(output)
    manifest = validate(package)
    events = pl.read_parquet(package/'bond_events.parquet').to_dicts()
    examples = []
    for kind in ('nonperformance','execution','terms_change'):
        eligible = [e for e in events if e['event_type']==kind and e['public_available_at']
            and (kind!='execution' or e['follows_event_id'])]
        if not eligible:raise ValueError('No real archived example: '+kind)
        event = sorted(eligible,key=lambda e:e['public_available_at'])[0]
        boundary = timestamp(event['public_available_at'])
        before = [e['event_id'] for e in events if e['public_available_at'] and timestamp(e['public_available_at'])<boundary]
        after = [e['event_id'] for e in events if e['public_available_at'] and timestamp(e['public_available_at'])<=boundary]
        observed = [e['event_id'] for e in events if timestamp(e['observed_at'])<=boundary]
        if event['event_id'] in before or event['event_id'] not in after or event['event_id'] in observed:
            raise ValueError('Archive publication/observation causality failed')
        if kind=='execution':
            parent = next(e for e in events if e['event_id']==event['follows_event_id'])
            if parent['obligation_id']!=event['obligation_id'] or parent['event_id'] not in after:
                raise ValueError('Execution did not preserve its exact preceding obligation')
        examples.append(dict(event_id=event['event_id'],event_type=kind,isin=event['isin'],
            source_url=event['source_url'],sha256=event['sha256'],evidence=event['evidence'],
            before=(boundary-timedelta(microseconds=1)).isoformat(),after=boundary.isoformat(),
            observed_at=event['observed_at'],follows_event_id=event['follows_event_id'],
            actual_payment_date=event['actual_payment_date']))
    result=dict(package_id=manifest['package_id'],manifest_sha256=digest((package/'manifest.json').read_bytes()),
        examples=examples,history_complete=False,scope='real source facts; no investor cash receipt or legal debt closure inferred')
    if output.exists() and read_json(output)!=result:raise ValueError('Refusing to replace another audit')
    write_json(output,result)
    print(output,flush=True)
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--package',required=True)
    parser.add_argument('--output',required=True)
    args=parser.parse_args()
    audit(args.package,args.output)
