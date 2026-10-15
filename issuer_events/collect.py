"""Prioritize credit disclosures from the existing public issuer search index."""
import argparse
from pathlib import Path
import re
from dividend_events.archive import lock_provider
from dividend_events.storage import EvidenceStore, read_json, write_json


def credit_candidate(title):
    return bool(re.search(r'неисполнени|исполнени.*обязательств|изменени.*(?:решени.*выпуск|услови)|реструктур|дефолт', title, re.I))


def collect(root, run):
    root, run = Path(root), Path(run)
    discovery = read_json(run/'discovery.json')
    provider = read_json(run/'settings.json').get('provider', 'skrin')
    lock_provider(root, provider)
    store = EvidenceStore(root/'evidence', delay=2, timeout=25, retries=1)
    target = root/'issuer-runs'/(run.name+'-credit')
    write_json(target/'discovery.json', discovery)
    selected = []
    for path in sorted((run/'issuers').glob('*.json')):
        issuer = read_json(path)
        for message in issuer['messages']:
            if credit_candidate(message['title']):
                selected.append(dict(inn=issuer['inn'], message=message, index_source_ids=issuer['source_ids']))
    write_json(target/'selection.json', selected)
    for index, row in enumerate(selected, 1):
        message = row['message']
        checkpoint = target/'messages'/(message['agency']+'-'+message['message_id']+'.json')
        if checkpoint.exists() and read_json(checkpoint).get('source_id'):
            continue
        try:
            _, source = store.fetch(message['url'], 'disclosure_message')
            row.update(source_id=source['source_id'], review=dict(status='downloaded'))
        except Exception as exc:
            row.update(source_id=None, review=dict(status='source_unavailable', reason=str(exc)))
        write_json(checkpoint, row)
        print(f'credit document {index}/{len(selected)}: {row["review"]["status"]}', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', default='artifacts/dividend-events')
    parser.add_argument('--index-run', required=True)
    args = parser.parse_args()
    collect(args.root, args.index_run)
