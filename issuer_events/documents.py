"""Instrument-neutral editions with replayable regulated-archive evidence."""
import json
from dividend_events import disclosure, prime
from dividend_events.model import public_boundary
from dividend_events.storage import identifier


def document(body, source, message, policy, expected_inn):
    provider = message.get('provider', 'skrin')
    record = (prime if provider == 'prime' else disclosure).extract_message(body)
    if record['inn'] != expected_inn:
        raise ValueError('Disclosure issuer INN mismatch')
    precision = record.get('precision', 'second')
    result = dict(source, document_key=identifier('document', [provider, message['agency'], message['message_id']]),
        edition_id=identifier('edition', [provider, message['agency'], message['message_id'], record['semantic_sha256']]),
        provider=provider, issuer_inn=expected_inn, title=record['metadata'].get('Тип события', message['title']),
        published_at=record['published_at'], publication_precision=precision, publication_timezone='Europe/Moscow',
        original_text=record['text'], evidence=record['text'], historical_availability='proven',
        publication_evidence=json.dumps(dict(kind='regulated_news_archive', provider=provider, source_id=source['source_id'],
            label=record['publication_label'], timezone='Europe/Moscow', agency=message['agency'], message_id=message['message_id'])),
        edition_evidence=json.dumps(dict(kind='regulated_news_archive', provider=provider, policy_source_id=policy['source_id'],
            policy_url=policy['source_url'], semantic_sha256=record['semantic_sha256'], issuer_inn=expected_inn,
            agency=message['agency'], message_id=message['message_id'])))
    result['public_available_at'] = public_boundary(result)
    return result
