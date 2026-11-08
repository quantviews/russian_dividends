"""Finish the archive campaign, retry timeouts once, then run offline acceptance."""
import json
import msvcrt
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback

PROJECT = Path(__file__).resolve().parent.parent
ROOT = PROJECT / 'artifacts/dividend-events'
BASELINE = ROOT / 'packages/dividends-795799d09665fd56ba9efe0c'
STATUS = ROOT / 'finalization-status.json'


def status(stage, **extra):
    payload = dict(stage=stage, updated_at=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), **extra)
    temp = STATUS.with_suffix('.json.tmp')
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    temp.replace(STATUS)
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def acquire(path):
    handle = path.open('a+b')
    if handle.tell() == 0:
        handle.write(b'0')
        handle.flush()
    handle.seek(0)
    try:
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
    except OSError:
        handle.close()
        return None
    return handle


def main():
    os.chdir(PROJECT)
    own_lock = acquire(ROOT/'acceptance-finalization.lock')
    if own_lock is None:
        raise RuntimeError('Another finalization is already running')
    status('waiting_for_collectors', acceptance_complete=False)
    while True:
        handles = []
        for provider in ('skrin', 'prime'):
            handle = acquire(ROOT/(provider+'.collection.lock'))
            if handle is None:
                break
            handles.append(handle)
        if len(handles) == 2:
            break
        for handle in handles:
            handle.close()
        progress = {}
        for provider in ('skrin', 'prime'):
            path = ROOT/'archive-runs'/(provider+'-20261015')/'document-progress.json'
            if path.exists():
                progress[provider] = json.loads(path.read_text(encoding='utf-8'))
        status('waiting_for_collectors', acceptance_complete=False,
            process_id=os.getpid(), collection_progress=progress)
        time.sleep(30)

    # Import only after collection stops: use the final parser on every raw document.
    from dividend_events.archive import build_archive_package
    from dividend_events.audit import audit
    from dividend_events.storage import read_json, write_json, digest
    from dividend_events.package import validate_package

    code_before = {p.name:digest(p.read_bytes()) for p in (PROJECT/'dividend_events').glob('*.py')}
    runs = [ROOT/'archive-runs'/name for name in ('skrin-20261015', 'prime-20261015')]
    from dividend_events.acceptance import audit_collection, retry_timeouts
    collection = {directory.name: audit_collection(directory) for directory in runs}
    status('retrying_timeouts', collection=collection, acceptance_complete=False)
    from dividend_events.storage import EvidenceStore
    retry_store = EvidenceStore(ROOT/'evidence', delay=2, timeout=60, retries=1)
    retries = {directory.name: retry_timeouts(directory, retry_store) for directory in runs}
    collection = {directory.name: audit_collection(directory) for directory in runs}
    write_json(ROOT/'timeout-retry-summary.json', retries)

    status('testing', collection=collection, acceptance_complete=False)
    test_root = PROJECT/'artifacts'/('pytest-acceptance-'+str(time.time_ns()))
    subprocess.run([sys.executable, '-m', 'pytest', '-q', '--basetemp', str(test_root), '--tb=short'], check=True)
    status('building_skrin', acceptance_complete=False)
    skrin = build_archive_package(BASELINE, ROOT, runs[0])
    status('building_combined', skrin_package=str(skrin), acceptance_complete=False)
    combined = build_archive_package(skrin, ROOT, runs[1])
    status('checking_idempotence', package=str(combined), acceptance_complete=False)
    if build_archive_package(BASELINE, ROOT, runs[0]) != skrin:
        raise ValueError('SKRIN rebuild is not idempotent')
    if build_archive_package(skrin, ROOT, runs[1]) != combined:
        raise ValueError('Combined rebuild is not idempotent')
    output = ROOT/'audits'/combined.name
    status('auditing', package=str(combined), acceptance_complete=False)
    proof = audit(combined, BASELINE, output)
    if audit(combined, BASELINE, output) != proof:
        raise ValueError('Acceptance audit is not repeatable')
    if code_before != {p.name:digest(p.read_bytes()) for p in (PROJECT/'dividend_events').glob('*.py')}:
        raise ValueError('Code changed during finalization; repeat with a stable parser')
    manifest = read_json(combined/'manifest.json')
    summary = read_json(combined/'summary.json')
    validation = validate_package(combined)
    if validation['rows']['coverage'] != 1410 or len(proof['csv_compatibility']) != 468:
        raise ValueError('Pinned universe or legacy CSV scope changed')
    report = PROJECT/'docs/dividend-events-final-acceptance.md'
    report.write_text(
        '# Приёмка архива дивидендных событий\n\n'
        'Полный проход по закреплённому реестру завершён. Приёмочные проверки пройдены; '
        'полнота исторических данных не заявляется (`history_complete=false`).\n\n'
        f'- Пакет: `{combined.as_posix()}`.\n'
        f'- Manifest SHA-256: `{digest((combined/"manifest.json").read_bytes())}`.\n'
        f'- События: {summary["rows"]["events"]}; с доказанной публичной доступностью: {summary["strict_public_events"]}.\n'
        f'- Выпуски / coverage: {summary["rows"]["securities"]} / {summary["rows"]["coverage"]}.\n'
        f'- Источники: {summary["rows"]["sources"]}; конфликты: {summary["rows"]["conflicts"]}.\n'
        f'- Аудит: `{(output/"acceptance-evidence.json").as_posix()}`.\n\n'
        'Выполнены автоматические тесты, повторные сборки обоих архивов с теми же ID, '
        'два одинаковых причинных аудита публикаций 2023–2026 годов и поздней поправки. '
        'Прежние CSV совпадают с исходным пакетом либо содержат только проверенные официальные '
        'дополнения, явно отмеченные в csv_compatibility; raw не изменяется.\n\n'
        '## Проход источников\n\n```json\n'+json.dumps(collection,ensure_ascii=False,indent=2)+'\n```\n\n'
        '## Ограничения\n\n'
        'СКРИН ограничивает поисковую выдачу скользящим годом. Недоступные источники, '
        'неоднозначные выпуски, неразобранные формулировки и несвязанные эпизоды '
        'сохранены в coverage и archive_collection_report. Неизвестная база акции '
        'не нормализуется; сверка CSV не превращает совпадение чисел в подтверждение. '
        'Правила регулируемых архивов подтверждают редакцию в рамках этих правил, '
        'но не являются независимым криптографическим timestamp.\n\n'
        '[Схема, API и воспроизведение](dividend-events.md). '
        '[Общий архив и облигационные события](issuer-events.md) подключены к moex-analytics.\n',
        encoding='utf-8')
    handoff = dict(schema_version=manifest['schema_version'], package_id=combined.name,
        package_path=combined.as_posix(), manifest_sha256=digest((combined/'manifest.json').read_bytes()),
        snapshot_at=manifest['snapshot_at'], requested_from=manifest['requested_from'], requested_to=manifest['requested_to'],
        securities=summary['rows']['securities'], events=summary['rows']['events'],
        strict_public_events=summary['strict_public_events'], history_complete=False,
        acceptance_complete=True, collection_status='completed_with_explicit_gaps', integration_ready=True,
        audit=(output/'acceptance-evidence.json').as_posix(), report=report.relative_to(PROJECT).as_posix())
    write_json(PROJECT/'metadata/dividend_events_handoff.json', handoff)
    readme = PROJECT/'README.md'
    readme_text = readme.read_text(encoding='utf-8')
    final_link = '\nФинальная проверка полного прохода: [приёмка и ограничения](docs/dividend-events-final-acceptance.md).\n'
    if 'docs/dividend-events-final-acceptance.md' not in readme_text:
        readme.write_text(readme_text+final_link, encoding='utf-8')
    status('building_shared_archive', acceptance_complete=True, package=str(combined))
    from issuer_events.archive import build as build_shared
    from issuer_events.audit import audit as audit_shared
    credit_run = ROOT/'issuer-runs/prime-20261015-credit'
    shared = build_shared(ROOT, [combined], [credit_run])
    if build_shared(ROOT, [combined], [credit_run]) != shared:
        raise ValueError('Shared archive rebuild is not idempotent')
    shared_audit = ROOT/'issuer-audits'/(shared.name+'.json')
    shared_proof = audit_shared(shared, shared_audit)
    if audit_shared(shared, shared_audit) != shared_proof:
        raise ValueError('Shared archive audit is not repeatable')
    shared_summary = read_json(shared/'summary.json')
    shared_handoff = read_json(PROJECT/'metadata/issuer_events_handoff.json')
    shared_handoff.update(package_id=shared.name, package_path=shared.as_posix(),
        manifest_sha256=digest((shared/'manifest.json').read_bytes()),
        documents=shared_summary['documents'], bond_event_facts=shared_summary['bond_events'],
        linked_executions=shared_summary['linked_executions'],
        audit=shared_audit.relative_to(PROJECT).as_posix(),
        consumer_refresh_required=True)
    shared_handoff.update(shared_summary['event_types'])
    write_json(PROJECT/'metadata/issuer_events_handoff.json', shared_handoff)
    status('complete', acceptance_complete=True, package=str(combined), report=str(report),
        shared_package=str(shared), consumer_refresh_required=True, collection=collection)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        status('failed', acceptance_complete=False, error=str(exc))
        traceback.print_exc()
        raise
