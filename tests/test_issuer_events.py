import json
from pathlib import Path
import pytest
from dividend_events.storage import EvidenceStore, write_json
from dividend_events import prime
from issuer_events.documents import document
from issuer_events.bonds import parse, link_executions, valid_isin
from issuer_events.archive import build, validate, read_as_of

ISIN = 'RU000A10E4X3'


def stored(tmp_path, text, title, number=1, published='04.09.2026 19:53'):
    store = EvidenceStore(tmp_path/'evidence', offline=True)
    policy = store.save(' '.join(prime.POLICY_MARKERS).encode('cp1251'),prime.POLICY_URL,'prime_archive_policy')
    message = dict(provider='prime', agency='5', message_id=f'00000000-0000-0000-0000-{number:012d}',title=title)
    body = (f'<div id="ctl00_Main_Pan"><h1 class="news_header">{title}</h1><span>{published}</span>'
        f'<p>1.4. ИНН эмитента: 5029169023. 1.7. Дата наступления события: 03.09.2026. '
        f'2.1. Биржевые облигации ISIN: {ISIN}. {text} 3. Подпись</p></div>').encode('cp1251')
    source = store.save(body, prime.HOST+'/news/-12/{'+message['message_id']+'}.uif','disclosure_message',observed_at='2026-10-15T12:00:00+00:00')
    doc = document(body,source,message,policy,'5029169023')
    return doc, store, dict(inn='5029169023',message=message,source_id=source['source_id'],review={'status':'downloaded'}), policy


def missed(tmp_path):
    return stored(tmp_path, '2.2. Содержание обязательства эмитента: выплата за 7 купонный период 41 100 000 руб. '
        '2.3. Дата, в которую обязательство эмитента должно быть исполнено: 20.08.2026. '
        '2.4. Сведения о неисполнении обязательства: Дефолт. Эмитентом не исполнено обязательство.',
        'Неисполнение обязательств эмитента перед владельцами его ценных бумаг')


def paid(tmp_path, coupon=7, percent=100):
    return stored(tmp_path, f'2.2. Отчетный (купонный) период: выплата за {coupon} купонный период. '
        '2.3. Общий размер выплаченных доходов: 41 100 000 руб. '
        '2.4. Дата, в которую обязанность по выплате доходов должна быть исполнена: 20.08.2026. '
        f'2.5. Объем (в процентах), в котором исполнено обязательство: {percent} (сто) %.',
        'Выплаченные доходы по эмиссионным ценным бумагам эмитента',2,'05.09.2026 12:00')


def test_exact_obligation_execution_preserves_breach_and_unknown_actual_date(tmp_path):
    breach = parse(missed(tmp_path)[0])[0][0]
    execution = parse(paid(tmp_path)[0])[0][0]
    assert breach['amount']=='41100000' and breach['coupon_number']=='7' and breach['due_date']=='2026-08-20'
    rows = link_executions([breach,execution])
    assert rows[1]['follows_event_id']==breach['event_id']
    assert rows[1]['actual_payment_date'] is None and rows[0]['event_type']=='nonperformance'
    other = parse(paid(tmp_path,8)[0])[0][0]
    assert link_executions([breach,other])[1]['follows_event_id'] is None
    partial = parse(paid(tmp_path,7,50)[0])[0][0]
    assert partial['status']=='reported_partially_executed'


def test_unknown_or_ambiguous_identity_and_zero_execution_not_promoted(tmp_path):
    doc = missed(tmp_path)[0]
    assert valid_isin(ISIN) and not valid_isin(ISIN[:-1]+'4')
    assert not parse(dict(doc, original_text=doc['original_text']+' RU000A104H99'))[0]
    assert not parse(paid(tmp_path,7,0)[0])[0]


def test_terms_change_retains_exact_description_without_invented_effective_date(tmp_path):
    doc = stored(tmp_path,'2.2. Краткое содержание зарегистрированных изменений: Срок погашения перенесён. '
        '2.3. Иная информация: подробности в документе.', 'Регистрация изменений в решение о выпуске ценных бумаг')[0]
    event = parse(doc)[0][0]
    assert event['event_type']=='terms_change' and event['evidence']=='Срок погашения перенесён.'
    assert event['due_date'] is None and event['amount'] is None


def test_delay_and_subsequent_execution_disclosed_together(tmp_path):
    doc=missed(tmp_path)[0]
    text=doc['original_text'].replace('Дефолт. Эмитентом не исполнено обязательство.',
        'Просрочка исполнения обязательства на срок менее 10 рабочих дней. Обязательство исполнено 21.08.2026.')
    text=text.replace('период 41 100 000 руб.','период, общий размер обязательства в денежном выражении: 41 100 000 руб.')
    events,_=parse(dict(doc,original_text=text))
    assert len(events)==2
    assert events[1]['follows_event_id']==events[0]['event_id']
    assert events[1]['actual_payment_date']=='2026-08-21'
    assert events[1]['public_available_at']==events[0]['public_available_at']
    assert link_executions(events)[1]['follows_event_id']==events[0]['event_id']
    mixed=text.replace('41 100 000 руб.', '41 100 000 руб. и частичное погашение номинальной стоимости 10 000 руб.')
    assert parse(dict(doc,original_text=mixed))[0][0]['obligation_id'] is None


def test_shared_archive_replays_raw_is_idempotent_and_time_safe(tmp_path):
    _,store,row,policy=missed(tmp_path)
    _,_,second,_=paid(tmp_path)
    run=tmp_path/'run'
    write_json(run/'discovery.json',dict(policy=policy))
    write_json(run/'selection.json',[row,second])
    write_json(run/'messages/a.json',row)
    write_json(run/'messages/b.json',second)
    package=build(tmp_path,runs=[run])
    assert build(tmp_path,runs=[run])==package
    assert not read_as_of(package,'2026-09-04T16:53:59+00:00')
    assert len(read_as_of(package,'2026-09-04T16:54:00+00:00'))==1
    assert not read_as_of(package,'2026-09-06T00:00:00+00:00','observed_as_of')
    assert len(read_as_of(package,'2026-10-16T00:00:00+00:00','observed_as_of'))==2
    source=store.sources()[0]
    (store.root/source['raw_path']).write_bytes(b'tampered')
    with pytest.raises(ValueError,match='Raw evidence hash'):validate(package)
