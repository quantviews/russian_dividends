"""Archive causality, identity, parser ambiguity and pagination regression checks."""
import json
from pathlib import Path
import pytest
from dividend_events import prime
from dividend_events.disclosure import parse_message, parse_correction, POLICY_MARKERS, search_page
from dividend_events.archive import collect_index
from dividend_events.storage import EvidenceStore, read_json
from dividend_events.model import merge_revisions


def security(kind="ordinary"):
    return dict(security_id="security:"+kind,issuer_id="issuer:test",ticker=kind,
                share_class=kind,instrument_type="common_share" if kind=="ordinary" else "preferred_share")


def body(text, title="Решения совета директоров (наблюдательного совета)"):
    return (f'<div id="ctl00_Main_Pan"><h1 class="news_header">{title}</h1>'
            '<span>Эмитент<br>17.03.2023 14:35</span><p>1. Общие сведения '
            'Идентификационный номер налогоплательщика (ИНН) эмитента: 7707083893</p>'
            f'<p>{text}</p><span id="ctl00_Main_Disclaimer">Служебный текст</span></div>').encode("cp1251")


def parse(tmp_path, content, securities=None, title=None):
    store=EvidenceStore(tmp_path,offline=True)
    message=dict(provider="prime",agency="5",message_id="00000000-0000-0000-0000-000000000000",title=title or "Решения совета директоров")
    source=store.save(content,prime.HOST+"/news/-12/{"+message["message_id"]+"}.uif","prime_message",observed_at="2026-10-15T00:00:00Z")
    policy=store.save(" ".join(prime.POLICY_MARKERS).encode("cp1251"),prime.POLICY_URL,"prime_archive_policy")
    events, review=prime.parse(content,source,message,securities or [security()],policy,"7707083893")
    return events,review,store


def test_real_wording_recommendation_is_not_quoted_future_approval(tmp_path):
    content=body('Рекомендовать следующий размер дивидендов по результатам 2022 года: '
        'по обыкновенным акциям – 25,00 руб. на одну акцию, по привилегированным акциям – 25,00 руб. на одну акцию. '
        'Вынести на утверждение собрания решение: «Выплатить дивиденды за 2022 год по обыкновенным акциям '
        '25,00 руб. на одну акцию, по привилегированным акциям – 25,00 руб. на одну акцию».')
    events,review,store=parse(tmp_path,content,[security(),security("preferred")])
    assert len(events)==2
    assert {e["event_type"] for e in events}=={"recommendation"}
    assert {e["amount"] for e in events}=={"25.00"}
    assert {e["public_available_at"] for e in events}=={"2023-03-17T11:36:00+00:00"}
    sources={s["source_id"]:s for s in store.sources()}
    for e in events:
        prime.validate_prime_evidence(e,sources,store.root)
    events[0]["published_at"]="2023-03-16T14:35:00+03:00"
    with pytest.raises(ValueError,match="evidence mismatch"):
        prime.validate_prime_evidence(events[0],sources,store.root)


def test_agenda_and_ambiguous_amounts_are_not_promoted(tmp_path):
    events,review,_=parse(tmp_path,body('Рекомендовать дивиденды за 2022 год 10 руб. на одну акцию.',
        'Проведение заседания совета директоров и его повестка дня'))
    assert not events and review["status"]=="needs_review"
    events,review,_=parse(tmp_path,body('Рекомендовать дивиденды за 2022 год 10 руб. на одну акцию. '
        'Дополнительно дивиденды за 2022 год 5 руб. на одну акцию.'))
    assert not events


def test_payment_deadline_does_not_become_actual_payment_date(tmp_path):
    events,review,_=parse(tmp_path,body('Дивиденды по результатам 2022 года 10 рублей на одну обыкновенную акцию. '
        'Дата, в которую обязанность должна быть исполнена: 15.05.2023. '
        'Причины исполнения обязательства не в полном объеме: обязательство исполнено полностью.',
        'Выплаченные доходы по эмиссионным ценным бумагам эмитента'))
    assert not events
    assert "actual_date" in review["reason"]


def test_coupon_template_mentioning_dividends_is_not_a_dividend_message(tmp_path):
    events,review,_=parse(tmp_path,body('Категория выплат по ценным бумагам (дивиденды по акциям; проценты по облигациям): '
        'проценты (купонный доход) по облигациям. Общий размер выплаченных доходов: 1000 руб.',
        'Выплаченные доходы по эмиссионным ценным бумагам эмитента'))
    assert not events and review['status']=='not_dividend' and review['reason']=='explicit_bond_income_category'


def test_nominal_value_is_not_the_dividend_amount(tmp_path):
    events,review,_=parse(tmp_path,body('Рекомендовать дивиденды по результатам 2022 года в размере 10% от '
        'номинальной стоимости 100 рублей на одну обыкновенную акцию.'))
    assert not events and review['status']=='needs_review'


def test_identity_mismatch_and_unknown_issue_do_not_guess(tmp_path):
    content=body('Рекомендовать дивиденды за 2022 год 10 руб. на одну акцию.')
    events,review,_=parse(tmp_path,content.replace(b'7707083893',b'7708004767'))
    assert not events and review["status"]=="identity_mismatch"
    other=dict(security(),security_id="security:second_issue")
    events,review,_=parse(tmp_path,content,[security(),other])
    assert not events and "ambiguous_issue_identity" in review["reason"]


def test_numbered_income_form_preserves_terms_separately_from_payment(tmp_path):
    content = body('2.1. Идентификационные признаки ценных бумаг: обыкновенные акции. '
        '2.5. Отчетный (купонный) период (год; 3, 6, 9 месяцев года), за который начислены доходы: 9 месяцев 2025 года; '
        '2.6. Общий размер начисленных (подлежащих выплате) доходов: 180 072 690,00 рублей; '
        '2.7. Размер начисленных (подлежащих выплате) доходов в расчете на одну ценную бумагу '
        '(размер объявленного дивиденда в расчете на одну акцию): 1 995,00 рублей; '
        '2.8. Форма выплаты: денежные средства; '
        '2.10. Дата, в которую обязанность по выплате доходов должна быть исполнена: 31.12.2025 года. '
        '3. Подпись', 'О начисленных доходах по эмиссионным ценным бумагам эмитента')
    events, _, _ = parse(tmp_path, content)
    assert {e['event_type'] for e in events} == {'approval', 'payment_terms'}
    assert {e['amount'] for e in events} == {'1995.00'}
    assert {e['reporting_period'] for e in events} == {'2025 9 months'}
    assert all(e['actual_payment_date'] is None for e in events)
    assert next(e for e in events if e['event_type'] == 'payment_terms')['payment_to'] == '2025-12-31'


def test_meeting_participant_register_is_not_dividend_record_date(tmp_path):
    content = body('Рекомендовать дивиденды по результатам 2025 года 10 рублей на одну обыкновенную акцию. '
        'Дата, на которую определяются лица, имеющие право на участие в общем собрании: 22.05.2026.')
    events, _, _ = parse(tmp_path, content)
    assert len(events) == 1 and events[0]['record_date'] is None


def test_rejected_meeting_proposal_is_not_an_approval(tmp_path):
    content = body('Предложено выплатить дивиденды по результатам 2025 года 10 рублей на одну обыкновенную акцию. '
        'Итоги голосования: решение не принято.', 'Решения общего собрания акционеров')
    events, review, _ = parse(tmp_path, content)
    assert not events and review['reason'] == 'unadopted_resolution_requires_question_scope_review'


def test_legal_reference_date_after_record_clause_is_not_record_date(tmp_path):
    content = body('Рекомендовать дивиденды по результатам 2025 года 10 рублей на одну обыкновенную акцию. '
        'Установить 12 декабря 2025 года в качестве даты, на которую определяются лица, '
        'имеющие право на получение дивидендов. Выплатить дивиденды в сроки, установленные '
        'Федеральным законом от 26.12.1995 № 208-ФЗ.')
    events, _, _ = parse(tmp_path, content)
    assert len(events) == 1 and events[0]['record_date'] == '2025-12-12'


def test_payment_deadline_relative_to_record_date_is_not_the_record_date(tmp_path):
    content = body('Рекомендовать дивиденды по результатам 2025 года 10 рублей на одну обыкновенную акцию. '
        'Выплатить другим акционерам в течение 25 рабочих дней с даты, на которую определяются лица, '
        'имеющие право на получение дивидендов — не позднее 23.07.2026 года.')
    events, _, _ = parse(tmp_path, content)
    assert len(events) == 1 and events[0]['record_date'] is None


def test_cumulative_dividend_with_previous_payment_needs_explicit_episode_review(tmp_path):
    content = body('Рекомендовать по итогам первого полугодия 2026 года дивиденды 9 рублей 22 копейки на одну обыкновенную акцию. '
        'С учетом выплаченных дивидендов по итогам первого квартала 2026 года в размере 4 рубля 36 копеек '
        'на одну обыкновенную акцию выплатить 4 рубля 86 копеек на одну обыкновенную акцию.')
    events, review, _ = parse(tmp_path, content)
    assert not events and review['reason'] == 'net_of_prior_distribution_requires_review'


def test_no_additional_distribution_does_not_cancel_previous_annual_dividend(tmp_path):
    annual, _, _ = parse(tmp_path, body('Выплатить дивиденды по результатам 2025 года 2 рубля на одну обыкновенную акцию.',
                                      'Решения общего собрания акционеров'))
    content = body('Решение о выплате дивидендов по итогам 2025 года принято ранее. '
        'Повторно по итогам 2025 года прибыль не распределять, дивиденды не выплачивать.',
        'Решения общего собрания акционеров')
    events, _, _ = parse(tmp_path, content)
    assert len(events) == 1 and events[0]['event_type'] == 'no_dividend'
    assert all(e['record_date'] is None and e['dividend_id'] != annual[0]['dividend_id'] for e in events)


def test_explicit_paid_total_preserves_unknown_date_and_partial_progress(tmp_path):
    events,review,_=parse(tmp_path,body('Дивиденды по результатам 2022 года 10 рублей на одну обыкновенную акцию. '
        'Общий размер выплаченных доходов по ценным бумагам эмитента: 1 000,50 руб. '
        'Объем (в процентах), в котором исполнено обязательство: 50 %. '
        'Дата, в которую обязанность должна быть исполнена: 15.05.2023.',
        'Выплаченные доходы по эмиссионным ценным бумагам эмитента'))
    assert len(events)==1
    assert events[0]['event_type']=='payment'
    assert events[0]['actual_payment_amount']=='1000.50'
    assert events[0]['actual_payment_date'] is None
    assert events[0]['gap_reason']=='actual_payment_date_not_disclosed'
    assert events[0]['partial_payment'] is True


def test_repeated_total_label_does_not_capture_next_per_share_field(tmp_path):
    content = body('Дивиденды по результатам 2025 года. '
        '2.4. Общий размер выплаченных доходов по ценным бумагам эмитента: '
        'общий размер выплаченных доходов по обыкновенным акциям Общества — 59 356 446,44 руб. '
        '2.5. Размер выплаченных доходов в расчете на одну ценную бумагу эмитента: '
        '0,0156 рублей на одну обыкновенную акцию. '
        '2.10. Объем (в процентах), в котором исполнено обязательство: 99,6 (девяносто девять целых шесть десятых) %. '
        '3. Подпись', 'Выплаченные доходы по эмиссионным ценным бумагам эмитента')
    events, _, _ = parse(tmp_path, content)
    assert len(events) == 1
    assert events[0]['amount'] == '0.0156'
    assert events[0]['actual_payment_amount'] == '59356446.44'
    assert events[0]['partial_payment'] is True
    changed, _, _ = parse(tmp_path, content.replace(b'59 356 446,44', b'59 356 447,44'))
    changed[0]['observed_at'] = '2026-10-16T00:00:00Z'
    revision = merge_revisions(events, changed)[-1]
    from dividend_events.model import validate_event
    validate_event(revision)
    assert revision['public_available_at'] is None
    assert 'actual_payment_date_not_disclosed' in revision['gap_reason']


def test_prime_postback_zero_count_does_not_truncate_after_two_pages(tmp_path,monkeypatch):
    def result(page):
        start=page*30
        rows=''.join(f'<tr><td>01.06.2023 10:00:00</td><td><a href="/Portal/Default.aspx?emid=7707083893">issuer</a></td>'
            f'<td><a href="-12/{{00000000-0000-0000-0000-{i:012d}}}.uif">Решения совета директоров</a></td></tr>' for i in range(start,min(start+30,65)))
        return (f'<input name="{prime.PREFIX}SPropertyValue" value="7707083893">Найдено сообщений: {65 if page==0 else 0}<table>{rows}</table>').encode('cp1251')
    class Store:
        calls=[]
        def fetch(self,url,kind,form):
            page=int(url.split('?p=')[-1]) if '?p=' in url else 0
            self.calls.append(page)
            return result(page),dict(source_id=f'source:{page}')
    store=Store()
    collect_index(store,tmp_path,{"7707083893":[security()]},{},"2023-01-01","2023-12-31","prime")
    checkpoint=read_json(tmp_path/'issuers/7707083893.json')
    assert store.calls==[0,1,2]
    assert checkpoint['status']=='indexed' and len(checkpoint['messages'])==65


def test_skrin_ignored_issuer_filter_rejected():
    with pytest.raises(ValueError,match="filter"):
        search_page(b'<input name="ev_kod_val" value="">',"7707083893")


def test_split_registry_conventions_do_not_create_false_conflicts():
    from dividend_events.package import detect_conflicts
    actions=[dict(ticker='VTBR',security_id='security:VTBR',action_date='2024-07-15',kind='reverse',ratio='5000',source_id='a'),
             dict(ticker='VTBR',security_id='security:VTBR',action_date='2024-07-15',kind='auto',ratio='0.0002',source_id='b')]
    assert not detect_conflicts([],actions)
    actions[0]['action_date']='2024-07-11'
    assert len(detect_conflicts([],actions))==1


def test_equivalent_reporting_periods_use_the_same_episode_key():
    from dividend_events.disclosure import find_period
    assert find_period('дивиденды за 6 месяцев 2025 года') == find_period('дивиденды по итогам первого полугодия 2025 года') == '2025 half year'
    assert find_period('дивиденды за I квартал 2025 года') == '2025 3 months'
    assert find_period('по результатам 9 (девяти) месяцев 2025 отчетного года') == '2025 9 months'


def test_same_profit_year_different_meetings_are_separate_distributions(tmp_path):
    def recommendation(day, amount):
        return body(f'Рекомендовать общему собранию акционеров ПАО Акрон {day} принять решение: '
                    f'Выплатить дивиденды по результатам 2021 года {amount} рублей на одну обыкновенную акцию.')
    july, _, _ = parse(tmp_path, recommendation('29 июля 2026 года', '235'))
    october, _, _ = parse(tmp_path, recommendation('22 октября 2026 года', '146'))
    assert july[0]['dividend_id'] != october[0]['dividend_id']
    approved, _, _ = parse(tmp_path, body('2.2. Дата проведения общего собрания: 29.07.2026. '
        '2.3. Решение: Выплатить дивиденды по результатам 2021 года 235 рублей на одну обыкновенную акцию.',
        'Решения общего собрания акционеров'))
    assert july[0]['dividend_id'] == approved[0]['dividend_id']
    october[0]['observed_at'] = '2026-10-16T00:00:00Z'
    # If the SAME supposedly immutable message ID changes its meeting date, it loses backdating.
    revised = merge_revisions(july, october)[-1]
    assert revised['public_available_at'] is None
    assert revised['supersedes_id'] == july[0]['event_id']


def test_dynamic_menu_is_not_a_revision_but_changed_archived_text_loses_backdating(tmp_path):
    content=body('Рекомендовать дивиденды за 2022 год 10 руб. на одну акцию.')
    first,_,_=parse(tmp_path,content)
    menu,_,_=parse(tmp_path,content+b'<footer>new menu</footer>')
    assert first[0]['event_id']==menu[0]['event_id']
    assert len(merge_revisions(first,menu))==1
    changed,_,_=parse(tmp_path,content.replace(b'10 ',b'12 '))
    changed[0]['observed_at']='2026-10-16T00:00:00Z'
    rows=merge_revisions(first,changed)
    assert len(rows)==2
    revision=rows[-1]
    assert revision['supersedes_id']==first[0]['event_id']
    assert revision['historical_availability']=='unproven'
    assert revision['public_available_at'] is None
    assert revision['gap_reason']=='archive_immutability_violation'


def test_explicit_parent_correction_keeps_original_and_separates_share_classes(tmp_path):
    content=body('Выплатить дивиденды по результатам 2025 года: 2 (два) рубля 85 копеек на одну обыкновенную акцию; '
        '0 (ноль) рублей 25 копеек на одну привилегированную акцию.', 'Решения общего собрания акционеров')
    securities=[security(),security('preferred')]
    parents,_,store=parse(tmp_path,content,securities)
    assert [(e['ticker'],e['amount']) for e in parents]==[('ordinary','2.85'),('preferred','0.25')]
    proof=json.loads(parents[0]['edition_evidence'])
    policy=next(s for s in store.sources() if s['source_id']==proof['policy_source_id'])
    text=('Ссылка на ранее опубликованное сообщение: https://disclosure.1prime.ru/catalog/-12/{'+proof['message_id']+'}.uif '
        'Полный текст публикуемого сообщения с учетом внесенного изменения: Выплатить дивиденды по результатам 2025 года: '
        '3 рубля 85 копеек на одну обыкновенную акцию; 0 рублей 25 копеек на одну привилегированную акцию.')
    correction=body(text,'Сообщение об изменении или корректировке информации').replace(b'17.03.2023',b'18.03.2023')
    message=dict(provider='prime',agency='5',message_id='00000000-0000-0000-0000-000000000001',title='Сообщение об изменении или корректировке информации')
    source=store.save(correction,prime.HOST+'/news/-12/{'+message['message_id']+'}.uif','prime_message')
    events,review=parse_correction(correction,source,message,securities,policy,'7707083893',parents)
    assert len(events)==2 and {e['event_type'] for e in events}=={'amendment'}
    assert {e['supersedes_id'] for e in events}=={e['event_id'] for e in parents}
    assert parents[0]['amount']=='2.85' and events[0]['amount']=='3.85'
    assert events[0]['change_type']=='amount'
    assert parse_correction(correction,source,message,securities,policy,'7707083893',[])[0]==[]


def test_overlay_revisits_correction_waiting_in_previous_archive_report(tmp_path):
    import polars as pl
    from dividend_events.archive import build_archive_package
    from dividend_events.package import build_package
    from dividend_events.schema import SCHEMAS
    from dividend_events.storage import canonical, digest, write_json
    root = tmp_path/'archive'
    parents, _, store = parse(root/'evidence', body('Выплатить дивиденды по результатам 2025 года '
        '2 рубля на одну обыкновенную акцию.', 'Решения общего собрания акционеров'))
    proof = json.loads(parents[0]['edition_evidence'])
    policy = next(s for s in store.sources() if s['source_id']==proof['policy_source_id'])
    correction = body('Ссылка на ранее опубликованное сообщение: https://disclosure.1prime.ru/news/-12/{'+proof['message_id']+'}.uif '
        'Полный текст публикуемого сообщения с учетом внесенного изменения: Выплатить дивиденды по результатам 2025 года '
        '3 рубля на одну обыкновенную акцию.', 'Сообщение об изменении или корректировке информации').replace(b'17.03.2023',b'18.03.2023')
    message = dict(provider='prime',agency='5',message_id='00000000-0000-0000-0000-000000000001',title='Сообщение об изменении или корректировке информации')
    source = store.save(correction,prime.HOST+'/news/-12/{'+message['message_id']+'}.uif','prime_message')
    old_review = dict(inn='7707083893',message=message,source_id=source['source_id'],event_ids=[],
        review=dict(status='needs_review',reason='correction_requires_original_message_link'))
    report = dict(discovery=dict(policy=policy),reviews=[old_review])
    store.save(canonical(report),'urn:test:prior-report','archive_collection_report')
    tables = {name:[] for name in SCHEMAS}
    tables.update(securities=[dict(security(), aliases_json='[]')],sources=store.sources(),coverage=[dict(security_id=security()['security_id'],ticker='ordinary',status='partial')])
    metadata = dict(requested_from='2023-01-01',requested_to='2026-10-05',snapshot_at='2026-10-05T23:59:59+03:00',universe_sha256=digest(canonical([security()])))
    previous = build_package(root/'packages',tables,store,metadata,Path.cwd())
    directory = root/'archive-runs/current'
    write_json(directory/'settings.json',dict(provider='prime'))
    write_json(directory/'discovery.json',dict(policy=policy,groups={'7707083893':[security()]},unmatched=[],source_ids=[policy['source_id']]))
    write_json(directory/'messages/parent.json',dict(inn='7707083893',source_id=parents[0]['source_id'],
        message=dict(provider='prime',agency='5',message_id=proof['message_id'],title='Решения общего собрания акционеров'),
        review=dict(status='parsed',reason='explicit_per_share_fact')))
    target = build_archive_package(previous,root,directory)
    events = pl.read_parquet(target/'events.parquet').to_dicts()
    amendments = [e for e in events if e['event_type']=='amendment']
    assert len(amendments)==1 and amendments[0]['supersedes_id']==parents[0]['event_id']
    assert amendments[0]['amount']=='3'
