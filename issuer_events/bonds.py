"""Conservative bond obligation facts; no inferred legal cure or investor cash receipt."""
from decimal import Decimal
import re
from dividend_events.official import russian_date
from dividend_events.storage import identifier
from . import PARSER_VERSION


def valid_isin(value):
    if not re.fullmatch(r'[A-Z]{2}[A-Z0-9]{9}\d', value):
        return False
    digits = ''.join(str(ord(c)-55) if c.isalpha() else c for c in value)
    return sum(sum(map(int, str(int(c)*(2 if i % 2 else 1)))) for i,c in enumerate(reversed(digits))) % 10 == 0


def field(text, heading):
    """Numbered disclosure fields; unnumbered forms end at the next known heading."""
    end = (r'(?=\s+\d+\.\d+\.?\s|\s+3\.\s+Подпись|'
        r'\s+(?:Отчетный\s*\(купонный\)|Общий размер (?:выплаченных|начисленных) доходов|Размер выплаченных|Общее количество|Форма выплаты|Дата, в которую|Объем\s*\(в процентах\))|$)')
    m = re.search(heading+r'[^:]{0,900}:\s*(.*?)'+end, text, re.I)
    return m[1].strip() if m else None


def money(value):
    if not value:
        return None, None
    matches = list(re.finditer(r'(\d[\d \u00a0]*(?:[.,]\d{1,2})?)\s*(?:\([^)]{1,250}\)\s*)?(руб(?:л\w*)?\.?|RUB|USD|EUR)(?:\s*(\d{1,2})\s*коп\w*\.?)?', value, re.I))
    if len(matches) != 1:
        return None, None
    m = matches[0]
    amount = Decimal(re.sub(r'\s', '', m[1]).replace(',', '.'))
    if m[3]:
        amount += Decimal(m[3])/100
    return format(amount, 'f'), 'RUB' if m[2].lower().startswith('руб') else m[2].upper()


def parse(document):
    text, title = document['original_text'], document['title']
    review = dict(edition_id=document['edition_id'], status='not_bond_event', reason='outside_supported_event_types')
    if not re.search('облигаци', text, re.I):
        return [], review
    if re.search('корректиров|отмен', title, re.I):
        return [], dict(review, status='needs_review', reason='correction_requires_explicit_parent')
    isins = set(re.findall(r'\b[A-Z]{2}[A-Z0-9]{9}\d\b', text))
    if len(isins) != 1 or not valid_isin(next(iter(isins))):
        return [], dict(review, status='needs_review', reason='missing_invalid_or_multiple_ISINs')
    isin = next(iter(isins))
    nonpayment = bool(re.search(r'неисполнени[ея].*обязательств', title, re.I))
    terms = bool(re.search(r'(?:регистраци\w*|внесени\w*).*изменени.*(?:решени.*выпуск|услови)', title, re.I))
    payment = bool(re.search(r'выплаченные доходы|исполнени[еи].*обязательств', title, re.I)) and not nonpayment
    if not (nonpayment or terms or payment):
        return [], review
    content = field(text, r'Содержание обязательства эмитента')
    if payment:
        content = field(text, r'Отчетный\s*\(купонный\)\s+период')
    content = content or ''
    coupons = set(re.findall(r'(\d+)\s*(?:-?\s*(?:го|й|ый))?\s+купонн', content, re.I))
    coupon = next(iter(coupons)) if len(coupons) == 1 else None
    kind = 'coupon' if re.search('купон', content, re.I) else 'principal' if re.search('погашени|номинальн', content, re.I) else 'buyback' if re.search('приобретени|выкуп', content, re.I) else None
    if re.search('купон',content,re.I) and re.search('погашени|номинальн',content,re.I):
        kind = None  # Several obligations in one field must be separated before linking.
    due_text = field(text, r'Дата, в которую (?:обязательство|обязанность) эмитента')
    if due_text is None:
        due_text = field(text, r'Дата, в которую обязанность по выплате')
    due = russian_date(due_text) if due_text else None
    effective_text = field(text, r'Дата наступления события')
    effective = russian_date(effective_text) if effective_text else None
    amount, currency = money(content)
    evidence = content
    status = None
    if nonpayment:
        evidence = field(text, r'(?:Сведения о|факт) неисполнени[ия]') or ''
        if not re.search(r'не исполн|не выполн|не выплат|дефолт|частичн|просрочка', evidence,re.I):
            return [], dict(review, status='needs_review', reason='no_explicit_nonperformance_statement')
        status = 'reported_nonperformance'
        event_type = 'nonperformance'
    elif payment:
        percentage = field(text, r'Объем\s*\(в процентах\)')
        percent = re.match(r'(\d+(?:[.,]\d+)?)\s*(?:\([^)]*\))?\s*%', percentage or '')
        actual, paid_currency = money(field(text, r'Общий размер выплаченных доходов'))
        if not percent or not 0 < Decimal(percent[1].replace(',', '.')) <= 100:
            return [], dict(review, status='needs_review', reason='execution_fraction_not_explicit')
        if actual is None or Decimal(actual) <= 0:
            return [], dict(review, status='needs_review', reason='paid_total_missing_or_multiple_payments')
        amount, currency = actual, paid_currency
        status = 'reported_fully_executed' if Decimal(percent[1].replace(',', '.')) == 100 else 'reported_partially_executed'
        evidence = percentage
        event_type = 'execution'
    else:
        evidence = field(text, r'Краткое содержание (?:зарегистрированных )?изменений') or field(text, r'Содержание изменений')
        if not evidence:
            return [], dict(review, status='needs_review', reason='changed_terms_not_explicitly_extracted')
        status = 'registered_changes' if re.search('Регистрация', title, re.I) else 'reported_changes'
        event_type = 'terms_change'
    obligation = None
    if kind and due and currency and (kind != 'coupon' or coupon):
        obligation = identifier('obligation', [isin, kind, coupon, due, currency])
    gaps = []
    if not effective:gaps.append('event_date_not_explicit')
    if event_type != 'terms_change' and not obligation:gaps.append('obligation_identity_incomplete')
    result = dict(event_id=identifier('bond-event', [document['edition_id'], isin, event_type]),
        edition_id=document['edition_id'], document_key=document['document_key'], issuer_inn=document['issuer_inn'],
        isin=isin, event_type=event_type, status=status, obligation_id=obligation,
        obligation_kind=kind, coupon_number=coupon, due_date=due, effective_from=effective,
        amount=amount, currency=currency, amount_basis='issue_total' if amount else None,
        actual_payment_date=None, follows_event_id=None, supersedes_id=None,
        source_id=document['source_id'], source_url=document['source_url'], sha256=document['sha256'],
        published_at=document['published_at'], public_available_at=document['public_available_at'],
        observed_at=document['observed_at'], evidence=evidence, parser_version=PARSER_VERSION,
        gap_reason=';'.join(gaps) or None)
    results = [result]
    if nonpayment and obligation:
        executed = re.search(r'Обязательство\s+(?:полностью\s+)?исполнено(?:\s+в полном объеме)?\s+(\d{2}\.\d{2}\.\d{4})',evidence,re.I)
        if executed:
            actual_date = russian_date(executed[1])
            if actual_date and actual_date >= due:
                results.append(dict(result,event_id=identifier('bond-event',[document['edition_id'],isin,'execution']),
                    event_type='execution',status='reported_fully_executed',effective_from=actual_date,
                    actual_payment_date=actual_date,follows_event_id=result['event_id'],evidence=executed[0]))
    return results, dict(review, status='parsed', reason='explicit_disclosure_fact')


def link_executions(events):
    """Link the exact obligation only; payment never deletes the preceding breach."""
    rows = [dict(e) for e in events]
    for event in rows:
        if event['event_type'] != 'execution' or not event['obligation_id']:
            continue
        candidates = [e for e in rows if e['event_type'] == 'nonperformance'
            and e['obligation_id'] == event['obligation_id'] and e['public_available_at'] and event['public_available_at']
            and e['public_available_at'] < event['public_available_at']]
        if candidates:
            event['follows_event_id'] = max(candidates, key=lambda e:e['public_available_at'])['event_id']
    return rows
