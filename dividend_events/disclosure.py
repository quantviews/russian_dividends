"""Regulated SKRIN news archive: issuer discovery, immutable message evidence and parsing.

The archive's technical conditions 3.6/3.7/3.9 provide the publication/edition
contract. This policy applies to news messages, NOT mutable issuer attachments.
"""

from collections import defaultdict
from datetime import datetime
from decimal import Decimal
import json
from pathlib import Path
import re
from urllib.parse import parse_qs, urlsplit
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup
import polars as pl

from .model import make_event, timestamp
from .official import russian_date
from .storage import canonical, digest, identifier, now, read_json, write_json
from .universe import block

HOST = "https://disclosure.skrin.ru"
POLICY_URL = HOST + "/index.asp?id=61&fid=0"
SEARCH_URL = HOST + "/EventList.asp?id=36"
POLICY_MARKERS = (
    "не вправе вносить изменения в Сообщение",
    "время и дату опубликования Сообщения",
    "полный текст публикуемого Сообщения с учетом внесенных изменений",
)


def clean(value):
    return " ".join(value.replace("\u200b", "").split())


def load_policy(store):
    body, source = store.fetch(POLICY_URL, "disclosure_archive_policy")
    text = clean(BeautifulSoup(body, "html.parser").get_text(" ", strip=True))
    if not all(marker in text for marker in POLICY_MARKERS):
        raise ValueError("Archive immutability policy missing/changed; historical proof disabled")
    return source


def categories(store):
    """Read codes from the actual selector; never guess a category ID."""
    rows, source_ids = [], []
    for node in ("16", "14", "1", "2", "11", "21"):
        body, source = store.fetch(HOST + "/get_evtp_nodes.asp", "disclosure_categories", form={"id": node})
        source_ids.append(source["source_id"])
        rows.extend(json.loads(body))
    patterns = ("решения совета", "решения общ", "начисленные доходы", "выплаченные доходы",
                "корректировке информации", "начисленные и/или выплаченные", "дивиденд",
                "пресс-релиз о решениях", "проведении общего собрания")
    selected = [row for row in rows if row.get("isFolder") == "0"
                and any(p in row["title"].lower() for p in patterns)]
    if not selected:
        raise ValueError("No relevant categories found")
    return selected, source_ids


def issuer_registry(package):
    """INN comes from pinned MOEX rows, matched on both ISIN and registration number."""
    root = Path(package)
    lookup = defaultdict(set)
    by_isin = defaultdict(set)
    for source in pl.read_parquet(root / "sources.parquet").to_dicts():
        if source["source_kind"] != "moex_registry":
            continue
        for row in block(read_json(root / source["raw_path"]), "securities"):
            inn = str(row.get("emitent_inn") or "")
            if inn and row.get("isin"):
                lookup[(row["isin"], row.get("regnumber"))].add(inn)
                by_isin[row["isin"]].add(inn)
    groups, unmatched = {}, []
    for security in pl.read_parquet(root / "securities.parquet").to_dicts():
        inns = lookup[(security["isin"], security["regnumber"])]
        if len(inns) == 1:
            inn = next(iter(inns))
            groups.setdefault(inn, []).append(security)
        else:
            issuer_candidates = by_isin.get(security["isin"], set())
            unmatched.append({"security_id": security["security_id"], "ticker": security["ticker"],
                              "status": "identity_unresolved", "reason": "historical_issue_registration_unresolved" if len(issuer_candidates)==1 else "no_unique_official_issuer_identifier",
                              "search_inn": next(iter(issuer_candidates)) if len(issuer_candidates)==1 else None})
    return groups, unmatched


def search_form(inn, start, end, selected, page):
    first, last = start.split("-"), end.split("-")
    return dict(ev_kod="inn", ev_kod_val=inn, ev_fi_name="", ev_type="", ev_type_excl="0",
                ev_type_val=",".join(sorted({r["id"] for r in selected}, key=int)),
                ev_type_papas=",".join(sorted({r["parent"] for r in selected}, key=int)),
                ev_from_dd=first[2], ev_from_mm=first[1], ev_from_yy=first[0],
                ev_to_dd=last[2], ev_to_mm=last[1], ev_to_yy=last[0], ev_pno=str(page))


def search_page(body, inn):
    soup = BeautifulSoup(body, "html.parser")
    selected_inn = soup.find("input", {"name": "ev_kod_val"})
    if selected_inn is None or selected_inn.get("value") != inn:
        raise ValueError("Issuer search filter was not applied")
    text = clean(soup.get_text(" ", strip=True))
    pages = re.search(r"Всего:\s*(\d+)\s*страниц", text)
    total = int(pages[1]) if pages else 1
    messages = []
    for anchor in soup.find_all("a", href=True):
        match = re.search(r"ShowMessage\((\d+),\s*(\d+)\)", anchor["href"])
        if not match:
            continue
        tr = anchor.find_parent("tr")
        messages.append(dict(message_id=match[1], agency=match[2], title=clean(anchor.get_text(" ", strip=True)),
                             listing_text=clean(tr.get_text(" ", strip=True)),
                             url=f"{HOST}/ShowMessage.asp?id=36&eid={match[1]}&agency={match[2]}"))
    return messages, total


def extract_message(body):
    soup = BeautifulSoup(body, "html.parser")
    publication_label = soup.find(string=lambda s: s and "Дата и время публикации" in s)
    if publication_label is None:
        raise ValueError("Not an archived news message")
    table = publication_label.find_parent("table")
    metadata = {}
    for tr in table.find_all("tr", recursive=False) or table.select(":scope > tbody > tr"):
        cells = tr.find_all("td", recursive=False)
        if len(cells) == 2:
            metadata[clean(cells[0].get_text(" ", strip=True))] = clean(cells[1].get_text(" ", strip=True))
    content = table.find("td", attrs={"colspan": "2", "valign": "top"})
    if content is None:
        raise ValueError("Message body not found")
    text = clean(content.get_text(" ", strip=True))
    pub = metadata.get("Дата и время публикации", "")
    day = russian_date(pub)
    clock = re.search(r"\b(\d{2}:\d{2}:\d{2})\b", pub)
    if not day or not clock:
        raise ValueError("Archive publication timestamp absent")
    published = datetime.fromisoformat(day + "T" + clock[1]).replace(tzinfo=ZoneInfo("Europe/Moscow")).isoformat()
    inn = re.search(r"(?:\(ИНН\)|ИНН\s*(?:эмитента|общества)?)[^\d]{0,150}(\d{10,12})\b", text, re.I)
    if not inn:
        # Regulatory form: spelled-out tax identifier can omit the acronym.
        inn = re.search(r"Идентификационный номер налогоплательщика[^\d]{0,150}(\d{10,12})\b", text, re.I)
    return dict(text=text, published_at=published, publication_label=pub,
                inn=inn[1] if inn else None, metadata=metadata,
                semantic_sha256=digest(text.encode("utf-8")))


def find_period(text):
    text = re.sub(r"([369])\s*\([^)]{1,30}\)\s*(?=месяц)", r"\1 ", text, flags=re.I)
    text = re.sub(r"(20\d{2})\s+(?:отчетного|отчётного|финансового)\s+года", r"\1 года", text, flags=re.I)
    patterns = [r"(?:по\s+(?:итогам|результатам)|за)\s+(?:работы\s+)?(?:(?:перв(?:ое|ого)\s+)?(полугоди[ея]|[369]\s*месяц\w*|девят[ьи]\s+месяц\w*|шест[ьи]\s+месяц\w*|три\s+месяц\w*)\s+)?(20\d{2})\s*(?:года|год|г\.)",
                r"(?:дивиденд\w*|прибыл\w*)\s+за\s+(20\d{2})\s*(?:год|г\.)"]
    found = set()
    for pattern in patterns:
        for match in re.finditer(pattern, text, re.I):
            groups = match.groups()
            year, interval = (groups[-1], groups[0] or "") if len(groups) == 2 else (groups[0], "")
            period = "half year" if "полугод" in interval or re.search(r"6|шест", interval) else "9 months" if re.search(r"9|девят", interval) else "3 months" if interval else "full year"
            found.add(year + " " + period)
    for match in re.finditer(r"(?:по\s+(?:итогам|результатам)|за)\s+(?:I|1|перв(?:ый|ого))\s+квартал\w*\s+(20\d{2})\s*год", text, re.I):
        found.add(match[1] + " 3 months")
    return next(iter(found)) if len(found) == 1 else None


def disclosure_field(text, heading):
    match = re.search(heading + r"[^:]{0,1000}:\s*(.*?)(?=\s+\d+\.\d+\.\s|\s+3\.\s+Подпись|$)", text, re.I)
    return match[1] if match else None


def meeting_episode_date(text, board, meeting, declared):
    if board:
        match = re.search(r"(?:Рекомендовать|Предложить)\s+(?:(?:годовому|внеочередному)\s+)?общему собранию акционеров(.{0,180}?)\s+принять", text, re.I)
        return russian_date(match[1]) if match else None
    if meeting:
        value = disclosure_field(text, r"Дата проведения (?:заседания\s+)?общего собрания")
        return russian_date(value) if value else None
    if declared:
        value = disclosure_field(text, r"Дата принятия решения о выплате")
        return russian_date(value) if value else None
    return None


def parse_message(body, source, message, securities, policy, expected_inn, *, record=None):
    record = record or extract_message(body)
    provider = message.get("provider", "skrin")
    text, title = record.get("analysis_text", record["text"]), record["metadata"].get("Тип события", message["title"])
    if record["inn"] != expected_inn:
        return [], {"status": "identity_mismatch", "reason": "message_INN_does_not_match_pinned_MOEX_issuer"}
    income_category = re.search(r"Категория выплат[^:]{0,650}:\s*([^.;]{1,220})", text, re.I)
    if income_category and re.search(r"купон|номинальн\w*\s+стоимост", income_category[1], re.I) and "дивиденд" not in income_category[1].lower():
        return [], {"status":"not_dividend", "reason":"explicit_bond_income_category"}
    if "дивиденд" not in text.lower():
        return [], {"status": "not_dividend", "reason": "no_dividend_content"}
    # Corrections/cancellations must be linked to their original document before promotion.
    if "корректиров" in title.lower() or "отмен" in title.lower():
        return [], {"status": "needs_review", "reason": "correction_requires_original_message_link"}
    board = ("совета" in title.lower() or "наблюдательного" in title.lower()) and "решени" in title.lower()
    meeting = ("собран" in title.lower() or "единственного" in title.lower()) and "решени" in title.lower()
    declared = "начислен" in title.lower()
    paid = "выплачен" in title.lower()
    if not (board or meeting or declared or paid):
        return [], {"status": "needs_review", "reason": "unsupported_event_context"}
    if (board or meeting) and re.search(r"решени[ея]\s+(?:[^.;]{0,140}\s+)?не\s+принят", text, re.I):
        return [], {"status": "needs_review", "reason": "unadopted_resolution_requires_question_scope_review"}
    if board and not re.search(r"рекоменд\w*|предложить", text, re.I):
        return [], {"status": "needs_review", "reason": "board_decision_is_not_a_dividend_recommendation"}
    if re.search(r"с учетом\s+(?:ранее\s+)?выплаченных\s+дивиденд", text, re.I):
        return [], {"status": "needs_review", "reason": "net_of_prior_distribution_requires_review"}
    # Extract only explicitly per-share amounts, never totals, percentages, years or nominal values.
    number = r"\d+(?:[ \u00a0]\d{3})*(?:[,.]\d+)?"
    share_words = r"(?:(?:размещенн\w*|обыкновенн\w*|привилегированн\w*|именн\w*|бездокументарн\w*)\s+)*акци[юи]"
    amount_re = re.compile(r"(?<![\d.,])(" + number + r")\s*(?:\([^)]{1,150}\)\s*)?(?:руб(?:лей|ля|ль|\.)?|₽)\s*(?:\([^)]{1,150}\)\s*)?(?:на|за)\s+(?:одну|1)\s+" + share_words, re.I)
    reverse_re = re.compile(r"(?:на|за)\s+(?:одну|1)\s+" + share_words + r"\s*(?:[–—:=-]|в размере)?\s*(" + number + r")\s*руб(?:лей|ля|ль|\.)?", re.I)
    kopeck_re = re.compile(r"(?<![\d.,])(" + number + r")\s*(?:\([^)]{1,100}\)\s*)?руб(?:лей|ля|ль|\.)?\s*(\d{1,2})\s*(?:\([^)]{1,100}\)\s*)?коп(?:еек|ейки|ейка|\.)?\s+(?:на|за)\s+(?:одну|1)\s+" + share_words, re.I)
    candidates = []
    for match in list(amount_re.finditer(text)) + list(reverse_re.finditer(text)) + list(kopeck_re.finditer(text)):
        if re.search(r"номинальн\w*\s+стоимост\w*\s*(?:в размере|до|[:–—-])?\s*$", text[max(0,match.start()-100):match.start()], re.I):
            continue
        left = max(0, match.start() - 350)
        right = min(len(text), match.end() + 100)
        context = text[left:right]
        if "дивиденд" not in context.lower() and not (paid and re.search(r"Категория выплат[^:]{0,500}:\s*дивиденды по акциям", text, re.I)):
            continue
        period = find_period(context) or find_period(text)
        if not period:
            continue
        preceding = text[max(left, match.start()-140):match.start()]
        following = text[match.start():match.end()]
        kind_text = preceding + " " + following
        ordinary = bool(re.search(r"обыкновенн", kind_text, re.I))
        preferred = bool(re.search(r"привилегированн", kind_text, re.I))
        # The nearest explicit class before the amount takes precedence over the previous clause.
        class_matches = list(re.finditer(r"обыкновенн\w*|привилегированн\w*", preceding, re.I))
        if re.search(r"обыкновенн|привилегированн", following, re.I):
            ordinary = bool(re.search(r"обыкновенн", following, re.I))
            preferred = bool(re.search(r"привилегированн", following, re.I))
        elif class_matches and not re.search(r"обыкновенн\w*\s+и\s+привилегированн", kind_text, re.I):
            ordinary = class_matches[-1][0].lower().startswith("обыкновенн")
            preferred = not ordinary
        classes = [k for k, present in (("ordinary", ordinary), ("preferred", preferred)) if present]
        if not classes:
            available = {s["share_class"] for s in securities if s["instrument_type"] in {"common_share", "preferred_share"}}
            if len(available) == 1:
                classes = list(available)
        for share_class in classes:
            value = Decimal(re.sub(r"\s", "", match[1]).replace(",", "."))
            if match.lastindex == 2:
                value += Decimal(match[2])/100
            candidates.append(dict(amount=str(value),
                                   share_class=share_class, period=period, evidence=context, no_dividend=False))
    no_pay = list(re.finditer(r"(?:дивиденды\s+(?:по\s+[^.;]{0,160})?не\s+(?:объявлять(?:\s+и\s+не\s+выплачивать)?|выплачивать)|не\s+(?:объявлять\s+и\s+не\s+)?выплачивать\s+дивиденды)", text, re.I))
    for match in no_pay:
        context = text[max(0,match.start()-900):match.end()+220]
        period = find_period(context) or find_period(text)
        if period:
            class_scope = match[0].lower()
            for share_class in {s["share_class"] for s in securities if s["instrument_type"] in {"common_share", "preferred_share"}}:
                if "привилегирован" in class_scope and "обыкновенн" not in class_scope and share_class != "preferred":
                    continue
                if "обыкновенн" in class_scope and "привилегирован" not in class_scope and share_class != "ordinary":
                    continue
                scope = "no_additional_distribution" if re.search(r"повторно|дополнительно", text[max(0,match.start()-120):match.end()], re.I) else None
                candidates.append(dict(amount="0", share_class=share_class, period=period, evidence=context, no_dividend=True, scope=scope))
    # Standard numbered income forms put the value after a long per-share label.
    if declared or paid:
        per_share = disclosure_field(text, r"(?<!Общий )Размер (?:начисленных|выплаченных)\s*\(")
        period_answer = disclosure_field(text, r"Отчетный\s*\(купонный\)\s+период")
        identity = disclosure_field(text, r"Идентификационные признаки ценных бумаг") or ""
        value = re.fullmatch(r"(" + number + r")\s*руб(?:лей|ля|ль|\.)?\s*[;.]?", per_share or "", re.I)
        period = find_period("за " + period_answer) if period_answer else None
        classes = [kind for kind, word in (("ordinary", "обыкновенн"), ("preferred", "привилегированн")) if word in identity.lower()]
        if value and period and len(classes) == 1:
            candidates.append(dict(amount=str(Decimal(re.sub(r"\s", "", value[1]).replace(",", "."))),
                share_class=classes[0], period=period, evidence=per_share, no_dividend=False))
    groups = defaultdict(list)
    for candidate in candidates:
        groups[(candidate["share_class"], candidate["period"])].append(candidate)
    events, ambiguous = [], []
    for (share_class, period), group in groups.items():
        if len({r["amount"] for r in group}) != 1:
            ambiguous.append("multiple_amounts_for_same_period_and_class")
            continue
        candidate = group[0]
        matches = [s for s in securities if s["share_class"] == share_class]
        # Registration numbers may use visually identical Cyrillic letters. Match
        # whole tokens: the main issue must not match an additional issue suffix.
        registration_text = text.upper().translate(str.maketrans("АВЕКМНОРСТХ", "ABEKMHOPCTX"))
        registrations = [s for s in matches if s.get("regnumber") and re.search(
            r"(?<![A-ZА-Я0-9-])" + re.escape(s["regnumber"].upper().translate(
                str.maketrans("АВЕКМНОРСТХ", "ABEKMHOPCTX"))) + r"(?![A-ZА-Я0-9-])", registration_text)]
        explicit = registrations or [s for s in matches if s.get("isin") and s["isin"] in text]
        if explicit:
            matches = explicit
        if len(matches) != 1:
            ambiguous.append("ambiguous_issue_identity")
            continue
        security = matches[0]
        event_type = "recommendation" if board else "payment" if paid else "no_dividend" if candidate["no_dividend"] or Decimal(candidate["amount"]) == 0 else "approval"
        status = {"recommendation":"proposed", "payment":"paid", "no_dividend":"no_dividend", "approval":"approved"}[event_type]
        actual_date = None
        actual_amount = None
        partial = None
        if paid:
            payment_date = re.search(r"(?:дата\s+(?:исполнения\s+(?:обязательств\w*|обязанности)|выплаты)|фактическ\w*\s+дата)[^.;]{0,150}(\d{2}\.\d{2}\.\d{4})", text, re.I)
            actual_date = russian_date(payment_date[1]) if payment_date else None
            total_field = disclosure_field(text, r"Общий размер выплаченных доходов") or ""
            totals_paid = list(re.finditer(r"(" + number + r")\s*(?:\([^)]{1,150}\)\s*)?руб", total_field, re.I))
            if len(totals_paid) == 1 and len(groups) == 1:
                actual_amount = str(Decimal(re.sub(r"\s", "", totals_paid[0][1]).replace(",", ".")))
                if Decimal(actual_amount) == 0:
                    ambiguous.append("zero_paid_total_does_not_confirm_payment")
                    continue
            percent_field = disclosure_field(text, r"Объем\s*\(в процентах\)") or ""
            percent = re.match(r"(\d+(?:[,.]\d+)?)\s*(?:\([^)]{1,100}\)\s*)?%", percent_field, re.I)
            if percent:
                partial = Decimal(percent[1].replace(",", ".")) < 100
            if actual_date is None and actual_amount is None:
                ambiguous.append("payment_without_unambiguous_actual_date")
                continue
        fields = dict(event_type=event_type, status=status, amount="0" if event_type == "no_dividend" else candidate["amount"], currency="RUB", amount_basis="unknown",
            reporting_period=period, decision_body="board" if board else "general_meeting" if meeting else None,
            published_at=record["published_at"], publication_precision=record.get("precision", "second"), publication_timezone="Europe/Moscow",
            verification_status="verified", historical_availability="proven", evidence=candidate["evidence"], original_text=record["text"],
            actual_payment_date=actual_date, actual_payment_amount=actual_amount, partial_payment=partial,
            gap_reason="actual_payment_date_not_disclosed" if paid and actual_date is None else None,
            share_basis_text="announced per-share amount; original share basis date not established",
            publication_evidence=json.dumps({"kind":"regulated_news_archive", "provider":provider, "source_id":source["source_id"],
                "label":record["publication_label"], "timezone":"Europe/Moscow", "agency":message["agency"], "message_id":message["message_id"]}),
            edition_evidence=json.dumps({"kind":"regulated_news_archive", "provider":provider, "policy_source_id":policy["source_id"],
                "policy_url":policy["source_url"], "clauses":["immutable news; corrections in separate messages"], "semantic_sha256":record["semantic_sha256"],
                "issuer_inn":expected_inn, "agency":message["agency"], "message_id":message["message_id"]}))
        rights = []
        for match in re.finditer(r"дат[ауые]\s*,?\s*на которую определя[юл]\w*\s+лица[^;]{0,240}", text, re.I):
            if re.search(r"\bс\s*$", text[max(0,match.start()-15):match.start()], re.I):
                continue  # "25 рабочих дней с даты ... — не позднее ..." is a payment deadline.
            clause = re.split(r"\.(?=\s+[А-ЯA-Z]|\s+\d+\.)", match[0])[0]
            if not re.search(r"получени\w*\s+дивиденд", clause, re.I):
                continue
            value = russian_date(clause)
            if value:
                rights.append(value)
        for match in re.finditer(r"(?:установить|утвердить|определить)\s+(.{0,70}?)\s+(?:датой|в качестве даты),?\s*на которую определяются(.{0,200})", text, re.I):
            if not re.search(r"получени\w*\s+дивиденд", match[2], re.I):
                continue
            value = russian_date(match[1])
            if value:
                rights.append(value)
        document_period = find_period(text)
        period_field = disclosure_field(text, r"Отчетный\s*\(купонный\)\s+период") if declared or paid else None
        if period_field:
            document_period = find_period("за " + period_field)
        if len(set(rights)) == 1 and document_period == period and Decimal(candidate["amount"]) > 0:
            fields.update(record_date=rights[0], record_date_origin="official")
        # An event date is not necessarily the date the meeting/board actually decided.
        decision = re.search(r"Дата\s+(?:проведения\s+(?:общего собрания|заседания совета директоров)|принятия решения советом директоров)[^:]{0,100}:\s*([^;]{1,80})", text, re.I)
        if decision:
            fields["decision_at"] = russian_date(decision[1])
        episode_date = meeting_episode_date(text, board, meeting, declared)
        logical_key = ["official_dividend", period, "meeting", episode_date] if episode_date else [
            "official_dividend", period, "unresolved_document", provider, message["agency"], message["message_id"]]
        edition = json.loads(fields["edition_evidence"])
        edition["episode_linkage"] = dict(basis="explicit_meeting_date" if episode_date else "unresolved_document", meeting_date=episode_date)
        fields["edition_evidence"] = json.dumps(edition)
        if candidate.get("scope"):
            logical_key.append(candidate["scope"])
        event = make_event(fields, source, security, logical_key)
        # Dynamic menus must not create another event for the same immutable message text.
        event["event_id"] = identifier("event", [security["security_id"], message["agency"], message["message_id"],
             record["semantic_sha256"], event_type, period, candidate["amount"]])
        event["revision_id"] = event["event_id"]
        events.append(event)
        if declared and event_type == "approval":
            deadline = disclosure_field(text, r"Дата,\s*в которую обязанность по выплате доходов")
            # A single explicitly stated deadline is not an actual payment date.
            if deadline and re.fullmatch(r"\d{2}\.\d{2}\.\d{4}\s*(?:года|г\.)?\s*[.;]?", deadline, re.I):
                terms = dict(event, event_type="payment_terms", payment_to=russian_date(deadline),
                    recipient_category="not_specified_in_source", evidence=deadline)
                terms["event_id"] = identifier("event", [event["event_id"], "payment_terms", deadline])
                terms["revision_id"] = terms["event_id"]
                events.append(terms)
    return events, {"status":"parsed" if events else "needs_review", "reason":";".join(sorted(set(ambiguous))) or ("explicit_per_share_fact" if events else "no_unambiguous_per_share_fact"),
                    "dividend_content":True, "published_at":record["published_at"]}


def parse_correction(body, source, message, securities, policy, expected_inn, previous):
    """Only an explicit archived document ID can establish a correction's parent."""
    if message.get("provider") == "prime":
        from .prime import extract_message as extract
    else:
        extract = extract_message
    record = extract(body)
    text = record["text"]
    identities = set()
    for match in re.finditer(r"https?://[^\s<>\"]+",text):
        url = urlsplit(match[0].rstrip(".,;)"))
        if url.hostname == "disclosure.skrin.ru" and url.path.lower() == "/showmessage.asp":
            query=parse_qs(url.query)
            if query.get("eid") and query.get("agency"):
                identities.add(("skrin", query["agency"][0], query["eid"][0]))
        if url.hostname == "disclosure.1prime.ru":
            guid=re.search(r"\{([A-Fa-f0-9-]+)\}\.uif",url.path)
            if guid:identities.add(("prime","5",guid[1].upper()))
    parents=[]
    for event in previous:
        try:proof=json.loads(event.get("edition_evidence") or "null")
        except (ValueError,TypeError):continue
        if isinstance(proof,dict) and proof.get("issuer_inn")==expected_inn and (
            proof.get("provider","skrin"),proof.get("agency"),proof.get("message_id")) in identities:
            parents.append(event)
    marker=re.search(r"Полный текст публикуемого сообщения с учетом внесенн\w* изменени\w*[^:]{0,300}:\s*",text,re.I)
    if not parents or marker is None:
        return [],dict(status="needs_review",reason="correction_requires_original_archived_event_and_full_corrected_text")
    bodies={e["decision_body"] for e in parents}
    if len(bodies)!=1 or next(iter(bodies)) not in {"board","general_meeting"}:
        return [],dict(status="needs_review",reason="unsupported_correction_event_context")
    record["analysis_text"]=text[marker.end():]
    record["metadata"]["Тип события"]="Решения совета директоров" if "board" in bodies else "Решения общего собрания акционеров"
    candidates,review=parse_message(body,source,message,securities,policy,expected_inn,record=record)
    result=[]
    for event in candidates:
        matches=[e for e in parents if e["reporting_period"]==event["reporting_period"] and e["security_id"]==event["security_id"]]
        if len(matches)!=1 or event["status"] not in {"proposed","approved"}:continue
        parent=matches[0]
        if event.get("public_available_at") and parent.get("public_available_at") and timestamp(event["public_available_at"]) < timestamp(parent["public_available_at"]):
            review["reason"] = "correction_publication_precedes_parent"
            continue
        edition = json.loads(event["edition_evidence"])
        edition["episode_linkage"] = dict(basis="explicit_parent_document", parent_event_id=parent["event_id"])
        event["edition_evidence"] = json.dumps(edition)
        event.update(event_type="amendment",supersedes_id=parent["event_id"],dividend_id=parent["dividend_id"],
            change_type="_and_".join(k for k in ("amount","record_date","payment_from","payment_to") if parent.get(k)!=event.get(k)) or "document_correction")
        event["event_id"]=identifier("event",[event["event_id"],"amendment",parent["event_id"]])
        event["revision_id"]=event["event_id"]
        result.append(event)
    return result,dict(status="parsed" if result else "needs_review",reason="explicit_archived_parent_link" if result else review["reason"],
                       dividend_content=True,published_at=record["published_at"])


def validate_archive_evidence(event, sources, root, cache=None):
    """Recheck the archived document and the policy, not just a free-form assertion."""
    if not (event.get("edition_evidence") or "").startswith("{"):
        return
    proof = json.loads(event["edition_evidence"])
    if proof.get("kind") != "regulated_news_archive":
        return
    if proof.get("provider") == "prime":
        from .prime import validate_prime_evidence
        return validate_prime_evidence(event, sources, root, cache)
    publication = json.loads(event["publication_evidence"])
    policy = sources.get(proof.get("policy_source_id"))
    if not policy or policy["source_url"] != POLICY_URL:
        raise ValueError("Missing archive policy evidence")
    cache = {} if cache is None else cache
    policy_key = ("policy", policy["source_id"])
    if policy_key not in cache:
        cache[policy_key] = clean(BeautifulSoup((Path(root) / policy["raw_path"]).read_bytes(), "html.parser").get_text(" ", strip=True))
    policy_text = cache[policy_key]
    if not all(marker in policy_text for marker in POLICY_MARKERS):
        raise ValueError("Archive policy does not establish immutable messages")
    source = sources[event["source_id"]]
    url = urlsplit(source["source_url"])
    query = parse_qs(url.query)
    if url.hostname != "disclosure.skrin.ru" or url.path.lower() != "/showmessage.asp":
        raise ValueError("Archive proof refers to a non-message URL")
    if query.get("eid") != [proof["message_id"]] or query.get("agency") != [proof["agency"]]:
        raise ValueError("Archive message identity mismatch")
    message_key = ("skrin", source["source_id"])
    if message_key not in cache:
        cache[message_key] = extract_message((Path(root) / source["raw_path"]).read_bytes())
    record = cache[message_key]
    if (record["semantic_sha256"] != proof["semantic_sha256"] or record["inn"] != proof["issuer_inn"]
        or record["published_at"] != event["published_at"] or record["text"] != event["original_text"]
        or event["publication_precision"] != "second" or event["publication_timezone"] != "Europe/Moscow"
        or publication.get("timezone") != "Europe/Moscow"
        or publication.get("source_id") != event["source_id"] or publication.get("label") != record["publication_label"]
        or event["evidence"] not in record["text"] or publication.get("agency") != proof["agency"] or publication.get("message_id") != proof["message_id"]):
        raise ValueError("Archive publication/content evidence mismatch")
