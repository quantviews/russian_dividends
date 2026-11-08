"""Strictly scoped official history adapters; current pages never prove past availability."""

from datetime import datetime
from decimal import Decimal
import re

from bs4 import BeautifulSoup

from .model import make_event

MONTHS = {name: i for i, name in enumerate("января февраля марта апреля мая июня июля августа сентября октября ноября декабря".split(), 1)}


def russian_date(text):
    text = " ".join(text.split())
    text = re.sub(r"(\d{4})(?=г(?:ода)?(?:\.|\b))", r"\1 ", text, flags=re.I)
    match = re.search(r"\b(\d{1,2})\.(\d{2})\.(\d{4})\b", text)
    if match:
        return datetime.strptime(match.group(), "%d.%m.%Y").date().isoformat()
    match = re.search(r"\b(\d{1,2})\s+(" + "|".join(MONTHS) + r")\s+(\d{4})\b", text.lower())
    if match:
        return datetime(int(match[3]), MONTHS[match[2]], int(match[1])).date().isoformat()
    return None


def decimal_text(text):
    return str(Decimal(re.sub(r"\s", "", text).replace(",", ".")))


def period_text(text):
    text = " ".join(text.split())
    year = re.search(r"\b(?:19|20)\d{2}\b", text)
    if not year:
        return None
    prefix = "full year"
    for pattern, value in ((r"9[МM]|девяти", "9 months"), (r"6[МM]|шести|полугоди", "half year"), (r"3[МM]|трех|трёх", "3 months")):
        if re.search(pattern, text, re.I):
            prefix = value
            break
    if "нераспредел" in text:
        prefix = "retained earnings"
    return year.group() + " " + prefix


def parse_official_history(body, source, security, adapter):
    soup = BeautifulSoup(body, "html.parser")
    events = []
    if adapter == "nornickel_history_v1":
        if security.get("isin") != "RU0007288411":
            raise ValueError("Nornickel adapter requires the verified ordinary issue ISIN")
        table = next((t for t in soup.find_all("z-table") if "Дата принятия решения" in str(t.get(":columns", ""))), None)
        if table is None:
            raise ValueError("Official history layout changed")
        candidates = []
        for row in table.find_all("z-table-row"):
            cells = [c.get_text(" ", strip=True) for c in row.find_all("z-table-col")]
            if len(cells) != 6:
                raise ValueError("Official history row layout changed")
            candidates.append((row, cells[0], cells[4], cells[1], cells[2], cells[3]))
    elif adapter == "lukoil_history_v1":
        if security.get("isin") != "RU0009024277":
            raise ValueError("Lukoil adapter requires the verified ordinary issue ISIN")
        candidates = []
        for table in soup.find_all("table"):
            if "Дивиденд на 1 акцию" not in table.get_text(" ", strip=True):
                continue
            for row in table.find_all("tr"):
                cells = [c.get_text(" ", strip=True) for c in row.find_all("td", recursive=False)]
                if len(cells) != 5 or "Всего" in cells[0]:
                    continue
                candidates.append((row, cells[1], cells[2], cells[4], "", ""))
        if not candidates:
            raise ValueError("Official history layout changed")
    else:
        raise ValueError("Unknown official adapter")
    seen = set()
    for row, period, amount_text, record_text, decision_text, payment_text in candidates:
        reporting = period_text(period)
        if not reporting:
            continue
        amount = decimal_text(amount_text)
        record = russian_date(record_text)
        decision = russian_date(decision_text)
        logical_key = ["official_dividend", reporting]
        if reporting in seen:
            # Keep distinct declarations; do not silently use record_date as the sole key.
            logical_key += [record, decision]
        seen.add(reporting)
        fields = dict(event_type="no_dividend" if Decimal(amount) == 0 else "approval",
            status="no_dividend" if Decimal(amount) == 0 else "approved", amount=amount,
            amount_basis="unknown", currency="RUB", reporting_period=reporting,
            decision_body="general_meeting" if decision else None, decision_at=decision,
            record_date=record, record_date_origin="official" if record else None,
            payment_to=russian_date(payment_text), recipient_category="not_disambiguated" if payment_text else None,
            verification_status="verified", historical_availability="unproven", publication_precision="unknown",
            evidence=row.get_text(" ", strip=True), original_text=str(row),
            share_basis_text="original per-share amount displayed by issuer; basis date not established",
            gap_reason="current_official_page_confirms_fact_but_not_historical_edition_or_publication_time")
        events.append(make_event(fields, source, security, logical_key))
    return events
