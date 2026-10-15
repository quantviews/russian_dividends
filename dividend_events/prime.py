"""PRIME public regulated news archive, including historical search before 2025."""
from datetime import datetime
import json
from pathlib import Path
import re
from urllib.parse import urljoin, urlsplit, unquote
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
from .disclosure import clean, parse_message
from .storage import digest

HOST = "https://disclosure.1prime.ru"
SEARCH_URL = HOST + "/news/search2.aspx"
SEED_URL = HOST + "/portal/default.aspx?emId=7707083893"
POLICY_URL = HOST + "/misc/faquser.aspx"
PREFIX = "ctl00$LeftColumn$LeftSearchBlock$"
POLICY_MARKERS = ("Удалить сообщение из Ленты новостей нельзя", "Привести полный текст корректного сообщения")


def soup(body):
    # This legacy ASP.NET site serves Windows-1251 without a reliable HTML meta charset.
    return BeautifulSoup(body.decode("cp1251"), "html.parser")


def load_policy(store):
    body, source = store.fetch(POLICY_URL, "prime_archive_policy")
    if not all(m in clean(soup(body).get_text(" ", strip=True)) for m in POLICY_MARKERS):
        raise ValueError("PRIME news correction policy absent")
    return source


def seed_form(store):
    body, source = store.fetch(SEED_URL, "prime_search_form")
    form = {i["name"]:i.get("value", "") for i in soup(body).find_all("input", type="hidden") if i.get("name")}
    return form, source


def search_form(seed, inn, start, end):
    form = dict(seed)
    def d(value):
        return datetime.fromisoformat(value).strftime("%d.%m.%Y")
    form.update({PREFIX+k:v for k,v in dict(STargetDocs="0", SName="", SPropertyName="INN",
        SPropertyValue=inn, SType="", SearchD1=d(start), SearchD2=d(end), SubmitSearchBlockForm="Найти").items()})
    return form


def search_page(body, inn):
    page = soup(body)
    selected = page.find("input", attrs={"name":PREFIX+"SPropertyValue"})
    if selected is None or selected.get("value") != inn:
        raise ValueError("PRIME issuer search filter was not applied")
    count = re.search(r"Найдено сообщений:\s*(\d+)", clean(page.get_text(" ", strip=True)))
    if not count:
        raise ValueError("PRIME did not return a valid search result")
    messages = []
    for a in page.find_all("a", href=True):
        if not re.search(r"/\{[A-Fa-f0-9-]+\}\.uif", a["href"]):
            continue
        tr = a.find_parent("tr")
        if tr is None:
            raise ValueError("PRIME result row missing")
        issuer = tr.find("a", href=re.compile("emid=", re.I))
        if issuer is None or not re.search(r"emid="+re.escape(inn)+r"(?:&|$)", issuer["href"], re.I):
            raise ValueError("PRIME returned another issuer")
        title = clean(a.get_text(" ", strip=True))
        messages.append(dict(provider="prime", agency="5", message_id=re.search(r"\{([^}]+)\}",a["href"])[1].upper(),
            title=title, listing_text=clean(tr.get_text(" ",strip=True)), url=urljoin(SEARCH_URL,a["href"])))
    return messages, (int(count[1])+29)//30 or 1, int(count[1])


def extract_message(body):
    page = soup(body)
    panel = page.find(id="ctl00_Main_Pan")
    if panel is None or panel.find("h1", class_="news_header") is None:
        raise ValueError("PRIME news body missing")
    title = clean(panel.find("h1", class_="news_header").get_text(" ",strip=True))
    label = panel.find("h1").find_next_sibling("span").get_text(" ",strip=True)
    match = re.search(r"(\d{2}\.\d{2}\.\d{4} \d{2}:\d{2})(?::(\d{2}))?",label)
    if not match:
        raise ValueError("PRIME publication timestamp missing")
    stamp = datetime.strptime(match[0], "%d.%m.%Y %H:%M:%S" if match[2] else "%d.%m.%Y %H:%M").replace(tzinfo=ZoneInfo("Europe/Moscow"))
    for node in list(panel.find_all(["h1","span"], recursive=False)):
        node.decompose()
    text = clean(panel.get_text(" ",strip=True))
    inn = re.search(r"(?:\(ИНН\)|ИНН)[^\d]{0,100}(\d{10,12})\b",text,re.I)
    return dict(text=text, published_at=stamp.isoformat(), precision="second" if match[2] else "minute",
        publication_label=match[0], inn=inn[1] if inn else None, metadata={"Тип события":title}, semantic_sha256=digest(text.encode("utf-8")))


def parse(body, source, message, securities, policy, inn):
    return parse_message(body, source, message, securities, policy, inn, record=extract_message(body))


def validate_prime_evidence(event, sources, root, cache=None):
    proof, publication = json.loads(event["edition_evidence"]), json.loads(event["publication_evidence"])
    policy = sources.get(proof.get("policy_source_id"))
    if not policy or policy["source_url"] != POLICY_URL:
        raise ValueError("Missing PRIME archive policy")
    cache = {} if cache is None else cache
    policy_key = ("policy", policy["source_id"])
    if policy_key not in cache:
        cache[policy_key] = clean(soup((Path(root)/policy["raw_path"]).read_bytes()).get_text(" ",strip=True))
    if not all(m in cache[policy_key] for m in POLICY_MARKERS):
        raise ValueError("Invalid PRIME archive policy")
    source = sources[event["source_id"]]
    url = urlsplit(source["source_url"])
    if url.hostname != "disclosure.1prime.ru" or "{"+proof["message_id"]+"}.uif" not in unquote(url.path):
        raise ValueError("PRIME message identity mismatch")
    message_key = ("prime", source["source_id"])
    if message_key not in cache:
        cache[message_key] = extract_message((Path(root)/source["raw_path"]).read_bytes())
    record = cache[message_key]
    if (record["semantic_sha256"] != proof["semantic_sha256"] or record["inn"] != proof["issuer_inn"]
        or record["published_at"] != event["published_at"] or record["precision"] != event["publication_precision"]
        or event["publication_timezone"] != "Europe/Moscow" or publication.get("timezone") != "Europe/Moscow"
        or record["text"] != event["original_text"] or publication.get("source_id") != event["source_id"]
        or publication.get("label") != record["publication_label"] or event["evidence"] not in record["text"]
        or publication.get("agency") != proof["agency"] or publication.get("message_id") != proof["message_id"]):
        raise ValueError("PRIME publication/content evidence mismatch")
