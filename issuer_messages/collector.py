"""Shared issuer evidence collection; no portfolio, trades or Telegram credentials."""
import re
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlsplit
from pathlib import Path
from bs4 import BeautifulSoup
from dividend_events.storage import EvidenceStore, digest, now, read_json, write_json

SOURCES = (
    ('volsta', 'https://volsta.ru/disclosure', 'pvo_statement'),
    ('evrotrans', 'https://www.evrotrans-ao.ru/feed/', 'issuer_statement'),
    ('interfax_eutr', 'https://www.interfax.ru/quote/EUTR/News', 'news_report'),
)

def clean(text):
    return ' '.join(text.split())

def record(source, inn, url, title, text, published, evidence):
    identity = digest((source + '\n' + url).encode())
    text = clean(text)
    return dict(message_id=identity, source_id=source, issuer_inn=inn, url=url,
                title=clean(title), text=text, published_at=published,
                observed_at=now(), content_sha256=digest((title+'\n'+text).encode()),
                isins=sorted(set(re.findall(r'\bRU[A-Z0-9]{10}\b', text))),
                case_numbers=sorted(set(re.findall(r'А\d{1,3}-\d+/20\d{2}', text))),
                evidence_id=evidence['source_id'], raw_path=evidence['raw_path'],
                parser_version='issuer-messages/1.0',
                verification='publisher_statement_not_court_act',
                deadlines_verified=[])

def parse_volsta(body, evidence):
    soup=BeautifulSoup(body, 'html.parser')
    if not soup.select('tr') or 'Сообщения представителя' not in soup.get_text():
        raise ValueError('Disclosure table absent')
    result=[]
    for row in soup.select('tr'):
        text=row.get_text(' ',strip=True)
        if 'RU000A107YT9' not in text:
            continue
        a=row.select_one('a[href^="#"]')
        if not a:
            raise ValueError('Disclosure anchor absent')
        content=soup.select_one(a['href'])
        if content is None or '6445005149' not in content.get_text():
            raise ValueError('Issuer identity not verified')
        cells=row.select('td')
        from datetime import datetime
        published=datetime.strptime(cells[1].get_text(strip=True),'%d.%m.%Y %H:%M').isoformat()+'+03:00'
        result.append(record('volsta','6445005149','https://volsta.ru/disclosure'+a['href'],a.get_text(' ',strip=True),content.get_text('\n',strip=True),published,evidence))
    return result

def parse_issuer_feed(body,evidence):
    xml=ET.fromstring(body)
    if xml.find('channel') is None:
        raise ValueError('RSS absent')
    result=[]
    for item in xml.findall('./channel/item'):
        url=item.findtext('link','')
        if urlsplit(url).hostname not in ('www.evrotrans-ao.ru','evrotrans-ao.ru'):
            raise ValueError('Unexpected issuer article host')
        raw=item.findtext('{http://purl.org/rss/1.0/modules/content/}encoded') or item.findtext('description','')
        text=BeautifulSoup(raw,'html.parser').get_text('\n',strip=True)
        published=parsedate_to_datetime(item.findtext('pubDate')).isoformat()
        result.append(record('evrotrans','5029169023',url,item.findtext('title',''),text,published,evidence))
    if not result:
        raise ValueError('Empty issuer feed; coverage unverified')
    return result

def news_links(body):
    soup=BeautifulSoup(body,'html.parser')
    links=[]
    for a in soup.select('a[href]'):
        title=a.get_text(' ',strip=True)
        if not re.search('евротранс',title,re.I):
            continue
        if re.fullmatch(r'/(business|russia|moscow)/\d+',a['href']):
            url=urljoin('https://www.interfax.ru',a['href'])
            if url not in links:links.append(url)
    if not links:raise ValueError('Issuer news list absent')
    return links[:8]

def parse_news(body,url,evidence):
    soup=BeautifulSoup(body,'html.parser');article=soup.select_one('[itemprop="articleBody"]')
    title=soup.select_one('h1');stamp=soup.select_one('time[datetime]')
    if article is None or title is None or not re.search('евротранс',article.get_text(),re.I):
        raise ValueError('Issuer article absent')
    return record('interfax_eutr','5029169023',url,title.get_text(),article.get_text('\n',strip=True),stamp.get('datetime') if stamp else None,evidence)

def collect(root):
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    lock=root/'run.lock'
    with lock.open('x') as f:f.write(now())
    try:
        previous=read_json(root/'latest.json') if (root/'latest.json').exists() else {'messages':[]}
        messages={m['message_id']:m for m in previous['messages']}
        health={};store=EvidenceStore(root/'evidence',delay=2,timeout=20,retries=0,refresh=True)
        for source,url,role in SOURCES:
            try:
                body,evidence=store.fetch(url,role)
                rows=[]
                if source=='volsta':rows=parse_volsta(body,evidence)
                elif source=='evrotrans':rows=parse_issuer_feed(body,evidence)
                else:
                    for link in news_links(body):
                        article,receipt=store.fetch(link,role)
                        rows.append(parse_news(article,link,receipt))
                for row in rows:
                    row['source_role']=role
                    old=messages.get(row['message_id'])
                    row['first_seen_at']=old['first_seen_at'] if old else row['observed_at']
                    row['revisions']=old.get('revisions',[]) if old else []
                    if old and old['content_sha256']!=row['content_sha256']:
                        row['revisions']=row['revisions']+[{'content_sha256':old['content_sha256'],'evidence_id':old['evidence_id'],'observed_at':old['observed_at']}]
                    messages[row['message_id']]=row
                health[source]={'status':'ok','checked_at':now(),'records_on_page':len(rows),'latest_publication':max((r['published_at'] for r in rows if r['published_at']),default=None),'coverage':'current_listing_only','url':url}
            except (ValueError,RuntimeError,OSError,ET.ParseError) as exc:
                health[source]={'status':'unavailable','checked_at':now(),'reason':type(exc).__name__,'url':url}
        package={'schema_version':1,'created_at':now(),'sources':health,'messages':list(messages.values()),'limitations':['Ни один подключённый источник не является судебным актом.','Текущие ленты не подтверждают полноту истории; Федресурс/e-disclosure недоступны.','Автоматического подтверждения юридических сроков нет.']}
        write_json(root/'latest.json',package)
        return package
    finally:lock.unlink()
