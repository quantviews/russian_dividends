from issuer_messages.collector import parse_volsta,parse_issuer_feed,news_links
import pytest
E={'source_id':'source:test','raw_path':'raw/test.bin'}

def test_wrong_issuer_is_not_accepted():
    html='<h1>Сообщения представителя</h1><table><tr><td>01.10.2026</td><td>01.10.2026 12:00</td><td>RU000A107YT9</td><td><a href="#x">Ответ</a></td></tr></table><div id="x">Другой эмитент ИНН 0000000000</div>'
    with pytest.raises(ValueError,match='identity'):parse_volsta(html,E)

def test_primary_record_preserves_date_identity_and_evidence():
    html='<h1>Сообщения представителя</h1><table><tr><td>01.10.2026</td><td>01.10.2026 12:00</td><td>RU000A107YT9</td><td><a href="#x">Ответ</a></td></tr></table><div id="x">ИНН 6445005149 RU000A107YT9 А57-25023/2026 Арест не снят</div>'
    r=parse_volsta(html,E)[0]
    assert r['published_at']=='2026-10-01T12:00:00+03:00'
    assert r['case_numbers']==['А57-25023/2026']
    assert r['deadlines_verified']==[]
    assert r['evidence_id']=='source:test'

def test_empty_page_is_failure_not_no_events():
    with pytest.raises(ValueError):parse_volsta('<html>Captcha</html>',E)
    with pytest.raises(ValueError):news_links('<html>unavailable</html>')

def test_feed_rejects_foreign_article_url():
    xml='<rss><channel><item><link>https://other.example/news</link></item></channel></rss>'
    with pytest.raises(ValueError,match='host'):parse_issuer_feed(xml,E)
