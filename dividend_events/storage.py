"""Content-addressed evidence, safe HTTP receipts, and resumable cached retrieval."""

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
import threading
from urllib.parse import urlsplit

import requests

from .schema import PARSER_VERSION


def now():
    return datetime.now(timezone.utc).isoformat()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def identifier(prefix, value):
    return prefix + ":" + digest(canonical(value))[:32]


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_bytes(canonical(value))
    temp.replace(path)


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


class EvidenceStore:
    def __init__(self, root, delay=2.0, timeout=25, retries=2, offline=False, refresh=False):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.delay, self.timeout, self.retries = delay, timeout, retries
        self.offline, self.refresh = offline, refresh
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "dividends-evidence/1.0 (public-data archival collector)"
        self.last_request = 0.0
        self.host_requests = {}
        self.host_failures = {}
        self.host_cooldowns = {}
        self.request_lock = threading.Lock()
        self.health_lock = threading.Lock()
        self.main_thread = threading.get_ident()
        self.thread_sessions = threading.local()

    def request_session(self):
        if threading.get_ident() == self.main_thread:
            return self.session
        if not hasattr(self.thread_sessions, "session"):
            self.thread_sessions.session = requests.Session()
            self.thread_sessions.session.headers.update(self.session.headers)
        return self.thread_sessions.session

    def wait_for_host(self, host):
        # A circuit pause is not a failed request for every remaining document.
        # Keep checkpoints resumable without racing through the entire queue.
        while (remaining := self.host_cooldowns.get(host, 0) - time.monotonic()) > 0:
            time.sleep(min(remaining, 1.0))

    def save(self, body, url, kind, status=None, headers=None, observed_at=None):
        sha = digest(body)
        relative = f"raw/{sha[:2]}/{sha}.bin"
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            with path.open("xb") as f:
                f.write(body)
        elif digest(path.read_bytes()) != sha:
            raise ValueError(f"Corrupt evidence: {path}")
        # One receipt per URL/content version; repeated retrieval preserves first observation.
        sid = identifier("source", [url, sha])
        receipt = self.root / "receipts" / (sid.split(":")[1] + ".json")
        if receipt.exists():
            return read_json(receipt)
        allowed = {k.lower(): v for k, v in (headers or {}).items()
                   if k.lower() in {"content-type", "last-modified", "etag", "date", "content-length"}}
        source = dict(source_id=sid, source_url=url, document_id=sha, raw_path=relative,
                      sha256=sha, observed_at=observed_at or now(),
                      content_type=allowed.get("content-type"), http_status=str(status) if status else None,
                      http_metadata_json=json.dumps(allowed), source_kind=kind, parser_version=PARSER_VERSION)
        write_json(receipt, source)
        return source

    def fetch(self, url, kind, *, form=None):
        parsed = urlsplit(url)
        if parsed.scheme not in {"https", "http"} or parsed.username or parsed.password:
            raise ValueError("Only public HTTP(S) URLs without credentials are supported")
        receipt_url = url if form is None else url + "#public-form=" + digest(canonical(form))
        cache = self.root / "cache" / (digest(receipt_url.encode()) + ".json")
        if cache.exists() and (not self.refresh or self.offline):
            source = read_json(cache)
            body = (self.root / source["raw_path"]).read_bytes()
            if digest(body) != source["sha256"]:
                raise ValueError("Cache hash mismatch")
            if source.get("http_status") and int(source["http_status"]) >= 400:
                raise PermissionError(f"source_unavailable: cached HTTP {source['http_status']}; {source['source_id']}")
            return body, source
        if self.offline:
            raise RuntimeError("offline_cache_miss")
        self.wait_for_host(parsed.hostname)
        error = None
        session = self.request_session()
        for attempt in range(self.retries + 1):
            host = parsed.hostname
            host_delay = max(self.delay, 2.0) if host == "xn--80aeiahhn9aobclif2kuc.xn--p1ai" else self.delay
            with self.request_lock:
                self.wait_for_host(host)
                time.sleep(max(0, self.delay - (time.monotonic() - self.last_request),
                               host_delay - (time.monotonic() - self.host_requests.get(host, 0))))
                self.last_request = time.monotonic()
                self.host_requests[host] = self.last_request
            try:
                session.cookies.clear()
                response = (session.get(url, timeout=self.timeout) if form is None else
                            session.post(url, data=form, timeout=self.timeout))
                source = self.save(response.content, receipt_url, kind, response.status_code, response.headers)
                session.cookies.clear()
                if response.status_code == 503:
                    with self.health_lock:
                        self.host_failures[host] = self.host_failures.get(host, 0) + 1
                        if self.host_failures[host] >= 3:
                            self.host_cooldowns[host] = time.monotonic() + 120
                            raise RuntimeError("source_temporarily_unavailable: repeated HTTP 503; " + source["source_id"])
                elif response.status_code < 400:
                    self.host_failures[host] = 0
                if response.status_code in {401, 403, 429}:
                    # Do not retry access restrictions or rate limits.
                    write_json(cache, source)
                    raise PermissionError(f"source_unavailable: HTTP {response.status_code}; {source['source_id']}")
                if response.status_code >= 500 and attempt < self.retries:
                    error = RuntimeError(f"HTTP {response.status_code}")
                    time.sleep(2 ** attempt)
                    continue
                if response.status_code >= 400:
                    raise RuntimeError(f"source_unavailable: HTTP {response.status_code}; {source['source_id']}")
                write_json(cache, source)
                return response.content, source
            except (requests.Timeout, requests.ConnectionError) as exc:
                error = exc
                if attempt < self.retries:
                    time.sleep(2 ** attempt)
        raise RuntimeError(str(error))

    def local(self, path, kind):
        path = Path(path).resolve()
        return self.save(path.read_bytes(), path.as_uri(), kind)

    def sources(self):
        return [read_json(p) for p in sorted((self.root / "receipts").glob("*.json"))]
