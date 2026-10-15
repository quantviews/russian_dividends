"""Resumable regulated disclosure archive collection (public search forms only)."""
import argparse
from collections import Counter
from datetime import date, timedelta
import json
import os
from pathlib import Path
import re

from .disclosure import categories, issuer_registry, load_policy, search_form, search_page, parse_message, SEARCH_URL
from .storage import EvidenceStore, canonical, digest, now, read_json, write_json

_RUN_LOCK = None


def selected_message(message):
    return message.get("provider") != "prime" or bool(re.search(
        r"решения (?:совета|общ|единственного)|начислен|выплачен|дивиденд|корректиров|изменении|отмен|дата, на которую", message["title"], re.I))


def lock_provider(root, provider):
    """Serialize network collection per provider across processes; OS releases on crash."""
    global _RUN_LOCK
    path = Path(root) / (provider + ".collection.lock")
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    if handle.tell() == 0:
        handle.write(b"0")
        handle.flush()
    handle.seek(0)
    try:
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        raise RuntimeError("Another collector uses this provider; run index/documents sequentially") from None
    _RUN_LOCK = handle


def build_archive_package(previous, root, directory):
    """Replay saved documents with the current parser; pin discovery and unresolved work."""
    import polars as pl
    from .package import build_package, validate_package, reconcile, detect_conflicts
    from .schema import SCHEMAS
    previous, root, directory = Path(previous), Path(root), Path(directory)
    validate_package(previous)
    store = EvidenceStore(root / "evidence", offline=True)
    tables = {name: pl.read_parquet(previous / (name + ".parquet")).to_dicts() for name in SCHEMAS}
    for source in tables["sources"]:
        raw = store.root / source["raw_path"]
        raw.parent.mkdir(parents=True, exist_ok=True)
        if not raw.exists():
            raw.write_bytes((previous / source["raw_path"]).read_bytes())
        receipt = store.root / "receipts" / (source["source_id"].split(":")[1] + ".json")
        if receipt.exists() and read_json(receipt) != source:
            raise ValueError("Evidence receipt differs from pinned parent")
        if not receipt.exists():
            write_json(receipt, source)
    discovery = read_json(directory / "discovery.json")
    # A shared ISIN can identify an issuer for discovery without proving that a
    # current declaration belongs to a historical registration/additional issue.
    _, historical_unmatched = issuer_registry(previous)
    discovery["unmatched"] = historical_unmatched
    policy, groups = discovery["policy"], discovery["groups"]
    sources = {s["source_id"]: s for s in store.sources()}
    needed = {s["source_id"] for s in tables["sources"]} | set(discovery["source_ids"])
    semantics_sources=[]
    from urllib.parse import unquote, urlsplit
    for registered in tables["sources"]:
        if registered["source_kind"] != "moexutils_splits":continue
        value=unquote(urlsplit(registered["source_url"]).path)
        if len(value)>2 and value[0]=="/" and value[2]==":":value=value[1:]
        semantics=Path(value).parent.parent / "docs" / "data-model.md"
        if semantics.exists():
            source=store.local(semantics,"moexutils_split_semantics")
            sources[source["source_id"]]=source
            needed.add(source["source_id"])
            semantics_sources.append(source["source_id"])
    if (directory / "extra_sources.json").exists():
        needed.update(read_json(directory / "extra_sources.json"))
    from .model import merge_revisions
    events = {e["event_id"]: e for e in tables["events"]}
    incoming = []
    reviews, by_issuer = [], {}
    for path in sorted((directory / "issuers").glob("*.json")):
        issuer = read_json(path)
        needed.update(issuer["source_ids"])
        needed.update(re.findall(r"source:[a-f0-9]{32}", issuer.get("error") or ""))
        by_issuer[issuer["inn"]] = issuer
    for path in sorted((directory / "messages").glob("*.json")):
        result = read_json(path)
        sid = result.get("source_id")
        parsed, review = [], result["review"]
        needed.update(re.findall(r"source:[a-f0-9]{32}", review.get("reason") or ""))
        if sid:
            needed.add(sid)
            source = sources.get(sid) or read_json(store.root / "receipts" / (sid.split(":")[1] + ".json"))
            sources[sid] = source
            try:
                if result["message"].get("provider") == "prime":
                    from .prime import parse
                else:
                    parse = parse_message
                parsed, review = parse((store.root / source["raw_path"]).read_bytes(), source,
                    result["message"], groups[result["inn"]], policy, result["inn"])
            except (ValueError, KeyError) as exc:
                review = dict(status="needs_review", reason=str(exc))
        for event in parsed:
            incoming.append(event)
        reviews.append(dict(inn=result["inn"], source_id=sid, message=result["message"], review=review,
                            event_ids=[e["event_id"] for e in parsed]))
    events = {e["event_id"]: e for e in merge_revisions(list(events.values()), sorted(incoming, key=lambda e:e["observed_at"]))}
    from .disclosure import parse_correction
    pending = [(r, policy) for r in reviews if "correction_requires" in r["review"].get("reason", "") and r["source_id"]]
    cross_archive_reviews = []
    seen_reviews = {r["source_id"] for r in reviews if r["source_id"]}
    # A correction discovered in one archive may name an original that only becomes
    # available when another archive is overlaid. Revisit those pinned review queues.
    previous_reports = sorted((s for s in tables["sources"] if s["source_kind"] == "archive_collection_report"),
                              key=lambda s:s["observed_at"], reverse=True)
    for report_source in previous_reports:
        prior_report = read_json(store.root / report_source["raw_path"])
        prior_policy = prior_report["discovery"]["policy"]
        for old_review in prior_report["reviews"]:
            sid = old_review.get("source_id")
            if not sid or sid in seen_reviews or "correction_requires" not in old_review["review"].get("reason", ""):
                continue
            seen_reviews.add(sid)
            if old_review["inn"] not in groups:
                continue
            review = dict(old_review, reprocessed_from_report=report_source["source_id"])
            cross_archive_reviews.append(review)
            pending.append((review, prior_policy))
    for _ in range(len(pending)):
        progressed = False
        for review, correction_policy in list(pending):
            source = sources[review["source_id"]]
            corrected, outcome = parse_correction((store.root/source["raw_path"]).read_bytes(), source,
                review["message"], groups[review["inn"]], correction_policy, review["inn"], list(events.values()))
            if corrected:
                events.update({e["event_id"]:e for e in corrected})
                review.update(review=outcome,event_ids=[e["event_id"] for e in corrected])
                pending.remove((review, correction_policy))
                progressed = True
        if not progressed:break
    tables["events"] = list(events.values())
    security_inn = {s["security_id"]: inn for inn, securities in groups.items() for s in securities}
    security_inn.update({s["security_id"]:s["search_inn"] for s in historical_unmatched if s.get("search_inn")})
    for row in tables["coverage"]:
        inn = security_inn.get(row["security_id"])
        issuer = by_issuer.get(inn)
        relevant = [e for e in events.values() if e["security_id"] == row["security_id"]]
        related_reviews = [r for r in reviews if r["inn"] == inn] if inn else []
        archive_status = ("identity_unresolved" if not inn else "not_checked" if not issuer else issuer["status"])
        downloaded = {(r["message"]["agency"],r["message"]["message_id"]) for r in related_reviews if r["source_id"]}
        selected = [m for m in issuer["messages"] if selected_message(m)] if issuer else []
        missing_docs = sum((m["agency"],m["message_id"]) not in downloaded for m in selected) if issuer else None
        note = dict(source=read_json(directory / "settings.json").get("provider", "skrin")+"_regulated_archive", status=archive_status,
            documents_indexed=len(issuer["messages"]) if issuer else None, documents_pending=missing_docs,
            documents_not_selected=len(issuer["messages"])-len(selected) if issuer else None,
            review_counts=dict(Counter(r["review"]["status"] for r in related_reviews)),
            error=issuer.get("error") if issuer else "No unique verified issuer mapping" if not inn else None,
            history_scope=issuer.get("history_scope", "rolling_year_search_not_full_requested_interval") if issuer else None)
        identity_note = next((s["reason"] for s in historical_unmatched if s["security_id"] == row["security_id"]), None)
        if identity_note:
            note["identity_gap"] = identity_note
        note["available_date_kind"] = ("publication_date" if any(e.get("published_at") for e in relevant)
            else "record_date_only" if row.get("available_from") else "none")
        statuses = json.loads(row.get("source_statuses_json") or "{}")
        statuses[note["source"]] = archive_status
        row["source_statuses_json"] = json.dumps(statuses)
        verified = [e for e in relevant if e["verification_status"] == "verified"]
        required = ("published_at", "edition_evidence", "actual_payment_date", "share_basis_date", "record_date")
        missing = {field:sum(e.get(field) is None for e in verified) for field in required}
        note["missing_field_counts"] = missing
        note["unresolved_episode_links"] = sum(
            (e.get("edition_evidence") or "").startswith("{") and
            json.loads(e["edition_evidence"]).get("episode_linkage", {}).get("basis") == "unresolved_document"
            for e in verified)
        row["missing_fields_json"] = json.dumps([field for field in required if not verified or missing[field]])
        if note["unresolved_episode_links"]:
            row["missing_fields_json"] = json.dumps(json.loads(row["missing_fields_json"]) + ["dividend_episode_link"])
        reasons = json.loads(row.get("reasons_json") or "[]")
        reasons.append(note)
        row["reasons_json"] = json.dumps(reasons, ensure_ascii=False)
        if any(e["verification_status"] == "verified" for e in relevant):
            row["status"] = "partial"
        elif issuer and issuer["status"] == "indexed":
            row["status"] = ("no_events_found" if not issuer["messages"] and issuer.get("provider") == "prime" and row["status"] != "partial"
                             else "partial")
        row["event_counts_json"] = json.dumps(dict(Counter(e["event_type"] for e in relevant)))
        ids = set(json.loads(row.get("source_ids_json") or "[]")) | {e["source_id"] for e in relevant}
        if issuer:
            ids.update(issuer["source_ids"])
            row["checked_at"] = issuer["checked_at"]
        row["source_ids_json"] = json.dumps(sorted(ids))
        dates = sorted(e["published_at"][:10] for e in relevant if e.get("published_at"))
        if dates:
            row["available_from"], row["available_to"] = dates[0], dates[-1]
    report = dict(settings=read_json(directory / "settings.json"), discovery=discovery,
        issuers=list(by_issuer.values()), reviews=reviews, cross_archive_corrections=cross_archive_reviews,
        pending_issuers=sorted(set(groups) - set(by_issuer)),
        indexed_messages=sum(len(i["messages"]) for i in by_issuer.values()), downloaded_messages=sum(r["source_id"] is not None for r in reviews),
        inspected_or_classified_messages=len(reviews),
        selection_policy="Standard Russian dividend decision/payment/correction categories; nonstandard categories remain uninspected",
        review_counts=dict(Counter(r["review"]["status"] for r in reviews)))
    report_source = store.save(canonical(report), "urn:dividends:archive-report:" + digest(canonical(report)), "archive_collection_report")
    needed.add(report_source["source_id"])
    sources[report_source["source_id"]] = report_source
    tables["sources"] = [sources.get(sid) or read_json(store.root / "receipts" / (sid.split(":")[1] + ".json")) for sid in sorted(needed)]
    tables["conflicts"] = detect_conflicts(tables["events"], tables["corporate_actions"])
    tables["reconciliation"] = reconcile(tables["securities"], tables["sources"], tables["events"], store)
    old = read_json(previous / "manifest.json")
    metadata = {k: old[k] for k in ("requested_from", "requested_to", "snapshot_at", "universe_sha256")}
    published = sorted(e["published_at"] for e in tables["events"] if e.get("published_at"))
    record_dates = sorted(e["record_date"] for e in tables["events"] if e.get("record_date"))
    metadata.update(parent_package=str(previous.resolve()), archive_report_source_id=report_source["source_id"],
        archive_report_source_ids=sorted(s["source_id"] for s in tables["sources"] if s["source_kind"] == "archive_collection_report"),
        actual_publication_from=published[0] if published else None, actual_publication_to=published[-1] if published else None,
        actual_record_date_from=record_dates[0] if record_dates else None, actual_record_date_to=record_dates[-1] if record_dates else None,
        corporate_action_semantics_source_ids=semantics_sources,
        archive_publication_policy="Regulated news archive contracts pinned per event; mutable attachments excluded",
        limitations=["Coverage gaps and parser review queue are pinned in archive_collection_report",
            "No events found is not evidence of no dividend; identity-unresolved issues remain in the universe",
            "Unknown original share basis is never silently normalized",
            "Archive contract evidence is not a contemporaneous cryptographic timestamp"])
    target = build_package(root / "packages", tables, store, metadata, Path.cwd())
    write_json(directory / "result.json", dict(package=str(target), validation=validate_package(target),
        archive_report_source_id=report_source["source_id"]))
    print(json.dumps(dict(package=str(target), summary=read_json(target / "summary.json")), ensure_ascii=False, indent=2))
    return target


def collect_index(store, directory, groups, selected, start, end, provider="skrin"):
    """Freeze each issuer's complete search result before downloading documents."""
    directory = Path(directory)
    for index, (inn, securities) in enumerate(sorted(groups.items()), 1):
        target = directory / "issuers" / (inn + ".json")
        if target.exists() and read_json(target).get("status") == "indexed" and read_json(target).get("pagination_version") == 3:
            continue
        messages, sources, requests = {}, set(), []
        try:
            intervals = [(start, end)]
            while intervals:
                first, last = intervals.pop()
                page, total, seen_pages, expected_count = 1, 1, set(), None
                interval_ids = set()
                while page <= total:
                    if provider == "prime":
                        from . import prime
                        form = prime.search_form(selected, inn, first, last)
                        if expected_count is not None:
                            form["ctl00$Main$StoredNewsCount"] = str(expected_count)
                        url = prime.SEARCH_URL + ("?p=" + str(page-1) if page > 1 else "")
                        body, source = store.fetch(url, "prime_search", form=form)
                        found, total, count = prime.search_page(body, inn)
                        if page == 1:
                            expected_count = count
                        elif count not in {0, expected_count}:
                            raise ValueError("PRIME pagination count changed")
                        # The legacy page loses its displayed count on postback but still
                        # returns the requested offset. Freeze the first-page count and
                        # verify row counts and unique message IDs on every subsequent page.
                        total = (expected_count + 29)//30 or 1
                        expected_rows = min(30, max(0, expected_count - (page-1)*30))
                        if len(found) != expected_rows:
                            raise ValueError("PRIME page is incomplete")
                    else:
                        form = search_form(inn, first, last, selected, page)
                        body, source = store.fetch(SEARCH_URL, "disclosure_search", form=form)
                        found, total = search_page(body, inn)
                    sources.add(source["source_id"])
                    requests.append(dict(form=form, source_id=source["source_id"]))
                    if total >= 99:
                        a, b = date.fromisoformat(first), date.fromisoformat(last)
                        if a == b:
                            raise ValueError("Daily search exceeds archive pagination limit")
                        mid = a + (b-a)//2
                        intervals.extend([(first, mid.isoformat()), ((mid+timedelta(days=1)).isoformat(), last)])
                        break
                    fingerprint = digest(canonical([(m["agency"], m["message_id"]) for m in found]))
                    if fingerprint in seen_pages or (not found and total > 1):
                        raise ValueError("Repeated/empty paginated result")
                    seen_pages.add(fingerprint)
                    for message in found:
                        key=(message["agency"], message["message_id"])
                        if key in interval_ids:
                            raise ValueError("Overlapping search pages: index changed during collection")
                        interval_ids.add(key)
                        # Guard against silently ignored date filters.
                        stamp = message["listing_text"][:10]
                        from .official import russian_date
                        day = russian_date(stamp)
                        if not day or not first <= day <= last:
                            raise ValueError("Archive ignored date filter: " + stamp)
                        messages[(message["agency"], message["message_id"])] = message
                    page += 1
                if provider == "prime" and total < 99 and len(interval_ids) != expected_count:
                    raise ValueError("PRIME unique result count mismatch")
            status, error = "indexed", None
        except Exception as exc:
            status, error = "source_unavailable", str(exc)
        write_json(target, dict(inn=inn, securities=[s["security_id"] for s in securities],
            status=status, error=error, checked_at=now(), source_ids=sorted(sources), requests=requests,
            messages=list(messages.values()), provider=provider, pagination_version=3,
            history_scope="rolling_year_search_not_full_requested_interval" if provider == "skrin" else "requested_interval"))
        print(f"index {index}/{len(groups)} {inn}: {status}, {len(messages)} messages", flush=True)


def collect_documents(store, directory, groups, policy, workers=2):
    from concurrent.futures import ThreadPoolExecutor
    directory = Path(directory)
    issuers = [read_json(p) for p in sorted((directory / "issuers").glob("*.json"))]
    # Round robin avoids allowing one large bank's coupon disclosures to starve other issuers.
    pending = [(issuer, message) for position in range(max((len(i["messages"]) for i in issuers), default=0))
               for issuer in issuers for message in issuer["messages"][position:position+1]]
    def collect_one(item):
        issuer, message = item
        target = directory / "messages" / (message["agency"] + "-" + message["message_id"] + ".json")
        if target.exists() and read_json(target).get("source_id"):
            return None
        result = dict(inn=issuer["inn"], message=message, checked_at=now())
        if not selected_message(message):
            result.update(events=[], review=dict(status="out_of_scope_title", reason="nonstandard_category_content_not_inspected"))
            write_json(target, result)
            return None
        try:
            body, source = store.fetch(message["url"], "disclosure_message")
            result["source_id"] = source["source_id"]
            if message.get("provider") == "prime":
                from .prime import parse
            else:
                parse = parse_message
            events, review = parse(body, source, message, groups[issuer["inn"]], policy, issuer["inn"])
            result.update(events=events, review=review)
        except Exception as exc:
            result.update(events=[], review=dict(status="needs_review" if result.get("source_id") else "source_unavailable", reason=str(exc)))
        write_json(target, result)
        return result
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for index, result in enumerate(pool.map(collect_one, pending), 1):
            if result and (index % 20 == 0 or result.get("events")):
                print(f"document {index}/{len(pending)}: {result['review']['status']}, {len(result['events'])} events", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--previous", required=True)
    parser.add_argument("--root", default="artifacts/dividend-events")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--start", default="2022-01-01", help="Includes preceding decision history")
    parser.add_argument("--end", default=date.today().isoformat())
    parser.add_argument("--phase", choices=["index", "documents", "build", "all"], default="all")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--refresh", action="store_true", help="Re-fetch source versions in a new run directory")
    parser.add_argument("--delay", type=float, default=2)
    parser.add_argument("--timeout", type=float, default=25)
    parser.add_argument("--provider", choices=["skrin", "prime"], default="skrin")
    parser.add_argument("--workers", type=int, choices=range(1,5), default=2, help="Document workers share one global request pause")
    args = parser.parse_args()
    if not args.run_id or Path(args.run_id).name != args.run_id or args.run_id in {".", ".."}:
        raise ValueError("run-id must be one directory name")
    date.fromisoformat(args.start)
    date.fromisoformat(args.end)
    if args.start > args.end or args.delay < 0 or args.timeout <= 0:
        raise ValueError("Invalid interval or HTTP settings")
    root = Path(args.root)
    directory = root / "archive-runs" / args.run_id
    if args.phase == "build":
        build_archive_package(args.previous, root, directory)
        return
    lock_provider(root, args.provider)
    settings = dict(previous=str(Path(args.previous).resolve()), start=args.start, end=args.end)
    if args.refresh:
        settings["refresh"] = True
    if args.provider != "skrin":
        settings["provider"] = args.provider
    if (directory / "settings.json").exists() and read_json(directory / "settings.json") != settings:
        raise ValueError("Run settings changed; choose a new run-id")
    write_json(directory / "settings.json", settings)
    store = EvidenceStore(root / "evidence", delay=args.delay, timeout=args.timeout, retries=1, offline=args.offline, refresh=args.refresh)
    if args.provider == "prime":
        from . import prime
        policy = prime.load_policy(store)
        selected, seed_source = prime.seed_form(store)
        sources = [seed_source["source_id"]]
    else:
        policy = load_policy(store)
        selected, sources = categories(store)
    groups, unmatched = issuer_registry(args.previous)
    write_json(directory / "discovery.json", dict(groups=groups, unmatched=unmatched, categories=selected,
        source_ids=sources + [policy["source_id"]], policy=policy))
    if args.phase in {"index", "all"}:
        collect_index(store, directory, groups, selected, args.start, args.end, args.provider)
    if args.phase in {"documents", "all"}:
        collect_documents(store, directory, groups, policy, args.workers)
    if args.phase == "all":
        build_archive_package(args.previous, root, directory)
    print(json.dumps(dict(issuers=len(groups), unmatched=len(unmatched), run=str(directory))))


if __name__ == "__main__":
    main()
