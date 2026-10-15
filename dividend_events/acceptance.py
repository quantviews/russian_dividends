"""Fail-closed collection gate before the final offline package acceptance."""
from pathlib import Path

from .archive import selected_message
from .storage import read_json, write_json, digest, canonical, now


def retry_timeouts(directory, store):
    """One bounded retry pass, under the caller's exclusive provider lock."""
    directory = Path(directory)
    discovery = read_json(directory / "discovery.json")
    counts = dict(selected=0, recovered=0, unavailable=0)
    for path in sorted((directory / "messages").glob("*.json")):
        previous = read_json(path)
        review = previous.get("review", {})
        if (previous.get("source_id") or review.get("status") != "source_unavailable"
                or "timed out" not in review.get("reason", "").lower()):
            continue
        counts["selected"] += 1
        # Preserve the original failed attempt even if recovery succeeds.
        backup = directory / "retry-history" / (digest(canonical(previous)) + ".json")
        if not backup.exists():
            write_json(backup, previous)
        message = previous["message"]
        result = dict(inn=previous["inn"], message=message, checked_at=now())
        try:
            body, source = store.fetch(message["url"], "disclosure_message")
            result["source_id"] = source["source_id"]
            if message.get("provider") == "prime":
                from .prime import parse
            else:
                from .disclosure import parse_message as parse
            events, review = parse(body, source, message,
                discovery["groups"][previous["inn"]], discovery["policy"], previous["inn"])
            result.update(events=events, review=review)
        except Exception as exc:
            result.update(events=[], review=dict(
                status="needs_review" if result.get("source_id") else "source_unavailable", reason=str(exc)))
        counts["recovered" if result.get("source_id") else "unavailable"] += 1
        write_json(path, result)
    write_json(directory / "timeout-retry-summary.json", dict(checked_at=now(), **counts))
    return counts


def audit_collection(directory):
    directory = Path(directory)
    discovery = read_json(directory / "discovery.json")
    issuers = {p.stem: read_json(p) for p in (directory / "issuers").glob("*.json")}
    if set(discovery["groups"]) != set(issuers):
        raise ValueError(f"Unattempted issuer searches in {directory.name}")
    indexed = selected = obtained = unavailable = 0
    for inn, issuer in issuers.items():
        if issuer.get("inn") != inn:
            raise ValueError("Issuer checkpoint identity mismatch")
        if "host circuit open" in (issuer.get("error") or ""):
            raise ValueError(f"Issuer search was skipped during outage: {inn}")
        for message in issuer["messages"]:
            indexed += 1
            if not selected_message(message):
                continue
            selected += 1
            checkpoint = directory / "messages" / (message["agency"] + "-" + message["message_id"] + ".json")
            if not checkpoint.exists():
                raise ValueError(f"Unattempted document: {checkpoint}")
            result = read_json(checkpoint)
            if result.get("inn") != inn or result.get("message") != message:
                raise ValueError(f"Document checkpoint identity mismatch: {checkpoint}")
            if result.get("source_id"):
                obtained += 1
            elif (result.get("review", {}).get("status") == "source_unavailable"
                  and result.get("checked_at") and result["review"].get("reason")
                  and "host circuit open" not in result["review"]["reason"]):
                unavailable += 1
            else:
                raise ValueError(f"Document was not retrieved or explicitly attempted: {checkpoint}")
    return dict(issuers=len(issuers), indexed_messages=indexed,
                selected_documents=selected, downloaded_documents=obtained,
                unavailable_documents=unavailable,
                unavailable_issuer_searches=sum(i["status"] != "indexed" for i in issuers.values()))
