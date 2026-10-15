"""Fail-closed collection gate before the final offline package acceptance."""
from pathlib import Path

from .archive import selected_message
from .storage import read_json


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
