from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from src.runtime.validation import FIELDS, require

REVISION_EVIDENCE_PATH = Path("data/runtime/approved_historical_revisions.json")
REVISION_ID = "HISTORICAL_SOURCE_REVISION:2026-09-11"
REQUIRED_APPROVAL_STATUS = "APPROVED"
REQUIRED_SOURCE_PROVENANCE = "yahoo_chart"


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def stable_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {"date": row.get("date"), "open": float(row.get("open")), "high": float(row.get("high")),
            "low": float(row.get("low")), "close": float(row.get("close")),
            "volume": float(row.get("volume")), "source": row.get("source")}


def row_hash(row: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical(stable_row(row)).encode("utf-8")).hexdigest()


def load_revision_evidence(root: Path) -> dict:
    path = root / REVISION_EVIDENCE_PATH
    if not path.exists():
        return {"revisions": []}
    return json.loads(path.read_text(encoding="utf-8"))


def approved_entries(evidence: Mapping[str, Any]) -> list[dict]:
    revisions = evidence.get("revisions", [])
    require(isinstance(revisions, list), "REVISION_EVIDENCE_SCHEMA")
    return [entry for entry in revisions if entry.get("revision_id") == REVISION_ID]


def validate_revision_evidence(evidence: Mapping[str, Any]) -> dict:
    entries = approved_entries(evidence)
    checks = []
    for entry in entries:
        affected = entry.get("affected_record", {})
        old_values = affected.get("old_values", {})
        corrected_values = affected.get("corrected_values", {})
        fields = affected.get("fields", [])
        old_row = {"date": affected.get("date"), "source": entry.get("source_provenance"), **old_values}
        new_row = {"date": affected.get("date"), "source": entry.get("source_provenance"), **corrected_values}
        check = {
            "revision_id": entry.get("revision_id") == REVISION_ID,
            "dataset": affected.get("dataset") == "historical_prices",
            "instrument": affected.get("instrument") in {"wti", "brent"},
            "date": affected.get("date") == "2026-09-11",
            "fields": isinstance(fields, list) and bool(fields) and all(field in FIELDS for field in fields),
            "old_values": all(field in old_values for field in FIELDS),
            "corrected_values": all(field in corrected_values for field in FIELDS),
            "before_hash": entry.get("before_hash") == row_hash(old_row),
            "after_hash": entry.get("after_hash") == row_hash(new_row),
            "source_provenance": entry.get("source_provenance") == REQUIRED_SOURCE_PROVENANCE,
            "upstream_revision_timestamp": isinstance(entry.get("upstream_revision_timestamp"), str) and bool(entry.get("upstream_revision_timestamp")),
            "approval_status": entry.get("approval_status") == REQUIRED_APPROVAL_STATUS,
            "revision_reason": isinstance(entry.get("revision_reason"), str) and bool(entry.get("revision_reason")),
        }
        check["status"] = "PASS" if all(check.values()) else "FAIL"
        checks.append({"instrument": affected.get("instrument"), "date": affected.get("date"), "checks": check})
    return {"validation_status": "PASS" if entries and all(item["checks"]["status"] == "PASS" for item in checks) else "FAIL", "revision_id": REVISION_ID, "entries": checks}


def match_approved_revision(*, evidence: Mapping[str, Any], instrument: str, old_row: Mapping[str, Any], incoming_row: Mapping[str, Any]) -> dict | None:
    validation = validate_revision_evidence(evidence)
    if validation["validation_status"] != "PASS":
        return None
    for entry in approved_entries(evidence):
        affected = entry["affected_record"]
        if affected.get("instrument") != instrument or affected.get("date") != old_row.get("date"):
            continue
        if row_hash(old_row) != entry.get("before_hash"):
            return None
        if row_hash(incoming_row) != entry.get("after_hash"):
            return None
        changed = [field for field in FIELDS if float(old_row[field]) != float(incoming_row[field])]
        if sorted(changed) != sorted(affected.get("fields", [])):
            return None
        return {"revision_id": entry["revision_id"], "instrument": instrument, "date": old_row["date"],
                "fields": changed, "before_hash": entry["before_hash"], "after_hash": entry["after_hash"],
                "source_provenance": entry["source_provenance"],
                "upstream_revision_timestamp": entry["upstream_revision_timestamp"],
                "approval_status": entry["approval_status"], "revision_reason": entry["revision_reason"]}
    return None


def revision_summary(accepted: Sequence[Mapping[str, Any]]) -> dict:
    return {"historical_revision_recovery": "PASS" if accepted else "NOT_APPLICABLE",
            "revision_id": REVISION_ID if accepted else None,
            "approved_revision_count": len(accepted),
            "approved_revisions": list(accepted)}
