from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from src.runtime.source import IntegrityError
from src.runtime.validation import PUBLIC_FILES, read_json

CONTRACT_VERSION = "OIS-PRODUCTION-BUNDLE-MANIFEST-V1"
REQUIRED_PREVIOUS_STATE_REQUIREMENTS = {"REQUIRED", "NOT_REQUIRED_INITIAL_STATE"}
REQUIRED_TOP_LEVEL_FIELDS = {
    "system",
    "version",
    "cadence",
    "run_id",
    "event",
    "production_snapshot_id",
    "commit_sha",
    "generated_at",
    "market_date",
    "snapshot_type",
    "validation",
    "freshness",
    "previous_state_requirement",
    "blocked_dependencies",
    "payload_references",
}


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception as exc:
        raise IntegrityError("INVALID_TIMESTAMP") from exc
    if parsed.tzinfo is None:
        raise IntegrityError("TIMESTAMP_REQUIRES_TIMEZONE")
    return parsed


def _load_public_documents(production_dir: Path) -> dict[str, dict]:
    documents = {}
    for filename in PUBLIC_FILES:
        path = production_dir / filename
        if not path.is_file():
            raise IntegrityError(f"MISSING_ARTIFACT:{filename}")
        documents[filename] = read_json(path)
    return documents


def _common_binding(documents: Mapping[str, Mapping[str, object]]) -> dict:
    status = documents["ois_status.json"]
    return {
        "production_snapshot_id": status.get("production_snapshot_id"),
        "run_id": status.get("run_id"),
        "commit_sha": status.get("commit_sha"),
        "source_as_of": status.get("source_as_of") or status.get("data_as_of"),
        "generated_at": status.get("generated_at"),
        "validation_status": status.get("validation_status"),
        "freshness_status": status.get("freshness_status"),
    }


def build_shadow_manifest(
    *,
    production_dir: str | Path,
    cadence: str,
    event: str,
    market_date: str,
    generated_at: str | None = None,
    previous_state_requirement: str = "REQUIRED",
    blocked_dependencies: list[str] | None = None,
    snapshot_type: str = "SHADOW_PRODUCTION_BUNDLE",
) -> dict:
    production_path = Path(production_dir)
    documents = _load_public_documents(production_path)
    binding = _common_binding(documents)
    payload_references = []
    for filename in PUBLIC_FILES:
        doc = documents[filename]
        for key in ("production_snapshot_id", "run_id", "commit_sha", "validation_status", "freshness_status"):
            if doc.get(key) != binding.get(key):
                raise IntegrityError(f"CROSS_FILE_BINDING:{filename}:{key}")
        payload_references.append({
            "path": (production_path / filename).as_posix(),
            "sha256": _sha256(production_path / filename),
            "schema_version": doc.get("schema_version"),
            "production_snapshot_id": doc.get("production_snapshot_id"),
            "run_id": doc.get("run_id"),
            "commit_sha": doc.get("commit_sha"),
        })
    manifest = {
        "system": "OIS",
        "version": CONTRACT_VERSION,
        "cadence": cadence,
        "run_id": binding["run_id"],
        "event": event,
        "production_snapshot_id": binding["production_snapshot_id"],
        "commit_sha": binding["commit_sha"],
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "market_date": market_date,
        "snapshot_type": snapshot_type,
        "validation": {
            "status": binding["validation_status"],
            "source": "bound_public_artifacts",
        },
        "freshness": {
            "status": binding["freshness_status"],
            "source_as_of": binding["source_as_of"],
            "artifact_generated_at": binding["generated_at"],
        },
        "previous_state_requirement": previous_state_requirement,
        "blocked_dependencies": sorted(set(blocked_dependencies or [])),
        "payload_references": payload_references,
    }
    manifest["manifest_sha256"] = hashlib.sha256(_canonical_bytes({k: v for k, v in manifest.items() if k != "manifest_sha256"})).hexdigest()
    return manifest


def validate_shadow_manifest(
    manifest: Mapping[str, object],
    *,
    root: str | Path = ".",
    expected_run_id: str | None = None,
    expected_commit_sha: str | None = None,
    expected_production_snapshot_id: str | None = None,
    now: datetime | None = None,
) -> dict:
    errors: list[str] = []
    root_path = Path(root)
    now = now or datetime.now(timezone.utc)
    errors.extend(f"MISSING_FIELD:{field}" for field in sorted(REQUIRED_TOP_LEVEL_FIELDS - set(manifest)))
    if manifest.get("version") != CONTRACT_VERSION:
        errors.append("CONTRACT_VERSION_MISMATCH")
    if expected_run_id is not None and manifest.get("run_id") != expected_run_id:
        errors.append("RUN_ID_MISMATCH")
    if expected_commit_sha is not None and manifest.get("commit_sha") != expected_commit_sha:
        errors.append("COMMIT_MISMATCH")
    if expected_production_snapshot_id is not None and manifest.get("production_snapshot_id") != expected_production_snapshot_id:
        errors.append("PRODUCTION_SNAPSHOT_BINDING_MISMATCH")
    if manifest.get("previous_state_requirement") not in REQUIRED_PREVIOUS_STATE_REQUIREMENTS:
        errors.append("INVALID_PREVIOUS_STATE_REQUIREMENT")
    try:
        generated_at = _timestamp(str(manifest.get("generated_at", "")))
        if generated_at > now:
            errors.append("FUTURE_DATED_ARTIFACT")
        if (now - generated_at).total_seconds() > 36 * 60 * 60:
            errors.append("STALE_ARTIFACT")
    except IntegrityError as exc:
        errors.append(str(exc))
    validation = manifest.get("validation", {})
    if not isinstance(validation, dict) or validation.get("status") != "PASS":
        errors.append("VALIDATION_FAIL")
    freshness = manifest.get("freshness", {})
    if not isinstance(freshness, dict) or freshness.get("status") != "PASS":
        errors.append("FRESHNESS_FAIL")
    if manifest.get("blocked_dependencies"):
        errors.append("BLOCKED_DEPENDENCIES_PRESENT")
    references = manifest.get("payload_references", [])
    if not isinstance(references, list) or {Path(str(ref.get("path", ""))).name for ref in references if isinstance(ref, dict)} != set(PUBLIC_FILES):
        errors.append("PUBLIC_ARTIFACT_SET_MISMATCH")
    for reference in references if isinstance(references, list) else []:
        if not isinstance(reference, dict):
            errors.append("INVALID_PAYLOAD_REFERENCE")
            continue
        path = root_path / str(reference.get("path", ""))
        if not path.is_file():
            errors.append(f"MISSING_ARTIFACT:{reference.get('path')}")
            continue
        if reference.get("sha256") != _sha256(path):
            errors.append(f"PAYLOAD_REFERENCE_HASH_MISMATCH:{reference.get('path')}")
        doc = read_json(path)
        for key in ("production_snapshot_id", "run_id", "commit_sha"):
            if reference.get(key) != doc.get(key) or manifest.get(key) != doc.get(key):
                errors.append(f"PAYLOAD_REFERENCE_BINDING_MISMATCH:{Path(str(reference.get('path'))).name}:{key}")
    expected_hash = manifest.get("manifest_sha256")
    actual_hash = hashlib.sha256(_canonical_bytes({k: v for k, v in manifest.items() if k != "manifest_sha256"})).hexdigest()
    if expected_hash != actual_hash:
        errors.append("CORRUPTED_MANIFEST")
    status = "PASS" if not errors else "FAIL_CLOSED"
    return {"validation_status": status, "errors": errors, "state_mutation_allowed": False, "portfolio_mutation_allowed": False, "ledger_mutation_allowed": False}
