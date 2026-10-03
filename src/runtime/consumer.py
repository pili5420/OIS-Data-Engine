from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Mapping

from src.runtime.shadow_manifest import CONTRACT_VERSION, validate_shadow_manifest
from src.runtime.validation import PUBLIC_FILES

PHASE_A_CONSUMER_CONTRACT = "OIS-PRODUCTION-BUNDLE-CONSUMER-PHASE-A-V1"
ALLOWED_PREVIOUS_STATE_REQUIREMENTS = {"REQUIRED", "NOT_REQUIRED_INITIAL_STATE"}
NO_RECALCULATION_TARGETS = (
    "WTI",
    "Brent",
    "MA20",
    "MA60",
    "MA120",
    "MACD",
    "RSI14",
    "rolling-180",
)


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _read_json_file(path: str | Path, error_code: str) -> tuple[dict | None, list[str]]:
    target = Path(path)
    if not target.is_file():
        return None, [error_code]
    try:
        loaded = json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return None, [error_code.replace("MISSING", "CORRUPTED")]
    if not isinstance(loaded, dict):
        return None, [error_code.replace("MISSING", "CORRUPTED")]
    return loaded, []


def _previous_state_id(previous_state: Mapping[str, object] | None) -> str | None:
    if previous_state is None:
        return None
    for key in ("current_state_id", "state_id"):
        if previous_state.get(key):
            return str(previous_state[key])
    return None


def _gate(status: bool, errors: list[str]) -> dict:
    return {"status": "PASS" if status else "FAIL_CLOSED", "errors": errors}


def build_phase_a_consumer_evidence(
    *,
    manifest_path: str | Path,
    previous_state_path: str | Path | None,
    root: str | Path = ".",
    expected_run_id: str | None = None,
    expected_commit_sha: str | None = None,
    expected_production_snapshot_id: str | None = None,
    expected_previous_state_id: str | None = None,
    render_preview_status: str = "PASS",
    now=None,
) -> dict:
    manifest, manifest_errors = _read_json_file(manifest_path, "MISSING_MANIFEST")
    previous_state, previous_errors = (None, ["MISSING_PREVIOUS_STATE"]) if previous_state_path is None else _read_json_file(previous_state_path, "MISSING_PREVIOUS_STATE")

    manifest_validation = {"validation_status": "FAIL_CLOSED", "errors": manifest_errors}
    if manifest is not None:
        manifest_validation = validate_shadow_manifest(
            manifest,
            root=root,
            expected_run_id=expected_run_id,
            expected_commit_sha=expected_commit_sha,
            expected_production_snapshot_id=expected_production_snapshot_id,
            now=now,
        )

    errors = list(manifest_validation.get("errors", [])) + previous_errors
    previous_requirement = manifest.get("previous_state_requirement") if manifest else None
    previous_id = _previous_state_id(previous_state)
    if previous_requirement not in ALLOWED_PREVIOUS_STATE_REQUIREMENTS:
        errors.append("INVALID_PREVIOUS_STATE_REQUIREMENT")
    if previous_requirement == "REQUIRED" and not previous_id:
        errors.append("MISSING_PREVIOUS_STATE_ID")
    bound_previous_id = manifest.get("previous_state_id") if isinstance(manifest, dict) else None
    if bound_previous_id is not None and previous_id != bound_previous_id:
        errors.append("PREVIOUS_STATE_ID_MISMATCH")
    if expected_previous_state_id is not None and previous_id != expected_previous_state_id:
        errors.append("PREVIOUS_STATE_ID_MISMATCH")

    reference_names = set()
    if isinstance(manifest, dict) and isinstance(manifest.get("payload_references"), list):
        reference_names = {Path(str(item.get("path", ""))).name for item in manifest["payload_references"] if isinstance(item, dict)}
    if reference_names != set(PUBLIC_FILES):
        errors.append("FOUR_ARTIFACT_BINDING_MISMATCH")

    data_errors = sorted(set(errors))
    data_pass = not data_errors
    render_errors = [] if render_preview_status == "PASS" else ["RENDER_GATE_FAIL"]
    render_pass = data_pass and not render_errors
    status = "PASS" if data_pass and render_pass else "FAIL_CLOSED"
    production_snapshot_id = manifest.get("production_snapshot_id") if manifest else None
    proposed_payload = {
        "contract": PHASE_A_CONSUMER_CONTRACT,
        "production_snapshot_id": production_snapshot_id,
        "previous_state_id": previous_id,
        "run_id": manifest.get("run_id") if manifest else None,
        "commit_sha": manifest.get("commit_sha") if manifest else None,
    }
    proposed_current_state_id = "ois-shadow-preview-" + hashlib.sha256(_canonical_bytes(proposed_payload)).hexdigest()[:24] if data_pass else None
    fail_closed_reason = sorted(set(data_errors + render_errors))

    return {
        "system": "OIS",
        "consumer_contract": PHASE_A_CONSUMER_CONTRACT,
        "manifest_version": manifest.get("version") if manifest else None,
        "cadence": manifest.get("cadence") if manifest else None,
        "run_id": manifest.get("run_id") if manifest else None,
        "production_snapshot_id": production_snapshot_id,
        "commit_sha": manifest.get("commit_sha") if manifest else None,
        "manifest_validation": manifest_validation,
        "previous_state_id": previous_id,
        "proposed_current_state_id": proposed_current_state_id,
        "four_artifact_binding": _gate("PUBLIC_ARTIFACT_SET_MISMATCH" not in data_errors and "FOUR_ARTIFACT_BINDING_MISMATCH" not in data_errors and not any("MISSING_ARTIFACT" in error or "BINDING_MISMATCH" in error for error in data_errors), fail_closed_reason),
        "data_gate": _gate(data_pass, data_errors),
        "render_gate": _gate(render_pass, render_errors),
        "validation_gate": _gate("VALIDATION_FAIL" not in data_errors, fail_closed_reason),
        "freshness_gate": _gate("FRESHNESS_FAIL" not in data_errors and "STALE_ARTIFACT" not in data_errors and "FUTURE_DATED_ARTIFACT" not in data_errors, fail_closed_reason),
        "blocked_dependency_gate": _gate("BLOCKED_DEPENDENCIES_PRESENT" not in data_errors, fail_closed_reason),
        "previous_state_gate": _gate(not any(error in data_errors for error in ("INVALID_PREVIOUS_STATE_REQUIREMENT", "MISSING_PREVIOUS_STATE", "CORRUPTED_PREVIOUS_STATE", "MISSING_PREVIOUS_STATE_ID", "PREVIOUS_STATE_ID_MISMATCH")), fail_closed_reason),
        "six_chart_preview_status": "ALLOWED" if render_pass else "BLOCKED",
        "fallback_used": False,
        "state_mutation_allowed": False,
        "portfolio_mutation_allowed": False,
        "ledger_mutation_allowed": False,
        "production_mutation_allowed": False,
        "no_recalculation_evidence": {
            "status": "PASS",
            "not_calculated": list(NO_RECALCULATION_TARGETS),
            "source": "production_manifest_and_bound_public_artifacts_only",
        },
        "fail_closed_reason": fail_closed_reason,
        "status": status,
    }
