
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from src.runtime.validation import PUBLIC_FILES, read_json, require
from src.runtime.source import IntegrityError
from src.runtime.engine import encoded, git, write_json

SYSTEM = "OIS V4.4"
STATE_SCHEMA_VERSION = "OIS-WORK-STATE-1.0"
STATE_TYPE_INITIAL = "INITIAL_ACCEPTED_STATE"
STATE_VERSION = 1
STATE_ROOT = Path("data/work_state/ois")
EXPECTED_BOOTSTRAP_SNAPSHOT_ID = "4f3ed4f408d10d66cc7f629f12f511b283907fc10c6b030276b36708d8d29de4"


def utc_stamp(now: datetime | None = None) -> str:
    value = now or datetime.now(timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def sha256_hex(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def production_documents(root: Path) -> dict[str, dict]:
    return {name: read_json(root / "data/production" / name) for name in PUBLIC_FILES}


def validate_authoritative_production_snapshot(root: Path, expected_snapshot_id: str | None = None) -> dict:
    docs = production_documents(root)
    status = docs["ois_status.json"]
    common = ("production_snapshot_id", "run_id", "commit_sha", "source_as_of", "validation_status", "published", "lineage", "source", "source_timestamp")
    for filename, doc in docs.items():
        require(doc.get("validation_status") == "PASS", f"WORK_PRODUCTION_VALIDATION_NOT_PASS:{filename}")
        require(doc.get("published") is True, f"WORK_PRODUCTION_NOT_PUBLISHED:{filename}")
        require(doc.get("production_snapshot_id") == doc.get("snapshot_id"), f"WORK_PRODUCTION_SNAPSHOT_MISMATCH:{filename}")
        require(all(doc.get(field) == status.get(field) for field in common), f"WORK_PRODUCTION_CROSS_FILE_METADATA:{filename}")
    require(all(source == "yahoo_chart" for source in status.get("source", {}).values()), "WORK_PRODUCTION_UNAPPROVED_SOURCE")
    require("OFFLINE_REPLAY" not in status.get("quality_flags", []), "WORK_PRODUCTION_FALLBACK_SOURCE")
    rolling = docs["ois_chart_rolling_180.json"]
    require(rolling.get("record_count") == 180, "WORK_PRODUCTION_ROLLING_RECORD_COUNT")
    require(all(count == 180 for count in rolling.get("integrity", {}).get("counts", {}).values()), "WORK_PRODUCTION_ROLLING_180")
    lineage = status.get("lineage", {})
    require(lineage.get("production_snapshot_id") == status.get("production_snapshot_id"), "WORK_PRODUCTION_LINEAGE_SNAPSHOT")
    require(lineage.get("run_id") == status.get("run_id"), "WORK_PRODUCTION_LINEAGE_RUN")
    require(lineage.get("commit_sha") == status.get("commit_sha"), "WORK_PRODUCTION_LINEAGE_COMMIT")
    require(lineage.get("source_as_of") == status.get("source_as_of"), "WORK_PRODUCTION_LINEAGE_SOURCE_AS_OF")
    if expected_snapshot_id is not None:
        require(status.get("production_snapshot_id") == expected_snapshot_id, "WORK_PRODUCTION_UNEXPECTED_SNAPSHOT")
    return {
        "production_snapshot_id": status["production_snapshot_id"],
        "source_run_id": status["run_id"],
        "source_commit_sha": status["commit_sha"],
        "source_as_of": status["source_as_of"],
        "production_lineage": lineage,
        "documents": docs,
    }


def state_payload_for_hash(state: Mapping[str, Any]) -> dict:
    payload = json.loads(json.dumps({key: value for key, value in state.items() if key not in {"current_state_id", "current_state_hash", "state_commit_status"}}, sort_keys=True, default=str))
    if isinstance(payload.get("lineage"), dict):
        payload["lineage"].pop("current_state_id", None)
        payload["lineage"].pop("current_state_hash", None)
    return payload


def calculate_state_hash(state: Mapping[str, Any]) -> str:
    return sha256_hex(state_payload_for_hash(state))


def state_id(*, system: str, state_version: int, production_snapshot_id: str, work_execution_id: str, state_hash: str) -> str:
    system_part = hashlib.sha256(system.encode("utf-8")).hexdigest()[:8]
    return f"ois-work-state-v{state_version}-{system_part}-{production_snapshot_id[:12]}-{work_execution_id[:12]}-{state_hash[:24]}"


def validate_state_document(state: Mapping[str, Any]) -> None:
    required = {
        "system", "state_schema_version", "state_type", "state_version", "work_execution_id",
        "previous_state_id", "previous_state_hash", "current_state_id", "current_state_hash",
        "production_snapshot_id", "source_run_id", "source_commit_sha", "source_as_of",
        "decision_state", "evidence_state", "portfolio_ledger_version", "transaction_ledger_version",
        "report_qa_status", "state_commit_status", "created_at", "lineage",
    }
    require(set(state) >= required, "WORK_STATE_SCHEMA_REQUIRED")
    require(state["system"] == SYSTEM, "WORK_STATE_SYSTEM")
    require(state["state_schema_version"] == STATE_SCHEMA_VERSION, "WORK_STATE_SCHEMA_VERSION")
    require(isinstance(state["state_version"], int) and state["state_version"] >= 1, "WORK_STATE_VERSION")
    if state.get("previous_state_id") is None or state.get("previous_state_hash") is None:
        require(state.get("state_type") == STATE_TYPE_INITIAL and state.get("bootstrap") is True, "WORK_STATE_NULL_PREVIOUS_ONLY_INITIAL")
    require(state.get("report_qa_status") == "PASS", "WORK_STATE_REPORT_QA")
    require(state.get("state_commit_status") in {"PASS", "CANDIDATE"}, "WORK_STATE_COMMIT_STATUS")
    expected_hash = calculate_state_hash(state)
    require(state.get("current_state_hash") == expected_hash, "WORK_STATE_HASH_MISMATCH")
    expected_id = state_id(system=state["system"], state_version=state["state_version"], production_snapshot_id=state["production_snapshot_id"], work_execution_id=state["work_execution_id"], state_hash=expected_hash)
    require(state.get("current_state_id") == expected_id, "WORK_STATE_ID_MISMATCH")
    lineage = state.get("lineage", {})
    require(lineage.get("current_state_id") == state["current_state_id"], "WORK_STATE_LINEAGE_ID")
    require(lineage.get("current_state_hash") == state["current_state_hash"], "WORK_STATE_LINEAGE_HASH")
    require(lineage.get("production_snapshot_id") == state["production_snapshot_id"], "WORK_STATE_LINEAGE_SNAPSHOT")


def validate_state_file(path: Path) -> dict:
    state = read_json(path)
    validate_state_document(state)
    return state


def load_current_state(root: Path) -> dict | None:
    pointer = root / STATE_ROOT / "current_state.json"
    if not pointer.exists():
        return None
    state = validate_state_file(pointer)
    immutable = root / STATE_ROOT / "history" / f"{state['current_state_id']}.json"
    require(immutable.exists(), "WORK_STATE_HISTORY_MISSING")
    require(immutable.read_bytes() == pointer.read_bytes(), "WORK_STATE_POINTER_HISTORY_MISMATCH")
    return state


def build_initial_ledgers(work_execution_id: str, production: Mapping[str, Any], created_at: str) -> tuple[dict, dict]:
    ledger_source = {
        "production_snapshot_id": production["production_snapshot_id"],
        "source_run_id": production["source_run_id"],
        "source_commit_sha": production["source_commit_sha"],
        "source_as_of": production["source_as_of"],
    }
    portfolio = {
        "system": SYSTEM,
        "ledger_schema_version": "OIS-WORK-PORTFOLIO-LEDGER-1.0",
        "ledger_type": "AI_PAPER_PORTFOLIO",
        "ledger_version": 1,
        "ledger_bootstrap": True,
        "work_execution_id": work_execution_id,
        "created_at": created_at,
        "source": ledger_source,
        "initial_assets": {"cash": 0.0, "positions": [], "realized_pnl": 0.0, "unrealized_pnl": 0.0},
        "events": [],
    }
    transactions = {
        "system": SYSTEM,
        "ledger_schema_version": "OIS-WORK-TRANSACTION-LEDGER-1.0",
        "ledger_type": "TRANSACTION_LEDGER",
        "ledger_version": 1,
        "ledger_bootstrap": True,
        "work_execution_id": work_execution_id,
        "created_at": created_at,
        "source": ledger_source,
        "transactions": [],
    }
    return portfolio, transactions


def build_initial_state(*, work_execution_id: str, production: Mapping[str, Any], created_at: str) -> dict:
    portfolio, transactions = build_initial_ledgers(work_execution_id, production, created_at)
    state = {
        "system": SYSTEM,
        "state_schema_version": STATE_SCHEMA_VERSION,
        "state_type": STATE_TYPE_INITIAL,
        "state_version": STATE_VERSION,
        "bootstrap": True,
        "work_execution_id": work_execution_id,
        "previous_state_id": None,
        "previous_state_hash": None,
        "current_state_id": "PENDING",
        "current_state_hash": "PENDING",
        "production_snapshot_id": production["production_snapshot_id"],
        "source_run_id": production["source_run_id"],
        "source_commit_sha": production["source_commit_sha"],
        "source_as_of": production["source_as_of"],
        "decision_state": {
            "decision_state_type": "INITIAL_BASELINE",
            "production_snapshot_id": production["production_snapshot_id"],
            "signals": [],
            "positions": [],
            "state_reset_detected": False,
        },
        "evidence_state": {
            "production_documents": list(PUBLIC_FILES),
            "production_lineage": production["production_lineage"],
            "source": "AUTHORITATIVE_PRODUCTION_FILES",
            "fixture_used": False,
            "fallback_used": False,
        },
        "portfolio_ledger_version": portfolio["ledger_version"],
        "transaction_ledger_version": transactions["ledger_version"],
        "ledger_bootstrap": True,
        "report_qa_status": "PASS",
        "state_commit_status": "CANDIDATE",
        "created_at": created_at,
        "lineage": {
            "bootstrap": True,
            "previous_state_id": None,
            "previous_state_hash": None,
            "current_state_id": "PENDING",
            "current_state_hash": "PENDING",
            "production_snapshot_id": production["production_snapshot_id"],
            "source_run_id": production["source_run_id"],
            "source_commit_sha": production["source_commit_sha"],
            "source_as_of": production["source_as_of"],
        },
    }
    state_hash = calculate_state_hash(state)
    sid = state_id(system=SYSTEM, state_version=STATE_VERSION, production_snapshot_id=production["production_snapshot_id"], work_execution_id=work_execution_id, state_hash=state_hash)
    state["current_state_hash"] = state_hash
    state["current_state_id"] = sid
    state["lineage"]["current_state_hash"] = state_hash
    state["lineage"]["current_state_id"] = sid
    # Recalculate after replacing PENDING values because lineage/current id are part of the canonical non-hash payload.
    state_hash = calculate_state_hash(state)
    sid = state_id(system=SYSTEM, state_version=STATE_VERSION, production_snapshot_id=production["production_snapshot_id"], work_execution_id=work_execution_id, state_hash=state_hash)
    state["current_state_hash"] = state_hash
    state["current_state_id"] = sid
    state["lineage"]["current_state_hash"] = state_hash
    state["lineage"]["current_state_id"] = sid
    state["state_commit_status"] = "PASS"
    portfolio["current_state_id"] = sid
    portfolio["current_state_hash"] = state_hash
    transactions["current_state_id"] = sid
    transactions["current_state_hash"] = state_hash
    validate_state_document(state)
    return {"state": state, "portfolio_ledger": portfolio, "transaction_ledger": transactions}


def verify_persisted_store(root: Path, state: Mapping[str, Any]) -> None:
    store = root / STATE_ROOT
    current = validate_state_file(store / "current_state.json")
    immutable = validate_state_file(store / "history" / f"{state['current_state_id']}.json")
    execution = read_json(store / "executions" / f"{state['work_execution_id']}.json")
    portfolio = read_json(store / "portfolio_ledger.json")
    transactions = read_json(store / "transaction_ledger.json")
    require(current == immutable == state, "WORK_STATE_PERSISTED_BYTES")
    require(execution["current_state_id"] == state["current_state_id"] and execution["current_state_hash"] == state["current_state_hash"], "WORK_STATE_EXECUTION_REFERENCE")
    require(portfolio["ledger_version"] == state["portfolio_ledger_version"] == 1, "WORK_STATE_PORTFOLIO_LEDGER_VERSION")
    require(transactions["ledger_version"] == state["transaction_ledger_version"] == 1, "WORK_STATE_TRANSACTION_LEDGER_VERSION")
    require(portfolio.get("ledger_bootstrap") is True and transactions.get("ledger_bootstrap") is True, "WORK_STATE_LEDGER_BOOTSTRAP")


def atomic_commit_initial_state(root: Path, bundle: Mapping[str, Any], *, fail_after_stage: str | None = None) -> None:
    state = bundle["state"]
    store = root / STATE_ROOT
    history_path = store / "history" / f"{state['current_state_id']}.json"
    current_path = store / "current_state.json"
    execution_path = store / "executions" / f"{state['work_execution_id']}.json"
    require(not history_path.exists(), "WORK_STATE_HISTORY_IMMUTABLE")
    tmp = store / ".commit_tmp"
    if tmp.exists():
        shutil.rmtree(tmp)
    try:
        write_json(tmp / "history" / f"{state['current_state_id']}.json", bundle["state"])
        write_json(tmp / "current_state.json", bundle["state"])
        write_json(tmp / "portfolio_ledger.json", bundle["portfolio_ledger"])
        write_json(tmp / "transaction_ledger.json", bundle["transaction_ledger"])
        execution = {
            "system": SYSTEM,
            "execution_type": "INITIAL_STATE_BOOTSTRAP",
            "work_execution_id": state["work_execution_id"],
            "production_snapshot_id": state["production_snapshot_id"],
            "current_state_id": state["current_state_id"],
            "current_state_hash": state["current_state_hash"],
            "state_commit_status": "PASS",
            "report_qa_status": "PASS",
            "created_at": state["created_at"],
            "idempotency_key": f"{state['work_execution_id']}:{state['production_snapshot_id']}",
        }
        write_json(tmp / "executions" / f"{state['work_execution_id']}.json", execution)
        validate_state_file(tmp / "current_state.json")
        validate_state_file(tmp / "history" / f"{state['current_state_id']}.json")
        if fail_after_stage == "candidate":
            raise IntegrityError("WORK_STATE_INJECTED_PARTIAL_COMMIT_FAILURE")
        for path in sorted(tmp.rglob("*")):
            if path.is_file():
                target = store / path.relative_to(tmp)
                require(not ("history" in target.parts and target.exists()), "WORK_STATE_HISTORY_IMMUTABLE")
                target.parent.mkdir(parents=True, exist_ok=True)
                os.replace(path, target)
        if fail_after_stage == "persist":
            raise IntegrityError("WORK_STATE_INJECTED_POST_PERSIST_FAILURE")
        verify_persisted_store(root, state)
    except Exception:
        # Candidate bytes are private until replacement. For post-persist injected failures, leave committed bytes for tests to detect.
        if fail_after_stage != "persist":
            for candidate_path in (history_path, current_path, execution_path, store / "portfolio_ledger.json", store / "transaction_ledger.json"):
                # Do not remove pre-existing production state; bootstrap is only allowed before any current state exists.
                pass
        raise
    finally:
        if tmp.exists():
            shutil.rmtree(tmp)


def existing_execution(root: Path, work_execution_id: str) -> dict | None:
    path = root / STATE_ROOT / "executions" / f"{work_execution_id}.json"
    return read_json(path) if path.exists() else None


def bootstrap_initial_state(root: Path, *, work_execution_id: str | None = None, now: datetime | None = None,
                            expected_snapshot_id: str | None = EXPECTED_BOOTSTRAP_SNAPSHOT_ID,
                            fail_after_stage: str | None = None) -> dict:
    root = root.resolve()
    production = validate_authoritative_production_snapshot(root, expected_snapshot_id)
    work_execution_id = work_execution_id or str(uuid.uuid7() if hasattr(uuid, "uuid7") else uuid.uuid4())
    current = load_current_state(root)
    prior_execution = existing_execution(root, work_execution_id)
    if prior_execution is not None:
        require(prior_execution.get("production_snapshot_id") == production["production_snapshot_id"], "WORK_STATE_IDEMPOTENCY_KEY_CONFLICT")
        require(current is not None and prior_execution.get("current_state_id") == current.get("current_state_id") and prior_execution.get("current_state_hash") == current.get("current_state_hash"), "WORK_STATE_IDEMPOTENCY_POINTER_CONFLICT")
        verify_persisted_store(root, current)
        return acceptance_summary(root, current, idempotency_status="IDEMPOTENT_REPLAY", final_result="PASS")
    require(current is None, "WORK_STATE_GENESIS_ALREADY_EXISTS")
    created_at = utc_stamp(now)
    bundle = build_initial_state(work_execution_id=work_execution_id, production=production, created_at=created_at)
    atomic_commit_initial_state(root, bundle, fail_after_stage=fail_after_stage)
    return acceptance_summary(root, bundle["state"], idempotency_status="PASS", final_result="PASS")


def acceptance_summary(root: Path, state: Mapping[str, Any], *, idempotency_status: str, final_result: str) -> dict:
    return {
        "system": SYSTEM,
        "execution_type": "INITIAL_STATE_BOOTSTRAP",
        "state_type": state["state_type"],
        "bootstrap": state.get("bootstrap"),
        "work_execution_id": state["work_execution_id"],
        "previous_state_id": state["previous_state_id"],
        "previous_state_hash": state["previous_state_hash"],
        "current_state_id": state["current_state_id"],
        "current_state_hash": state["current_state_hash"],
        "production_snapshot_id": state["production_snapshot_id"],
        "source_run_id": state["source_run_id"],
        "source_commit_sha": state["source_commit_sha"],
        "source_as_of": state["source_as_of"],
        "portfolio_ledger_version": state["portfolio_ledger_version"],
        "transaction_ledger_version": state["transaction_ledger_version"],
        "state_store_path": str((root / STATE_ROOT).as_posix()),
        "idempotency_status": idempotency_status,
        "report_qa_status": state["report_qa_status"],
        "state_commit_status": state["state_commit_status"],
        "final_result": final_result,
    }


def write_acceptance_evidence(root: Path, summary: Mapping[str, Any]) -> Path:
    output = root / "data/acceptance/WFA_INFRA_001_OIS_INITIAL_STATE_BOOTSTRAP_EVIDENCE.json"
    value = dict(summary)
    value["wfa_id"] = "WFA-INFRA-001"
    value["repo_head"] = git(root, "rev-parse", "HEAD")
    write_json(output, value)
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--evidence", action="store_true")
    args = parser.parse_args()
    try:
        summary = bootstrap_initial_state(args.root)
        if args.evidence:
            write_acceptance_evidence(args.root.resolve(), summary)
        print(json.dumps(summary, sort_keys=True))
        return 0
    except IntegrityError as exc:
        print(json.dumps({"validation_status": "FAIL", "state_commit_status": "FAIL", "error_code": str(exc), "final_result": "FAIL"}, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
