from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import io
import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from scripts.build_rolling_180 import REQUIRED_DATASETS, state_dates
from src.indicators.technical import build_indicators
from src.payload.chart_payload import build_chart_payload
from src.rollover.normalization import check_rollover
from src.sources.adapters import PriceRow
from src.runtime.source import IntegrityError, TransientError, fetch, stamp
from src.runtime.shadow_manifest import build_shadow_manifest, validate_shadow_manifest
from src.runtime.revisions import load_revision_evidence, match_approved_revision, revision_summary
from src.runtime.validation import (CHECKS, FIELDS, PUBLIC_FILES, read_json, require,
                                    validate_documents, validate_history, validate_indicators)


CSV_FIELDS = ["date", *FIELDS, "source"]
STATE_PATH = "data/runtime/history.json"
PRODUCTION_BUNDLE_MANIFEST_PATH = "data/production/ois_production_bundle_manifest_v1.json"
ALLOWED_CANDIDATE_FILES = {f"data/production/{filename}" for filename in PUBLIC_FILES} | {STATE_PATH} | {
    f"data/production/ois_{key}_{suffix}" for key in ("wti", "brent") for suffix in ("clean.csv", "indicators.json")} | {
    PRODUCTION_BUNDLE_MANIFEST_PATH}


def encoded(value: dict) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode("utf-8")


def digest(value: dict) -> str:
    return hashlib.sha256(encoded(value)).hexdigest()


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(encoded(value))


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True, encoding="utf-8").strip()


def load_previous(root: Path) -> tuple[dict, dict | None, bool]:
    state_path = root / STATE_PATH
    migrated = state_path.exists()
    if migrated:
        state = read_json(state_path)
        require(state["schema_version"] == "OIS-HISTORY-1.0", "HISTORY_SCHEMA")
        require(state["snapshot_id"] == digest(state["commodities"]), "HISTORY_HASH_MISMATCH")
    else:
        commodities = {}
        for key in ("wti", "brent"):
            path = root / "data/production" / f"ois_{key}_clean.csv"
            with path.open(encoding="utf-8", newline="") as handle:
                commodities[key] = [{**row, **{field: float(row[field]) for field in FIELDS}} for row in csv.DictReader(handle)]
        state = {"commodities": commodities}
    rolling_path = root / "data/production/ois_chart_rolling_180.json"
    rolling = read_json(rolling_path) if rolling_path.exists() else None
    if rolling is not None:
        dates = state_dates(rolling)
        require(dates == sorted(set(dates)), "PREVIOUS_ROLLING_DATE_ORDER")
        payload = read_json(root / "data/production/ois_chart_payload.json")
        for key in REQUIRED_DATASETS:
            mapped = {row["date"]: row for row in payload["datasets"][key]}
            require(all(mapped.get(row["date"]) == row for row in rolling["datasets"][key]), "PREVIOUS_ROLLING_PAYLOAD_MISMATCH")
    elif migrated:
        raise IntegrityError("MISSING_PERSISTENT_ROLLING_STATE")
    return state, rolling, migrated


def merge_history(previous: list[dict], incoming: list[dict], migrated: bool, *, instrument: str | None = None, revision_evidence: dict | None = None) -> tuple[list[dict], int, list[dict]]:
    old = {row["date"]: row for row in previous}
    require(len(old) == len(previous), "PREVIOUS_DUPLICATE_DATE")
    require(incoming[-1]["date"] >= previous[-1]["date"], "SOURCE_DATE_REGRESSION")
    overlap = set(old) & {row["date"] for row in incoming}
    require(bool(overlap), "HISTORY_NO_OVERLAP")
    incoming_dates = {row["date"] for row in incoming}
    require(all(day in incoming_dates for day in old if incoming[0]["date"] <= day <= incoming[-1]["date"]),
            "SOURCE_HISTORICAL_SESSION_DISAPPEARED")
    revisions = 0
    accepted_revisions: list[dict] = []
    for row in incoming:
        day = row["date"]
        if day in old:
            changed = any(row[field] != old[day][field] for field in FIELDS)
            if changed:
                accepted = match_approved_revision(evidence=revision_evidence or {}, instrument=instrument or "", old_row=old[day], incoming_row=row) if migrated else None
                require((not migrated) or accepted is not None, f"HISTORICAL_SOURCE_REVISION:{day}")
                revisions += 1
                if accepted is not None:
                    accepted_revisions.append(accepted)
            if (not migrated) or changed:
                old[day] = row
        elif day > previous[-1]["date"]:
            old[day] = row
        elif day >= previous[0]["date"]:
            require(not migrated, f"HISTORICAL_SESSION_INSERTION:{day}")
            old[day] = row
            revisions += 1
        # Do not prepend older observations: EMA seed must never move.
    return [old[day] for day in sorted(old)], revisions, accepted_revisions


def make_documents(history: dict, previous_rolling: dict | None, migrated: bool,
                   now: datetime, flags: list[str], revisions: dict, accepted_revisions: list[dict] | None = None,
                   migration_from_legacy: bool = False) -> tuple[dict, dict]:
    commodities = history["commodities"]
    clean, indicators = {}, {}
    for key, rows in commodities.items():
        clean[key] = [{field: row[field] for field in CSV_FIELDS} for row in rows]
        indicators[key] = build_indicators(clean[key])
        validate_indicators(clean[key], indicators[key])
    require(commodities["wti"][-1]["date"] == commodities["brent"][-1]["date"], "LATEST_DATES_UNSYNCHRONIZED")
    now_string = stamp(now)
    snapshot_id = digest(commodities)
    base_run_id = os.environ.get("GITHUB_RUN_ID") or f"local-{snapshot_id[:12]}"
    run_id = f"legacy-migration-{base_run_id}" if migration_from_legacy else base_run_id
    commit_sha = os.environ.get("GITHUB_SHA") or "local"
    source_as_of = commodities["wti"][-1]["date"]
    revision_info = revision_summary(accepted_revisions or [])
    common = {
        "runtime_contract_version": "1.0", "generated_at": now_string,
        "data_as_of": source_as_of, "source_as_of": source_as_of,
        "source": {key: rows[-1]["source"] for key, rows in commodities.items()},
        "source_timestamp": {key: rows[-1]["source_timestamp"] for key, rows in commodities.items()},
        "snapshot_id": snapshot_id, "production_snapshot_id": snapshot_id,
        "run_id": run_id, "commit_sha": commit_sha, "published": True,
        "validation_status": "PASS",
        "quality_flags": sorted(set(flags)), "missing_fields": [],
        "duplicate_status": "PASS", "freshness_status": "PASS",
        "lineage": {"production_snapshot_id": snapshot_id, "run_id": run_id, "commit_sha": commit_sha,
                    "source_as_of": source_as_of, "migration_from_legacy": migration_from_legacy, **revision_info},
    }
    history.update({**common, "schema_version": "OIS-HISTORY-1.0", "record_count": sum(map(len, commodities.values()))})
    report = {**common, "schema_version": "OIS-VALIDATION-1.0", "record_count": 2,
              "overall_validation": "PASS", "chart_payload_schema": "PASS", "checks": dict.fromkeys(CHECKS, "PASS"),
              "last_successful_update": now_string, "last_attempt": now_string, "failure_reason": None,
              "bootstrap_revised_rows": revisions}
    for name, key in (("WTI", "wti"), ("Brent", "brent")):
        rollover = check_rollover([PriceRow(**row) for row in clean[key]], name).to_dict()
        report[name] = {"validation_status": "PASS", "source_date": common["data_as_of"],
                        "source": common["source"][key], "rows": len(clean[key]), "missing_close": 0,
                        "duplicate_dates": 0, "unique_dates": len(clean[key]), "rollover_validation": rollover}
    payload = build_chart_payload(clean["wti"], clean["brent"], indicators["wti"], indicators["brent"], report, max_points=250)
    payload.update({**common, "record_count": 250})
    datasets = {key: rows[-180:] for key, rows in payload["datasets"].items()}
    dates = [row["date"] for row in datasets[REQUIRED_DATASETS[0]]]
    appended = 0
    if previous_rolling:
        old_dates = state_dates(previous_rolling)
        require(dates[-1] >= old_dates[-1], "ROLLING_DATE_REGRESSION")
        appended = len([day for day in dates if day > old_dates[-1]])
        if migrated:
            revision_dates = {item["date"] for item in (accepted_revisions or [])}
            earliest_revision = min(revision_dates) if revision_dates else None
            for key, rows in datasets.items():
                old = {row["date"]: row for row in previous_rolling["datasets"][key]}
                for row in rows:
                    require(row["date"] not in old or row == old[row["date"]] or (earliest_revision is not None and row["date"] >= earliest_revision), "ROLLING_HISTORICAL_INDICATOR_DRIFT")
                retained = [row for row in previous_rolling["datasets"][key] if row["date"] >= dates[0]]
                added = [row for row in rows if row["date"] > old_dates[-1]]
                if earliest_revision is None:
                    require(retained + added == rows, "PERSISTENT_ROLLING_CONTINUITY")
    rolling = {**common, "schema_version": "OIS-ROLLING-180-1.0", "record_count": 180,
               "window_size": 180, "first_source_date": dates[0], "last_source_date": dates[-1],
               "update_mode": "CONTROLLED_HISTORICAL_REBUILD" if (accepted_revisions or []) else (("INCREMENTAL_APPEND_DROP" if appended else previous_rolling["update_mode"]) if migrated else "FULL_RECONCILIATION"),
               "runtime_update_result": "CONTROLLED_HISTORICAL_REBUILD" if (accepted_revisions or []) else ("APPENDED" if appended else "NO_NEW_TRADING_DAY"),
               "last_full_reconciliation_at": previous_rolling["last_full_reconciliation_at"] if migrated else now_string,
               "source_payload_generated_at": now_string, "latest_complete_source_date": payload["latest_complete_source_date"],
               "appended_trading_days": appended, "dropped_trading_days": appended,
               "integrity": {"counts": dict.fromkeys(datasets, 180), "dates_synchronized": True, "duplicate_dates": False},
               "datasets": datasets}
    status = {**common, "schema_version": "OIS-STATUS-1.0", "record_count": 2,
              "service": "OIS Technical Data Engine", "status": "PASS", "validation": "PASS",
              "last_successful_update": now_string, "WTI_source_date": common["data_as_of"], "Brent_source_date": common["data_as_of"]}
    documents = dict(zip(PUBLIC_FILES, (status, report, payload, rolling)))
    return documents, indicators




def sync_final_binding_artifacts(root: Path, candidate: Path) -> list[str]:
    """Regenerate Production final-binding artifacts inside a private candidate.

    The Work current state and ledgers may legitimately remain on their latest
    cadence transition, but their authoritative production pointer and the
    execution data layer must bind to the freshly built Production snapshot.
    """
    from src.execution_layer import EXECUTION_LAYER_PATH, build_execution_data_layer, validate_execution_data_layer
    from src.work_state import PERSISTENT_STATE_SSOT_PATH, STATE_ROOT, write_production_persistent_state_ssot

    root_store = root / STATE_ROOT
    if not root_store.exists():
        return []
    candidate_store = candidate / STATE_ROOT
    if candidate_store.exists():
        shutil.rmtree(candidate_store)
    shutil.copytree(root_store, candidate_store)
    ssot_path = write_production_persistent_state_ssot(candidate)
    execution_doc = build_execution_data_layer(candidate)
    validate_execution_data_layer(execution_doc)
    write_json(candidate / EXECUTION_LAYER_PATH, execution_doc)
    status = read_json(candidate / "data/production/ois_status.json")
    ssot = read_json(ssot_path)
    authoritative = ssot.get("authoritative_production_pointer", {})
    require(authoritative.get("production_snapshot_id") == status.get("production_snapshot_id"), "FINAL_BINDING_SSOT_SNAPSHOT")
    require(authoritative.get("run_id") == status.get("run_id"), "FINAL_BINDING_SSOT_RUN")
    require(authoritative.get("commit_sha") == status.get("commit_sha"), "FINAL_BINDING_SSOT_COMMIT")
    require(authoritative.get("source_as_of") == status.get("source_as_of"), "FINAL_BINDING_SSOT_SOURCE_AS_OF")
    execution = read_json(candidate / EXECUTION_LAYER_PATH)
    binding = execution.get("authoritative_production_binding", {})
    require(binding.get("production_snapshot_id") == status.get("production_snapshot_id"), "FINAL_BINDING_EXECUTION_SNAPSHOT")
    require(binding.get("run_id") == status.get("run_id"), "FINAL_BINDING_EXECUTION_RUN")
    require(binding.get("commit_sha") == status.get("commit_sha"), "FINAL_BINDING_EXECUTION_COMMIT")
    require(binding.get("source_as_of") == status.get("source_as_of"), "FINAL_BINDING_EXECUTION_SOURCE_AS_OF")
    return [PERSISTENT_STATE_SSOT_PATH.as_posix(), EXECUTION_LAYER_PATH.as_posix()]


def materialize_production_bundle_manifest(candidate: Path, now: datetime, cadence: str = "OIS_1830_PRODUCTION") -> dict:
    """Bind the four public Production artifacts without recalculating them."""
    history = read_json(candidate / STATE_PATH)
    manifest = build_shadow_manifest(
        production_dir=candidate / "data/production",
        cadence=cadence,
        event=os.environ.get("GITHUB_EVENT_NAME", "local"),
        market_date=history["data_as_of"],
        generated_at=stamp(now),
        reference_root=candidate,
    )
    validation = validate_shadow_manifest(manifest, root=candidate, now=now)
    require(validation["validation_status"] == "PASS", "PRODUCTION_BUNDLE_MANIFEST_INVALID")
    write_json(candidate / PRODUCTION_BUNDLE_MANIFEST_PATH, {**manifest, "contract_validation": validation})
    return manifest


def build_candidate(root: Path, candidate: Path, now: datetime, *, fetcher=fetch, fixture: Path | None = None) -> dict:
    require(candidate.resolve() != root.resolve() and not candidate.exists(), "CANDIDATE_MUST_BE_NEW_DIRECTORY")
    state, rolling, migrated = load_previous(root)
    legacy_metadata_migration = False
    if migrated:
        legacy_metadata_migration = preflight_current_production(root, now)
    revision_evidence = load_revision_evidence(root)
    commodities, flags, revisions, accepted_revisions = {}, [], {}, []
    for key, ticker in (("wti", "CL=F"), ("brent", "BZ=F")):
        previous = state["commodities"][key]
        # Legacy baselines are revalidated before merging any incoming observations.
        validate_history(previous, now, fresh=False)
        if fixture:
            rows = read_json(fixture)["commodities"][key]
            source_flags = ["OFFLINE_REPLAY"]
        else:
            rows, source_flags = fetcher(ticker, now)
        validate_history(rows, now)
        commodities[key], revisions[key], accepted = merge_history(previous, rows, migrated, instrument=key, revision_evidence=revision_evidence)
        accepted_revisions.extend(accepted)
        validate_history(commodities[key], now)
        flags.extend(source_flags)
    if not migrated:
        flags.append("LEGACY_BOOTSTRAP_RECONCILIATION")
    history = {"commodities": commodities}
    documents, indicators = make_documents(history, rolling, migrated, now, flags, revisions, accepted_revisions, legacy_metadata_migration)
    validate_documents(documents, history, now)
    # Nothing has been written into production; all candidate bytes are private.
    for filename, document in documents.items():
        write_json(candidate / "data/production" / filename, document)
    for key, rows in commodities.items():
        write_json(candidate / "data/production" / f"ois_{key}_indicators.json", indicators[key])
        output = io.StringIO(newline="")
        writer = csv.DictWriter(output, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows({field: row[field] for field in CSV_FIELDS} for row in rows)
        (candidate / "data/production" / f"ois_{key}_clean.csv").write_bytes(output.getvalue().encode("utf-8"))
    write_json(candidate / STATE_PATH, history)
    validate_bundle(candidate, now)
    materialize_production_bundle_manifest(candidate, now)
    final_binding_files = sync_final_binding_artifacts(root, candidate) if fixture is None else []
    manifest_files = sorted(ALLOWED_CANDIDATE_FILES | set(final_binding_files))
    candidate_manifest = {"validation_status": "PASS", "generated_at": stamp(now), "base_commit": git(root, "rev-parse", "HEAD"),
                "publishable": fixture is None, "snapshot_id": history["snapshot_id"],
                "final_binding_files": final_binding_files,
                "files": {filename: hashlib.sha256((candidate / filename).read_bytes()).hexdigest() for filename in manifest_files}}
    write_json(candidate / "manifest.json", candidate_manifest)
    return {"validation_status": "PASS", "data_as_of": history["data_as_of"], "rolling_count": 180,
            "candidate": str(candidate), "bootstrap_revised_rows": revisions, "accepted_historical_revisions": accepted_revisions, "legacy_metadata_migration": legacy_metadata_migration, "publishable": fixture is None}


NEW_METADATA_FIELDS = ("production_snapshot_id", "run_id", "commit_sha", "source_as_of", "published", "lineage")
LEGACY_SCHEMA_VERSIONS = {
    "ois_status.json": "OIS-STATUS-1.0",
    "ois_ingestion_validation.json": "OIS-VALIDATION-1.0",
    "ois_chart_payload.json": "OIS-CHART-1.0",
    "ois_chart_rolling_180.json": "OIS-ROLLING-180-1.0",
}


def has_all_new_metadata(document: dict) -> bool:
    return all(field in document for field in NEW_METADATA_FIELDS)


def has_any_new_metadata(document: dict) -> bool:
    return any(field in document for field in NEW_METADATA_FIELDS)


def preflight_current_production(root: Path, now: datetime) -> bool:
    try:
        validate_bundle(root, now, check_freshness=False)
        return False
    except IntegrityError:
        validate_legacy_bundle(root, now)
        return True


def validate_legacy_bundle(root: Path, now: datetime) -> None:
    history = read_json(root / STATE_PATH)
    require(history["schema_version"] == "OIS-HISTORY-1.0" and history["snapshot_id"] == digest(history["commodities"]), "HISTORY_HASH_MISMATCH")
    documents = {filename: read_json(root / "data/production" / filename) for filename in PUBLIC_FILES}
    for filename, document in documents.items():
        require(document.get("schema_version") == LEGACY_SCHEMA_VERSIONS[filename], f"LEGACY_SCHEMA_UNSUPPORTED:{filename}")
        require(not has_any_new_metadata(document), f"LEGACY_METADATA_PARTIAL_OR_CONFLICTING:{filename}")
    validation_time = datetime.fromisoformat(history["generated_at"].replace("Z", "+00:00"))
    legacy_common = ("runtime_contract_version", "generated_at", "data_as_of", "source", "source_timestamp", "snapshot_id",
                     "validation_status", "quality_flags", "missing_fields", "duplicate_status", "freshness_status")
    baseline = documents["ois_status.json"]
    for filename, document in documents.items():
        require(all(key in document and document[key] == baseline[key] for key in legacy_common), f"LEGACY_CROSS_FILE_METADATA:{filename}")
    require(baseline["snapshot_id"] == history["snapshot_id"], "LEGACY_SNAPSHOT_HASH_MISMATCH")
    require(baseline["data_as_of"] == history["data_as_of"], "LEGACY_SOURCE_AS_OF_MISMATCH")
    report = documents["ois_ingestion_validation.json"]
    require(baseline["status"] == baseline["validation"] == report["overall_validation"] == "PASS", "LEGACY_STATUS_MISMATCH")
    require(report["checks"] == dict.fromkeys(CHECKS, "PASS"), "LEGACY_VALIDATION_CHECKS")
    require(report["chart_payload_schema"] == "PASS", "LEGACY_CHART_SCHEMA_STATUS")
    payload = documents["ois_chart_payload.json"]
    rolling = documents["ois_chart_rolling_180.json"]
    require(rolling["integrity"] == {"counts": dict.fromkeys(payload["datasets"], 180), "dates_synchronized": True, "duplicate_dates": False}, "LEGACY_ROLLING_INTEGRITY_METADATA")
    dates = state_dates(rolling)
    require(dates == sorted(set(dates)) and len(dates) == 180, "LEGACY_ROLLING_DATE_ORDER")
    for dataset in REQUIRED_DATASETS:
        records = payload["datasets"][dataset]
        retained = rolling["datasets"][dataset]
        require(len(retained) == 180 and retained == records[-180:], "LEGACY_ROLLING_CROSS_FILE_MISMATCH")
    expected, _ = make_documents(copy.deepcopy(history), None, False, validation_time, history["quality_flags"], {}, [], False)
    for field in ("datasets", "wti", "brent"):
        require(payload[field] == expected["ois_chart_payload.json"][field], "LEGACY_PAYLOAD_CALCULATION_MISMATCH")
    for name, key in (("WTI", "wti"), ("Brent", "brent")):
        rows = history["commodities"][key]
        validate_history(rows, now, fresh=False)
        require(report[name]["source_date"] == rows[-1]["date"] and report[name]["rows"] == len(rows), "LEGACY_SOURCE_METADATA_MISMATCH")
        require(baseline[f"{name}_source_date"] == rows[-1]["date"], "LEGACY_STATUS_SOURCE_DATE")
        indicators = read_json(root / "data/production" / f"ois_{key}_indicators.json")
        validate_indicators(rows, indicators)
        with (root / "data/production" / f"ois_{key}_clean.csv").open(encoding="utf-8", newline="") as handle:
            csv_rows = list(csv.DictReader(handle))
        require(len(csv_rows) == len(rows), "LEGACY_CSV_COUNT_MISMATCH")
        for csv_row, row in zip(csv_rows, rows):
            require(csv_row["date"] == row["date"] and csv_row["source"] == row["source"] and
                    all(float(csv_row[field]) == row[field] for field in FIELDS), "LEGACY_CSV_HISTORY_MISMATCH")


def validate_bundle(root: Path, now: datetime, *, check_freshness: bool = True) -> None:
    history = read_json(root / STATE_PATH)
    require(history["schema_version"] == "OIS-HISTORY-1.0" and history["snapshot_id"] == digest(history["commodities"]), "HISTORY_HASH_MISMATCH")
    documents = {filename: read_json(root / "data/production" / filename) for filename in PUBLIC_FILES}
    validation_time = now if check_freshness else datetime.fromisoformat(history["generated_at"].replace("Z", "+00:00"))
    validate_documents(documents, history, validation_time)
    expected, _ = make_documents(copy.deepcopy(history), None, False, validation_time, history["quality_flags"], {}, [], False)
    for field in ("datasets", "wti", "brent"):
        require(documents["ois_chart_payload.json"][field] == expected["ois_chart_payload.json"][field], "PAYLOAD_CALCULATION_MISMATCH")
    require(documents["ois_status.json"]["snapshot_id"] == history["snapshot_id"], "SNAPSHOT_HASH_MISMATCH")
    for key, rows in history["commodities"].items():
        indicators = read_json(root / "data/production" / f"ois_{key}_indicators.json")
        validate_indicators(rows, indicators)
        with (root / "data/production" / f"ois_{key}_clean.csv").open(encoding="utf-8", newline="") as handle:
            csv_rows = list(csv.DictReader(handle))
        require(len(csv_rows) == len(rows), "CSV_COUNT_MISMATCH")
        for csv_row, row in zip(csv_rows, rows):
            require(csv_row["date"] == row["date"] and csv_row["source"] == row["source"] and
                    all(float(csv_row[field]) == row[field] for field in FIELDS), "CSV_HISTORY_MISMATCH")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Build and validate an isolated OIS candidate; never writes production.")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, help="Offline replay only; resulting candidate cannot be published")
    parser.add_argument("--as-of", help="Offline replay clock; requires --fixture")
    args = parser.parse_args(argv)
    now = datetime.now(timezone.utc)
    try:
        require(not args.as_of or args.fixture is not None, "REPLAY_CLOCK_REQUIRES_FIXTURE")
        if args.as_of:
            now = datetime.fromisoformat(args.as_of.replace("Z", "+00:00"))
            require(now.tzinfo is not None, "REPLAY_CLOCK_REQUIRES_TIMEZONE")
        result = build_candidate(args.root, args.candidate, now, fixture=args.fixture)
    except Exception as exc:
        # Do not serialize source payloads, URLs, environment variables, or secrets.
        result = {"validation_status": "FAIL", "generated_at": stamp(now),
                  "failure_class": "TRANSIENT" if isinstance(exc, TransientError) else "DATA_INTEGRITY_OR_RUNTIME",
                  "error_code": str(exc) if isinstance(exc, IntegrityError) else type(exc).__name__, "published": False}
    write_json(args.report, result)
    print(json.dumps(result, sort_keys=True))
    return 0 if result["validation_status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
