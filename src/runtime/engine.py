from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from scripts.build_rolling_180 import REQUIRED_DATASETS, state_dates
from src.indicators.technical import build_indicators
from src.payload.chart_payload import build_chart_payload
from src.rollover.normalization import check_rollover
from src.sources.adapters import PriceRow
from src.runtime.source import IntegrityError, TransientError, fetch, stamp
from src.runtime.validation import (CHECKS, FIELDS, PUBLIC_FILES, read_json, require,
                                    validate_documents, validate_history, validate_indicators)


CSV_FIELDS = ["date", *FIELDS, "source"]
STATE_PATH = "data/runtime/history.json"


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


def merge_history(previous: list[dict], incoming: list[dict], migrated: bool) -> tuple[list[dict], int]:
    old = {row["date"]: row for row in previous}
    require(len(old) == len(previous), "PREVIOUS_DUPLICATE_DATE")
    require(incoming[-1]["date"] >= previous[-1]["date"], "SOURCE_DATE_REGRESSION")
    overlap = set(old) & {row["date"] for row in incoming}
    require(bool(overlap), "HISTORY_NO_OVERLAP")
    incoming_dates = {row["date"] for row in incoming}
    require(all(day in incoming_dates for day in old if incoming[0]["date"] <= day <= incoming[-1]["date"]),
            "SOURCE_HISTORICAL_SESSION_DISAPPEARED")
    revisions = 0
    for row in incoming:
        day = row["date"]
        if day in old:
            changed = any(row[field] != old[day][field] for field in FIELDS)
            if changed:
                require(not migrated, f"HISTORICAL_SOURCE_REVISION:{day}")
                revisions += 1
            if not migrated:
                old[day] = row
        elif day > previous[-1]["date"]:
            old[day] = row
        elif day >= previous[0]["date"]:
            require(not migrated, f"HISTORICAL_SESSION_INSERTION:{day}")
            old[day] = row
            revisions += 1
        # Do not prepend older observations: EMA seed must never move.
    return [old[day] for day in sorted(old)], revisions


def make_documents(history: dict, previous_rolling: dict | None, migrated: bool,
                   now: datetime, flags: list[str], revisions: dict) -> tuple[dict, dict]:
    commodities = history["commodities"]
    clean, indicators = {}, {}
    for key, rows in commodities.items():
        clean[key] = [{field: row[field] for field in CSV_FIELDS} for row in rows]
        indicators[key] = build_indicators(clean[key])
        validate_indicators(clean[key], indicators[key])
    require(commodities["wti"][-1]["date"] == commodities["brent"][-1]["date"], "LATEST_DATES_UNSYNCHRONIZED")
    now_string = stamp(now)
    common = {
        "runtime_contract_version": "1.0", "generated_at": now_string,
        "data_as_of": commodities["wti"][-1]["date"],
        "source": {key: rows[-1]["source"] for key, rows in commodities.items()},
        "source_timestamp": {key: rows[-1]["source_timestamp"] for key, rows in commodities.items()},
        "snapshot_id": digest(commodities), "validation_status": "PASS",
        "quality_flags": sorted(set(flags)), "missing_fields": [],
        "duplicate_status": "PASS", "freshness_status": "PASS",
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
            for key, rows in datasets.items():
                old = {row["date"]: row for row in previous_rolling["datasets"][key]}
                for row in rows:
                    require(row["date"] not in old or row == old[row["date"]], "ROLLING_HISTORICAL_INDICATOR_DRIFT")
                retained = [row for row in previous_rolling["datasets"][key] if row["date"] >= dates[0]]
                added = [row for row in rows if row["date"] > old_dates[-1]]
                require(retained + added == rows, "PERSISTENT_ROLLING_CONTINUITY")
    rolling = {**common, "schema_version": "OIS-ROLLING-180-1.0", "record_count": 180,
               "window_size": 180, "first_source_date": dates[0], "last_source_date": dates[-1],
               "update_mode": ("INCREMENTAL_APPEND_DROP" if appended else previous_rolling["update_mode"]) if migrated else "FULL_RECONCILIATION",
               "runtime_update_result": "APPENDED" if appended else "NO_NEW_TRADING_DAY",
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


def build_candidate(root: Path, candidate: Path, now: datetime, *, fetcher=fetch, fixture: Path | None = None) -> dict:
    require(candidate.resolve() != root.resolve() and not candidate.exists(), "CANDIDATE_MUST_BE_NEW_DIRECTORY")
    state, rolling, migrated = load_previous(root)
    if migrated:
        validate_bundle(root, now, check_freshness=False)
    commodities, flags, revisions = {}, [], {}
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
        commodities[key], revisions[key] = merge_history(previous, rows, migrated)
        validate_history(commodities[key], now)
        flags.extend(source_flags)
    if not migrated:
        flags.append("LEGACY_BOOTSTRAP_RECONCILIATION")
    history = {"commodities": commodities}
    documents, indicators = make_documents(history, rolling, migrated, now, flags, revisions)
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
    manifest = {"validation_status": "PASS", "generated_at": stamp(now), "base_commit": git(root, "rev-parse", "HEAD"),
                "publishable": fixture is None, "snapshot_id": history["snapshot_id"],
                "files": {str(path.relative_to(candidate)).replace("\\", "/"): hashlib.sha256(path.read_bytes()).hexdigest()
                          for path in sorted((candidate / "data").rglob("*")) if path.is_file()}}
    write_json(candidate / "manifest.json", manifest)
    return {"validation_status": "PASS", "data_as_of": history["data_as_of"], "rolling_count": 180,
            "candidate": str(candidate), "bootstrap_revised_rows": revisions, "publishable": fixture is None}


def validate_bundle(root: Path, now: datetime, *, check_freshness: bool = True) -> None:
    history = read_json(root / STATE_PATH)
    require(history["schema_version"] == "OIS-HISTORY-1.0" and history["snapshot_id"] == digest(history["commodities"]), "HISTORY_HASH_MISMATCH")
    documents = {filename: read_json(root / "data/production" / filename) for filename in PUBLIC_FILES}
    validation_time = now if check_freshness else datetime.fromisoformat(history["generated_at"].replace("Z", "+00:00"))
    validate_documents(documents, history, validation_time)
    expected, _ = make_documents(history.copy(), None, False, validation_time, history["quality_flags"], {})
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
