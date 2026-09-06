from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

from src.indicators.technical import build_indicators
from src.payload.chart_payload import build_chart_payload, validate_chart_payload_schema
from src.rollover.normalization import check_rollover
from src.sources.adapters import PriceRow, SourceRouter
from src.validation.quality import validate_rows


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
STAGING_DIR = DATA_DIR / "staging"
PRODUCTION_DIR = DATA_DIR / "production"
ARCHIVE_DIR = DATA_DIR / "archive"
LOG_DIR = ROOT / "logs"

WtiTicker = "CL=F"
BrentTicker = "BZ=F"
MIN_ROWS = 250
PREFERRED_RANGE = "2y"
PRODUCTION_MAX_ROWS = 750
CSV_FIELDS = ["date", "open", "high", "low", "close", "volume", "source"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="OIS Technical Data Engine updater")
    parser.add_argument("--initialize", action="store_true", help="Fetch baseline history and publish if valid.")
    parser.add_argument("--force", action="store_true", help="Re-fetch and republish even when no new date is found.")
    parser.add_argument("--validate-only", action="store_true", help="Validate current production files only.")
    args = parser.parse_args(argv)

    ensure_dirs()
    log_event("start", {"args": vars(args)})
    try:
        if args.validate_only:
            result = validate_only()
        else:
            result = run_update(initialize=args.initialize, force=args.force)
        log_event("finish", result)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("overall_validation") in {"PASS", "NO_UPDATE"} else 1
    except Exception as exc:  # noqa: BLE001 - command boundary reports every fatal failure.
        log_event("fatal_error", {"error": f"{type(exc).__name__}: {exc}"})
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


def run_update(initialize: bool = False, force: bool = False) -> dict:
    router = SourceRouter()
    wti_result = router.fetch_history(WtiTicker, min_rows=MIN_ROWS, range_=PREFERRED_RANGE)
    brent_result = router.fetch_history(BrentTicker, min_rows=MIN_ROWS, range_=PREFERRED_RANGE)
    failures = [res.error for res in (wti_result, brent_result) if res.error]
    if failures:
        write_failure_validation(failures)
        return {"overall_validation": "FAIL", "failure_reason": "; ".join(failures)}

    wti_rows = trim_rows(wti_result.rows)
    brent_rows = trim_rows(brent_result.rows)
    previous_wti = read_production_csv("ois_wti_clean.csv")
    previous_brent = read_production_csv("ois_brent_clean.csv")

    if not initialize and not force and previous_wti and previous_brent:
        latest_existing = (previous_wti[-1]["date"], previous_brent[-1]["date"])
        latest_incoming = (wti_rows[-1].date, brent_rows[-1].date)
        if latest_incoming <= latest_existing:
            validation = build_validation(
                rows_by_commodity={
                    "WTI": price_rows_from_dicts(previous_wti),
                    "Brent": price_rows_from_dicts(previous_brent),
                },
                last_attempt=_now_utc(),
                failure_reason="NO_NEW_COMPLETE_TRADING_DAY",
            )
            validation["overall_validation"] = "NO_UPDATE"
            write_json(PRODUCTION_DIR / "ois_ingestion_validation.json", validation)
            write_status(validation)
            return {
                "overall_validation": "NO_UPDATE",
                "reason": "NO_NEW_COMPLETE_TRADING_DAY",
                "latest_complete_source_date": {"WTI": latest_existing[0], "Brent": latest_existing[1]},
            }

    validation = build_validation(
        rows_by_commodity={"WTI": wti_rows, "Brent": brent_rows},
        last_attempt=_now_utc(),
        failure_reason=None,
    )
    if validation["overall_validation"] != "PASS":
        write_json(PRODUCTION_DIR / "ois_ingestion_validation.json", validation)
        write_status(validation)
        return validation

    clean_wti = [row.to_csv_dict() for row in wti_rows]
    clean_brent = [row.to_csv_dict() for row in brent_rows]
    wti_indicators = build_indicators(clean_wti)
    brent_indicators = build_indicators(clean_brent)
    payload = build_chart_payload(clean_wti, clean_brent, wti_indicators, brent_indicators, validation)
    schema_ok, schema_errors = validate_chart_payload_schema(payload)
    if not schema_ok:
        validation["overall_validation"] = "FAIL"
        validation["failure_reason"] = f"chart payload schema failed: {schema_errors}"
        write_json(PRODUCTION_DIR / "ois_ingestion_validation.json", validation)
        write_status(validation)
        return validation

    validation["chart_payload_schema"] = "PASS"
    validation["last_successful_update"] = _now_utc()
    publish_atomic(
        {
            "ois_wti_clean.csv": clean_wti,
            "ois_brent_clean.csv": clean_brent,
            "ois_wti_indicators.json": wti_indicators,
            "ois_brent_indicators.json": brent_indicators,
            "ois_chart_payload.json": payload,
            "ois_ingestion_validation.json": validation,
        }
    )
    write_status(validation)
    archive_snapshot(validation["last_successful_update"][:10])
    return {
        "overall_validation": "PASS",
        "WTI_rows": len(clean_wti),
        "Brent_rows": len(clean_brent),
        "WTI_latest_source_date": clean_wti[-1]["date"],
        "Brent_latest_source_date": clean_brent[-1]["date"],
        "chart_payload_schema": "PASS",
    }


def validate_only() -> dict:
    wti_rows = price_rows_from_dicts(read_production_csv("ois_wti_clean.csv"))
    brent_rows = price_rows_from_dicts(read_production_csv("ois_brent_clean.csv"))
    validation = build_validation(
        rows_by_commodity={"WTI": wti_rows, "Brent": brent_rows},
        last_attempt=_now_utc(),
        failure_reason=None,
    )
    payload_path = PRODUCTION_DIR / "ois_chart_payload.json"
    if payload_path.exists():
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
        ok, errors = validate_chart_payload_schema(payload)
        validation["chart_payload_schema"] = "PASS" if ok else "FAIL"
        if errors:
            validation["chart_payload_schema_errors"] = errors
            validation["overall_validation"] = "FAIL"
    else:
        validation["chart_payload_schema"] = "MISSING"
        validation["overall_validation"] = "FAIL"
    write_json(PRODUCTION_DIR / "ois_ingestion_validation.json", validation)
    write_status(validation)
    return validation


def build_validation(
    rows_by_commodity: dict[str, list[PriceRow]],
    last_attempt: str,
    failure_reason: str | None,
) -> dict:
    reports = {}
    overall = "PASS"
    for commodity, rows in rows_by_commodity.items():
        quality = validate_rows(rows, min_rows=MIN_ROWS, expected_source=rows[-1].source if rows else None)
        rollover = check_rollover(rows, commodity)
        if quality.validation_status == "FAIL" or rollover.validation_status == "FAIL":
            overall = "FAIL"
        data = quality.to_dict()
        data.update(
            {
                "rollover_detected": rollover.rollover_detected,
                "rollover_validation": rollover.to_dict(),
                "cross_check_status": "NOT_CONFIGURED",
            }
        )
        reports[commodity] = data

    return {
        "schema_version": "OIS-VALIDATION-1.0",
        "generated_at": _now_utc(),
        "WTI": reports["WTI"],
        "Brent": reports["Brent"],
        "overall_validation": overall,
        "last_successful_update": read_last_successful_update(),
        "last_attempt": last_attempt,
        "failure_reason": failure_reason,
    }


def write_failure_validation(failures: list[str]) -> None:
    validation = {
        "schema_version": "OIS-VALIDATION-1.0",
        "generated_at": _now_utc(),
        "WTI": {},
        "Brent": {},
        "overall_validation": "FAIL",
        "last_successful_update": read_last_successful_update(),
        "last_attempt": _now_utc(),
        "failure_reason": "; ".join(failures),
    }
    write_json(PRODUCTION_DIR / "ois_ingestion_validation.json", validation)
    write_status(validation)


def publish_atomic(files: dict[str, list[dict] | dict]) -> None:
    staging_run = STAGING_DIR / f"publish-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    staging_run.mkdir(parents=True, exist_ok=False)
    try:
        for filename, content in files.items():
            path = staging_run / filename
            if filename.endswith(".csv"):
                write_csv(path, content)  # type: ignore[arg-type]
            else:
                write_json(path, content)  # type: ignore[arg-type]
        for path in staging_run.iterdir():
            os.replace(path, PRODUCTION_DIR / path.name)
    finally:
        shutil.rmtree(staging_run, ignore_errors=True)


def write_status(validation: dict) -> None:
    wti = validation.get("WTI", {})
    brent = validation.get("Brent", {})
    status = {
        "service": "OIS Technical Data Engine",
        "status": validation.get("overall_validation"),
        "last_successful_update": validation.get("last_successful_update"),
        "WTI_source_date": wti.get("source_date"),
        "Brent_source_date": brent.get("source_date"),
        "validation": validation.get("overall_validation"),
        "schema_version": "OIS-STATUS-1.0",
    }
    write_json(PRODUCTION_DIR / "ois_status.json", status)


def archive_snapshot(day: str) -> None:
    archive_dir = ARCHIVE_DIR / day
    archive_dir.mkdir(parents=True, exist_ok=True)
    for path in PRODUCTION_DIR.iterdir():
        if path.is_file():
            shutil.copy2(path, archive_dir / path.name)


def read_last_successful_update() -> str | None:
    status_path = PRODUCTION_DIR / "ois_status.json"
    if not status_path.exists():
        return None
    try:
        return json.loads(status_path.read_text(encoding="utf-8")).get("last_successful_update")
    except json.JSONDecodeError:
        return None


def trim_rows(rows: list[PriceRow]) -> list[PriceRow]:
    return rows[-PRODUCTION_MAX_ROWS:]


def read_production_csv(filename: str) -> list[dict]:
    path = PRODUCTION_DIR / filename
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def price_rows_from_dicts(rows: list[dict]) -> list[PriceRow]:
    output = []
    for row in rows:
        output.append(
            PriceRow(
                date=row["date"],
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=int(float(row["volume"])),
                source=row["source"],
                contract_info="continuous_front_month",
            )
        )
    return output


def write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def ensure_dirs() -> None:
    for path in [STAGING_DIR, PRODUCTION_DIR, ARCHIVE_DIR, LOG_DIR]:
        path.mkdir(parents=True, exist_ok=True)


def log_event(event: str, payload: dict) -> None:
    ensure_dirs()
    entry = {"timestamp": _now_utc(), "event": event, **payload}
    with (LOG_DIR / "ois_update.log").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())

