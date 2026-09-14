from __future__ import annotations

import json
import math
from datetime import date, datetime, timezone
from decimal import Decimal, localcontext
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from src.runtime.source import IntegrityError, latest_completed, schedule


ROOT = Path(__file__).resolve().parents[2]
PUBLIC_FILES = ("ois_status.json", "ois_ingestion_validation.json", "ois_chart_payload.json", "ois_chart_rolling_180.json")
CHECKS = ("freshness", "missing_value", "duplicate", "trading_date", "schema", "type", "range",
          "indicator_calculation", "rolling_count", "cross_file_consistency", "historical_drift")
FIELDS = ("open", "high", "low", "close", "volume")


def require(condition: bool, code: str) -> None:
    if not condition:
        raise IntegrityError(code)


def read_json(path: Path) -> dict:
    def reject(value):
        raise IntegrityError(f"NON_FINITE_JSON:{value}")
    def unique(pairs):
        output = {}
        for key, value in pairs:
            require(key not in output, f"DUPLICATE_JSON_KEY:{key}")
            output[key] = value
        return output
    return json.loads(path.read_text(encoding="utf-8"), parse_constant=reject, object_pairs_hook=unique)


def validate_history(rows: list[dict], now: datetime, *, fresh: bool = True) -> None:
    require(len(rows) >= 370, "INSUFFICIENT_INDICATOR_WARMUP")
    dates = [row["date"] for row in rows]
    require(dates == sorted(set(dates)), "DUPLICATE_OR_UNSORTED_DATE")
    sessions = schedule(dates[0], max(dates[-1], now.date().isoformat()))
    for row in rows:
        day = row["date"]
        require(date.fromisoformat(day).isoformat() == day and day in sessions, f"NON_TRADING_DATE:{day}")
        require(sessions[day] <= now, f"INCOMPLETE_TRADING_DATE:{day}")
        require(isinstance(row.get("source"), str) and bool(row["source"]), "MISSING_SOURCE")
        for field in FIELDS:
            value = row.get(field)
            require(type(value) in (int, float) and math.isfinite(value), f"MISSING_OR_INVALID_TYPE:{day}:{field}")
        require(all(row[key] > 0 for key in ("open", "high", "low", "close")), f"PRICE_RANGE:{day}")
        require(row["low"] <= min(row["open"], row["close"]) <= max(row["open"], row["close"]) <= row["high"], f"OHLC_RANGE:{day}")
        require(row["volume"] >= 0 and int(row["volume"]) == row["volume"], f"VOLUME_RANGE:{day}")
    if fresh:
        require(dates[-1] == latest_completed(now), f"STALE_SOURCE:{dates[-1]}:expected:{latest_completed(now)}")


def reference_indicators(rows: list[dict]) -> list[dict]:
    """Independent Decimal recurrence; never calls the production indicator functions."""
    with localcontext() as ctx:
        ctx.prec = 40
        values = [Decimal(str(row["close"])) for row in rows]
        e12 = e26 = values[0]
        signal = Decimal(0)
        gain = loss = Decimal(0)
        result = []
        for i, close in enumerate(values):
            if i:
                e12 += (close - e12) * Decimal(2) / 13
                e26 += (close - e26) * Decimal(2) / 27
            dif = e12 - e26
            signal += (dif - signal) / 5
            record = {"date": rows[i]["date"], "close": close, "dif": dif, "dea": signal, "histogram": dif - signal}
            for period in (20, 60, 120):
                record[f"ma{period}"] = sum(values[i-period+1:i+1]) / period if i >= period-1 else None
            if i:
                delta = close - values[i-1]
                up, down = max(delta, Decimal(0)), max(-delta, Decimal(0))
                if i <= 14:
                    gain += up / 14
                    loss += down / 14
                else:
                    gain = (gain * 13 + up) / 14
                    loss = (loss * 13 + down) / 14
            record["rsi14"] = (Decimal(100) if loss == 0 else 100 - 100 / (1 + gain / loss)) if i >= 14 else None
            result.append({key: round(float(value), 6) if isinstance(value, Decimal) else value for key, value in record.items()})
        return result


def validate_indicators(rows: list[dict], actual: dict) -> None:
    expected = reference_indicators(rows)
    require(actual["rows"] == len(rows) and len(actual["data"]) == len(rows), "INDICATOR_COUNT")
    for ref, got in zip(expected, actual["data"]):
        require(set(got) == set(ref), "INDICATOR_FIELDS")
        for field, value in ref.items():
            if value is None or isinstance(value, str):
                require(got[field] == value, f"INDICATOR_VALUE:{field}")
            else:
                require(type(got[field]) in (float, int) and math.isfinite(got[field]) and abs(value - got[field]) <= 0.0000011,
                        f"INDICATOR_DRIFT:{ref['date']}:{field}")
    require(actual["latest"] == actual["data"][-1], "INDICATOR_LATEST")


def validate_documents(documents: dict[str, dict], history: dict, now: datetime) -> None:
    schema = read_json(ROOT / "schemas" / "ois_runtime.schema.json")
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    baseline = documents["ois_status.json"]
    common = ("runtime_contract_version", "generated_at", "data_as_of", "source", "source_timestamp", "snapshot_id",
              "validation_status", "quality_flags", "missing_fields", "duplicate_status", "freshness_status")
    for filename in PUBLIC_FILES:
        document = documents[filename]
        errors = sorted(validator.iter_errors(document), key=lambda error: str(error.path))
        require(not errors, f"SCHEMA:{filename}:" + (errors[0].message if errors else ""))
        require(all(document[key] == baseline[key] for key in common), f"CROSS_FILE_METADATA:{filename}")
    require(baseline["data_as_of"] == latest_completed(now), "CROSS_FILE_FRESHNESS")
    require(datetime.fromisoformat(baseline["generated_at"].replace("Z", "+00:00")) <= now, "GENERATED_AT_FUTURE")
    for ts in baseline["source_timestamp"].values():
        require(datetime.fromisoformat(ts.replace("Z", "+00:00")) <= now, "SOURCE_TIMESTAMP_FUTURE")
    status = documents["ois_status.json"]
    report = documents["ois_ingestion_validation.json"]
    payload = documents["ois_chart_payload.json"]
    rolling = documents["ois_chart_rolling_180.json"]
    require(status["status"] == status["validation"] == report["overall_validation"] == "PASS", "LEGACY_STATUS_MISMATCH")
    require(report["checks"] == dict.fromkeys(CHECKS, "PASS"), "VALIDATION_CHECKS")
    require(report["chart_payload_schema"] == "PASS", "CHART_SCHEMA_STATUS")
    require(status["last_successful_update"] == report["last_successful_update"] == status["generated_at"], "UPDATE_TIMESTAMP_MISMATCH")
    require(rolling["source_payload_generated_at"] == payload["generated_at"], "ROLLING_PAYLOAD_GENERATION")
    dates = None
    for name, key in (("WTI", "wti"), ("Brent", "brent")):
        rows = history["commodities"][key]
        validate_history(rows, now)
        for obj in (payload[key], report[name]):
            require(obj["source_date"] == baseline["data_as_of"] and obj["rows"] == len(rows), "SOURCE_METADATA_MISMATCH")
        require(status[f"{name}_source_date"] == rows[-1]["date"], "STATUS_SOURCE_DATE")
        require(payload["latest_complete_source_date"][key] == rolling["latest_complete_source_date"][key] == rows[-1]["date"], "LATEST_SOURCE_DATE")
        for suffix in ("price_structure", "macd", "rsi"):
            dataset = f"{key}_{suffix}"
            records = payload["datasets"][dataset]
            require([row["date"] for row in records] == [row["date"] for row in rows[-250:]], "PAYLOAD_DATE_ALIGNMENT")
            retained = rolling["datasets"][dataset]
            require(len(retained) == 180 and retained == records[-180:], "ROLLING_CROSS_FILE_MISMATCH")
            current_dates = [row["date"] for row in retained]
            require(current_dates == sorted(set(current_dates)), "ROLLING_DUPLICATE_OR_ORDER")
            if dates is None:
                dates = current_dates
            require(dates == current_dates, "ROLLING_UNSYNCHRONIZED_DATES")
    require(rolling["first_source_date"] == dates[0] and rolling["last_source_date"] == dates[-1], "ROLLING_ENDPOINT")
    require(rolling["integrity"] == {"counts": dict.fromkeys(payload["datasets"], 180), "dates_synchronized": True, "duplicate_dates": False}, "ROLLING_INTEGRITY_METADATA")
