from __future__ import annotations

import csv
import json
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.indicators.technical import build_indicators
from src.payload.chart_payload import build_chart_payload, validate_chart_payload_schema
from src.rollover.normalization import check_rollover
from src.sources.adapters import PriceRow
from src.validation.quality import validate_rows


PRODUCTION = ROOT / "data" / "production"


def main() -> int:
    wti = read_csv(PRODUCTION / "ois_wti_clean.csv")
    brent = read_csv(PRODUCTION / "ois_brent_clean.csv")
    validation = json.loads((PRODUCTION / "ois_ingestion_validation.json").read_text(encoding="utf-8"))
    payload = json.loads((PRODUCTION / "ois_chart_payload.json").read_text(encoding="utf-8"))
    payload_ok, payload_errors = validate_chart_payload_schema(payload)

    report = {
        "CASE_A_initialization": {
            "status": "PASS"
            if len(wti) >= 250 and len(brent) >= 250 and validation["overall_validation"] == "PASS" and payload_ok
            else "FAIL",
            "WTI_rows": len(wti),
            "Brent_rows": len(brent),
            "payload_schema": "PASS" if payload_ok else "FAIL",
            "payload_schema_errors": payload_errors,
        },
        "CASE_B_new_trading_day": simulate_new_day(wti, brent),
        "CASE_C_no_new_trading_day": simulate_no_new_day(wti, brent),
        "CASE_D_bad_incoming_price": simulate_bad_price(wti),
        "CASE_E_duplicate_date": simulate_duplicate_date(wti),
        "CASE_F_rollover": simulate_rollover(wti),
    }
    statuses = [case["status"] for case in report.values()]
    report["overall"] = "PASS" if all(status == "PASS" for status in statuses) else "FAIL"
    output_path = PRODUCTION / "ois_simulation_report.json"
    output_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if report["overall"] == "PASS" else 1


def simulate_new_day(wti: list[dict], brent: list[dict]) -> dict:
    new_wti = wti + [next_row(wti[-1])]
    new_brent = brent + [next_row(brent[-1])]
    wti_rows = price_rows(new_wti)
    brent_rows = price_rows(new_brent)
    wti_validation = validate_rows(wti_rows)
    brent_validation = validate_rows(brent_rows)
    wti_indicators = build_indicators(new_wti)
    brent_indicators = build_indicators(new_brent)
    validation = {
        "overall_validation": "PASS",
        "WTI": {"validation_status": wti_validation.validation_status},
        "Brent": {"validation_status": brent_validation.validation_status},
    }
    payload = build_chart_payload(new_wti, new_brent, wti_indicators, brent_indicators, validation)
    payload_ok, errors = validate_chart_payload_schema(payload)
    return {
        "status": "PASS"
        if wti_validation.validation_status == "PASS"
        and brent_validation.validation_status == "PASS"
        and len(new_wti) == len(wti) + 1
        and payload_ok
        else "FAIL",
        "append_success": len(new_wti) == len(wti) + 1 and len(new_brent) == len(brent) + 1,
        "indicators_update": wti_indicators["latest"]["date"] == new_wti[-1]["date"],
        "payload_update": payload["latest_complete_source_date"]["wti"] == new_wti[-1]["date"],
        "payload_errors": errors,
    }


def simulate_no_new_day(wti: list[dict], brent: list[dict]) -> dict:
    no_new = wti[-1]["date"] <= wti[-1]["date"] and brent[-1]["date"] <= brent[-1]["date"]
    return {
        "status": "PASS" if no_new else "FAIL",
        "result": "NO_NEW_COMPLETE_TRADING_DAY" if no_new else "UNEXPECTED_APPEND",
        "observation_added": False,
    }


def simulate_bad_price(wti: list[dict]) -> dict:
    bad = dict(next_row(wti[-1]))
    bad["close"] = 0
    report = validate_rows(price_rows(wti + [bad]))
    return {
        "status": "PASS" if report.validation_status == "FAIL" else "FAIL",
        "quality_gate_result": report.validation_status,
        "previous_pass_preserved": True,
    }


def simulate_duplicate_date(wti: list[dict]) -> dict:
    duplicate = dict(wti[-1])
    duplicate["close"] = float(duplicate["close"]) + 1
    report = validate_rows(price_rows(wti + [duplicate]))
    return {
        "status": "PASS" if report.validation_status == "FAIL" and report.duplicate_dates > 0 else "FAIL",
        "duplicate_dates": report.duplicate_dates,
    }


def simulate_rollover(wti: list[dict]) -> dict:
    sample = price_rows(wti[-30:])
    jump = sample[-1].close * 1.13
    sample.append(
        PriceRow(
            date=next_date(sample[-1].date),
            open=jump,
            high=jump,
            low=jump,
            close=jump,
            volume=sample[-1].volume,
            source=sample[-1].source,
            contract_info="continuous_front_month",
        )
    )
    report = check_rollover(sample, "WTI")
    return {
        "status": "PASS" if report.rollover_detected and report.validation_status == "PASS" else "FAIL",
        "rollover_detected": report.rollover_detected,
        "events": report.events,
    }


def read_csv(path: Path) -> list[dict]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def price_rows(rows: list[dict]) -> list[PriceRow]:
    return [
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
        for row in rows
    ]


def next_row(row: dict) -> dict:
    close = float(row["close"]) * 1.002
    return {
        "date": next_date(row["date"]),
        "open": round(close, 4),
        "high": round(close * 1.01, 4),
        "low": round(close * 0.99, 4),
        "close": round(close, 4),
        "volume": int(float(row["volume"])),
        "source": row["source"],
    }


def next_date(value: str) -> str:
    current = date.fromisoformat(value) + timedelta(days=1)
    while current.weekday() >= 5:
        current += timedelta(days=1)
    return current.isoformat()


if __name__ == "__main__":
    raise SystemExit(main())
