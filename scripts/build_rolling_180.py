from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_DIR = ROOT / "data" / "production"
PAYLOAD_PATH = PRODUCTION_DIR / "ois_chart_payload.json"
STATE_PATH = PRODUCTION_DIR / "ois_chart_rolling_180.json"
WINDOW = 180
SCHEMA_VERSION = "OIS-ROLLING-180-1.0"
REQUIRED_DATASETS = (
    "wti_price_structure",
    "wti_macd",
    "wti_rsi",
    "brent_price_structure",
    "brent_macd",
    "brent_rsi",
)


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def common_dates(payload: dict) -> list[str]:
    datasets = payload.get("datasets", {})
    date_sets: list[set[str]] = []
    for key in REQUIRED_DATASETS:
        rows = datasets.get(key)
        if not rows:
            raise ValueError(f"missing or empty dataset: {key}")
        dates = [row["date"] for row in rows]
        if len(dates) != len(set(dates)):
            raise ValueError(f"duplicate dates in dataset: {key}")
        date_sets.append(set(dates))

    shared = sorted(set.intersection(*date_sets))
    if len(shared) < WINDOW:
        raise ValueError(f"only {len(shared)} synchronized trading days; need {WINDOW}")
    return shared


def payload_maps(payload: dict) -> dict[str, dict[str, dict]]:
    return {
        key: {row["date"]: row for row in payload["datasets"][key]}
        for key in REQUIRED_DATASETS
    }


def window_dates(payload: dict) -> list[str]:
    return common_dates(payload)[-WINDOW:]


def build_full(payload: dict, mode: str) -> dict:
    dates = window_dates(payload)
    maps = payload_maps(payload)
    stamp = now_utc()
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": stamp,
        "validation_status": "PASS",
        "window_size": WINDOW,
        "update_mode": mode,
        "last_source_date": dates[-1],
        "first_source_date": dates[0],
        "last_full_reconciliation_at": stamp,
        "source_payload_generated_at": payload.get("generated_at"),
        "latest_complete_source_date": payload.get("latest_complete_source_date"),
        "appended_trading_days": 0,
        "dropped_trading_days": 0,
        "integrity": {
            "counts": {key: WINDOW for key in REQUIRED_DATASETS},
            "dates_synchronized": True,
            "duplicate_dates": False,
        },
        "datasets": {
            key: [maps[key][date] for date in dates]
            for key in REQUIRED_DATASETS
        },
    }


def state_dates(state: dict) -> list[str]:
    datasets = state.get("datasets", {})
    expected: list[str] | None = None
    for key in REQUIRED_DATASETS:
        rows = datasets.get(key)
        if not rows or len(rows) != WINDOW:
            raise ValueError(f"rolling state count invalid for {key}")
        dates = [row["date"] for row in rows]
        if len(dates) != len(set(dates)):
            raise ValueError(f"rolling state duplicate dates for {key}")
        if expected is None:
            expected = dates
        elif dates != expected:
            raise ValueError("rolling state datasets are not date-synchronized")
    assert expected is not None
    return expected


def history_was_revised(state: dict, payload: dict) -> bool:
    maps = payload_maps(payload)
    for key in REQUIRED_DATASETS:
        for row in state["datasets"][key]:
            current = maps[key].get(row["date"])
            if current is None or current != row:
                return True
    return False


def full_reconciliation_due(state: dict) -> bool:
    value = state.get("last_full_reconciliation_at")
    if not value:
        return True
    return (datetime.now(timezone.utc) - parse_utc(value)).days >= 7


def build_incremental(state: dict, payload: dict) -> dict:
    existing_dates = state_dates(state)
    shared_dates = common_dates(payload)
    latest_date = shared_dates[-1]
    last_date = state.get("last_source_date")

    if existing_dates[-1] != last_date:
        raise ValueError("last_source_date does not match rolling state endpoint")

    if latest_date < last_date:
        raise ValueError("production latest date is older than rolling state")

    if full_reconciliation_due(state) or history_was_revised(state, payload):
        return build_full(payload, "FULL_RECONCILIATION")

    if latest_date == last_date:
        return state

    new_dates = [date for date in shared_dates if date > last_date]
    if not new_dates:
        raise ValueError("latest date advanced but no new synchronized trading dates found")
    if len(new_dates) >= WINDOW:
        return build_full(payload, "FULL_RECONCILIATION")

    maps = payload_maps(payload)
    keep = WINDOW - len(new_dates)
    updated = {
        key: state["datasets"][key][-keep:] + [maps[key][date] for date in new_dates]
        for key in REQUIRED_DATASETS
    }
    dates = [row["date"] for row in updated[REQUIRED_DATASETS[0]]]
    for key in REQUIRED_DATASETS[1:]:
        if [row["date"] for row in updated[key]] != dates:
            raise ValueError("incremental update produced unsynchronized dates")

    stamp = now_utc()
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": stamp,
        "validation_status": "PASS",
        "window_size": WINDOW,
        "update_mode": "INCREMENTAL_APPEND_DROP",
        "last_source_date": dates[-1],
        "first_source_date": dates[0],
        "last_full_reconciliation_at": state["last_full_reconciliation_at"],
        "source_payload_generated_at": payload.get("generated_at"),
        "latest_complete_source_date": payload.get("latest_complete_source_date"),
        "appended_trading_days": len(new_dates),
        "dropped_trading_days": len(new_dates),
        "integrity": {
            "counts": {key: WINDOW for key in REQUIRED_DATASETS},
            "dates_synchronized": True,
            "duplicate_dates": False,
        },
        "datasets": updated,
    }


def validate_payload(payload: dict) -> None:
    if payload.get("validation_status") != "PASS":
        raise ValueError("production chart payload validation_status is not PASS")
    latest = payload.get("latest_complete_source_date", {})
    if latest.get("wti") != latest.get("brent"):
        raise ValueError("WTI and Brent latest source dates are not synchronized")


def write_state(state: dict) -> None:
    temp = STATE_PATH.with_suffix(".tmp")
    temp.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    temp.replace(STATE_PATH)


def main() -> int:
    payload = load_json(PAYLOAD_PATH)
    validate_payload(payload)

    if not STATE_PATH.exists():
        state = build_full(payload, "FULL_LOAD")
        write_state(state)
        print(json.dumps({"status": "PASS", "mode": "FULL_LOAD", "last_source_date": state["last_source_date"], "window": WINDOW}))
        return 0

    try:
        previous = load_json(STATE_PATH)
        state = build_incremental(previous, payload)
    except (ValueError, KeyError, json.JSONDecodeError):
        state = build_full(payload, "FULL_RECONCILIATION")

    if state != load_json(STATE_PATH):
        write_state(state)

    print(json.dumps({"status": "PASS", "mode": state.get("update_mode", "NO_UPDATE"), "last_source_date": state["last_source_date"], "window": WINDOW}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
