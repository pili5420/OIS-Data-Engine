
from __future__ import annotations

import argparse
import json
from datetime import datetime, time
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from src.runtime.engine import write_json
from src.runtime.source import IntegrityError
from src.runtime.validation import read_json, require
from src.work_state import PERSISTENT_STATE_SSOT_PATH, SYSTEM, load_current_state, sha256_hex, validate_production_persistent_state_ssot

EXECUTION_LAYER_PATH = Path("data/execution/ois/OIS_EXECUTION_DATA_LAYER.json")
EXECUTION_LAYER_SCHEMA_VERSION = "OIS-EXECUTION-DATA-LAYER-1.0"
REQUIRED_EXECUTION_SYMBOLS = ("00642U", "00715L", "00673R")
TAIPEI = ZoneInfo("Asia/Taipei")
APPROVED_EXECUTION_SOURCE_IDS = frozenset({"TWSE_INTRADAY_EXECUTION_PRICE", "TPEX_INTRADAY_EXECUTION_PRICE"})
DECISION_CADENCES = {
    "OIS_0935_OPENING": {"label": "09:35", "decision_time": time(9, 35), "freshness_threshold_seconds": 900},
    "OIS_1205_MIDDAY": {"label": "12:05", "decision_time": time(12, 5), "freshness_threshold_seconds": 900},
}


def parse_market_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=TAIPEI)
    return parsed.astimezone(TAIPEI)


def decision_timestamp(*, trade_date: str, decision_cadence: str) -> datetime:
    require(decision_cadence in DECISION_CADENCES, f"OIS_EXECUTION_DECISION_CADENCE:{decision_cadence}")
    try:
        date_value = datetime.strptime(trade_date, "%Y-%m-%d").date()
    except ValueError as exc:
        raise IntegrityError(f"OIS_EXECUTION_TRADE_DATE:{trade_date}") from exc
    return datetime.combine(date_value, DECISION_CADENCES[decision_cadence]["decision_time"], tzinfo=TAIPEI)


def execution_source_binding(ssot: Mapping[str, Any], state: Mapping[str, Any]) -> dict:
    current = ssot["current_state"]
    authoritative = ssot["authoritative_production_pointer"]
    return {
        "persistent_state_ssot_path": PERSISTENT_STATE_SSOT_PATH.as_posix(),
        "persistent_state_ssot_sha256": ssot.get("manifest_sha256"),
        "current_state_id": current["state_id"],
        "current_state_hash": current["state_hash"],
        "current_state_production_snapshot_id": current["production_snapshot_id"],
        "production_snapshot_id": authoritative["production_snapshot_id"],
        "technical_source_as_of": authoritative["source_as_of"],
        "work_trading_date": state.get("trading_date"),
        "source_run_id": authoritative["run_id"],
        "source_commit_sha": authoritative["commit_sha"],
        "current_state_transition_pending": current["production_snapshot_id"] != authoritative["production_snapshot_id"],
    }


def approved_source_metadata(record: Mapping[str, Any]) -> dict | None:
    metadata = record.get("source_metadata")
    if not isinstance(metadata, Mapping):
        return None
    if metadata.get("approved") is not True:
        return None
    source_id = metadata.get("source_id")
    if source_id not in APPROVED_EXECUTION_SOURCE_IDS:
        return None
    source_name = metadata.get("source_name")
    retrieval_timestamp = metadata.get("retrieval_timestamp")
    if not isinstance(source_name, str) or not source_name:
        return None
    if parse_market_timestamp(retrieval_timestamp) is None:
        return None
    return {
        "source_id": source_id,
        "source_name": source_name,
        "source_url": metadata.get("source_url"),
        "retrieval_timestamp": retrieval_timestamp,
        "approved": True,
    }


def evaluate_execution_market_record(record: Mapping[str, Any] | None, *, symbol: str, decision_cadence: str, decision_trade_date: str) -> dict:
    if record is None:
        return {"validation_status": "BLOCKED", "execution_gate": "OFFICIAL_EXECUTION_MARKET_DATA_MISSING", "fail_closed_reason": "MISSING_EXECUTION_MARKET_DATA"}
    if record.get("symbol") != symbol:
        return {"validation_status": "BLOCKED", "execution_gate": "SYMBOL_MISMATCH", "fail_closed_reason": "SYMBOL_MISMATCH"}
    trade_date = record.get("trade_date")
    if not isinstance(trade_date, str) or len(trade_date) < 10:
        return {"validation_status": "BLOCKED", "execution_gate": "TRADE_DATE_MISSING", "fail_closed_reason": "TRADE_DATE_MISSING"}
    if trade_date != decision_trade_date:
        return {"validation_status": "BLOCKED", "execution_gate": "STALE_DAY", "fail_closed_reason": "TRADE_DATE_MISMATCH"}
    market_timestamp = parse_market_timestamp(record.get("market_timestamp"))
    if market_timestamp is None:
        return {"validation_status": "BLOCKED", "execution_gate": "MARKET_TIMESTAMP_INVALID", "fail_closed_reason": "MARKET_TIMESTAMP_INVALID"}
    decision_at = decision_timestamp(trade_date=decision_trade_date, decision_cadence=decision_cadence)
    freshness_seconds = int((decision_at - market_timestamp).total_seconds())
    if freshness_seconds < 0:
        return {"validation_status": "BLOCKED", "execution_gate": "FUTURE_TIMESTAMP", "fail_closed_reason": "MARKET_TIMESTAMP_AFTER_DECISION_TIME", "freshness_seconds": freshness_seconds}
    threshold = DECISION_CADENCES[decision_cadence]["freshness_threshold_seconds"]
    if freshness_seconds > threshold:
        return {"validation_status": "BLOCKED", "execution_gate": "STALE_QUOTE", "fail_closed_reason": "FRESHNESS_THRESHOLD_EXCEEDED", "freshness_seconds": freshness_seconds}
    last_price = record.get("last_price")
    if not isinstance(last_price, (int, float)) or last_price <= 0:
        return {"validation_status": "BLOCKED", "execution_gate": "PRICE_MISSING", "fail_closed_reason": "LAST_PRICE_MISSING_OR_INVALID", "freshness_seconds": freshness_seconds}
    metadata = approved_source_metadata(record)
    if metadata is None:
        return {"validation_status": "BLOCKED", "execution_gate": "UNAPPROVED_SOURCE", "fail_closed_reason": "SOURCE_METADATA_NOT_APPROVED", "freshness_seconds": freshness_seconds}
    if record.get("tradable") is not True:
        return {"validation_status": "BLOCKED", "execution_gate": "NOT_TRADABLE", "fail_closed_reason": "INSTRUMENT_NOT_TRADABLE", "freshness_seconds": freshness_seconds, "trade_date": trade_date, "market_timestamp": market_timestamp.isoformat(), "last_price": float(last_price), "tradable": False, "approved_source_metadata": metadata}
    return {
        "validation_status": "PASS",
        "execution_gate": "PASS",
        "fail_closed_reason": None,
        "trade_date": trade_date,
        "market_timestamp": market_timestamp.isoformat(),
        "last_price": float(last_price),
        "freshness_seconds": freshness_seconds,
        "decision_cadence": decision_cadence,
        "decision_timestamp": decision_at.isoformat(),
        "freshness_threshold_seconds": threshold,
        "tradable": True,
        "approved_source_metadata": metadata,
    }


def build_execution_data_layer(root: Path, official_execution_market_data: Mapping[str, Mapping[str, Any]] | None = None,
                               *, decision_cadence: str = "OIS_0935_OPENING", decision_trade_date: str | None = None) -> dict:
    root = root.resolve()
    state = load_current_state(root)
    require(state is not None, "OIS_EXECUTION_CURRENT_STATE_MISSING")
    ssot = validate_production_persistent_state_ssot(root)
    ssot_path = root / PERSISTENT_STATE_SSOT_PATH
    manifest_hash = sha256_hex(read_json(ssot_path)) if ssot_path.exists() else None
    ssot = dict(ssot)
    ssot["manifest_sha256"] = manifest_hash
    binding = execution_source_binding(ssot, state)
    authoritative = ssot["authoritative_production_pointer"]
    decision_trade_date = decision_trade_date or state.get("trading_date")
    require(isinstance(decision_trade_date, str) and len(decision_trade_date) >= 10, "OIS_EXECUTION_DECISION_TRADE_DATE")
    decision_at = decision_timestamp(trade_date=decision_trade_date, decision_cadence=decision_cadence)
    official_execution_market_data = official_execution_market_data or {}
    instruments = []
    pass_count = 0
    tradable_count = 0
    for symbol in REQUIRED_EXECUTION_SYMBOLS:
        market_record = official_execution_market_data.get(symbol)
        evaluation = evaluate_execution_market_record(market_record, symbol=symbol, decision_cadence=decision_cadence, decision_trade_date=decision_trade_date)
        status = evaluation["validation_status"]
        if status == "PASS":
            pass_count += 1
            if evaluation["tradable"] is True:
                tradable_count += 1
        instrument = {
            "symbol": symbol,
            "instrument_scope": "OIS_EXECUTION_TARGET",
            "validation_status": status,
            "execution_gate": evaluation["execution_gate"],
            "fail_closed_reason": evaluation.get("fail_closed_reason"),
            "trade_date": evaluation.get("trade_date", market_record.get("trade_date") if isinstance(market_record, Mapping) else decision_trade_date),
            "market_timestamp": evaluation.get("market_timestamp", market_record.get("market_timestamp") if isinstance(market_record, Mapping) else None),
            "last_price": evaluation.get("last_price", market_record.get("last_price") if isinstance(market_record, Mapping) else None),
            "freshness_seconds": evaluation.get("freshness_seconds"),
            "freshness_threshold_seconds": evaluation.get("freshness_threshold_seconds", DECISION_CADENCES[decision_cadence]["freshness_threshold_seconds"]),
            "decision_cadence": decision_cadence,
            "decision_timestamp": decision_at.isoformat(),
            "tradable": evaluation.get("tradable", False),
            "approved_source_metadata": evaluation.get("approved_source_metadata"),
            "source_binding": binding,
            "fallback_used": False,
            "fixture_used": False,
            "desktop_data_used": False,
        }
        instruments.append(instrument)
    if pass_count == len(REQUIRED_EXECUTION_SYMBOLS):
        layer_status = "PASS"
    elif pass_count > 0:
        layer_status = "PARTIAL"
    else:
        layer_status = "BLOCKED"
    return {
        "system": SYSTEM,
        "execution_layer_schema_version": EXECUTION_LAYER_SCHEMA_VERSION,
        "required_symbols": list(REQUIRED_EXECUTION_SYMBOLS),
        "validation_status": layer_status,
        "publishable": pass_count > 0,
        "decision_cadence": decision_cadence,
        "decision_trade_date": decision_trade_date,
        "decision_timestamp": decision_at.isoformat(),
        "execution_price_gate": layer_status,
        "pass_count": pass_count,
        "blocked_count": len(REQUIRED_EXECUTION_SYMBOLS) - pass_count,
        "tradable_count": tradable_count,
        "execution_market_data_source": "APPROVED_INTRADAY_EXECUTION_PRICE_SOURCE" if pass_count else None,
        "fail_closed_reason": None if pass_count else "NO_SYMBOL_PASSED_EXECUTION_PRICE_GATE",
        "authoritative_production_binding": {
            "production_snapshot_id": authoritative["production_snapshot_id"],
            "run_id": authoritative["run_id"],
            "commit_sha": authoritative["commit_sha"],
            "source_as_of": authoritative["source_as_of"],
            "persistent_state_ssot_sha256": manifest_hash,
            "current_state_transition_pending": binding["current_state_transition_pending"],
        },
        "technical_source_as_of_binding_required": False,
        "state_ledger_ssot_binding": "PASS",
        "strategy_modified": False,
        "rolling_180_modified": False,
        "six_chart_renderer_modified": False,
        "instruments": instruments,
        "no_fallback_confirmation": "PASS",
    }


def validate_execution_data_layer(doc: Mapping[str, Any]) -> None:
    require(doc.get("system") == SYSTEM, "OIS_EXECUTION_SYSTEM")
    require(doc.get("execution_layer_schema_version") == EXECUTION_LAYER_SCHEMA_VERSION, "OIS_EXECUTION_SCHEMA_VERSION")
    require(tuple(doc.get("required_symbols", [])) == REQUIRED_EXECUTION_SYMBOLS, "OIS_EXECUTION_REQUIRED_SYMBOLS")
    require(doc.get("decision_cadence") in DECISION_CADENCES, "OIS_EXECUTION_DECISION_CADENCE")
    require(doc.get("technical_source_as_of_binding_required") is False, "OIS_EXECUTION_TECHNICAL_SOURCE_BOUND")
    authoritative = doc.get("authoritative_production_binding")
    require(isinstance(authoritative, dict) and authoritative.get("production_snapshot_id") and authoritative.get("run_id") and authoritative.get("commit_sha"), "OIS_EXECUTION_AUTHORITATIVE_BINDING")
    instruments = doc.get("instruments")
    require(isinstance(instruments, list) and len(instruments) == len(REQUIRED_EXECUTION_SYMBOLS), "OIS_EXECUTION_INSTRUMENT_COUNT")
    seen = [item.get("symbol") for item in instruments if isinstance(item, dict)]
    require(tuple(seen) == REQUIRED_EXECUTION_SYMBOLS, "OIS_EXECUTION_SYMBOL_ORDER")
    pass_count = 0
    for item in instruments:
        symbol = item.get("symbol")
        require(item.get("fallback_used") is False, f"OIS_EXECUTION_FALLBACK:{symbol}")
        require(item.get("fixture_used") is False, f"OIS_EXECUTION_FIXTURE:{symbol}")
        require(item.get("desktop_data_used") is False, f"OIS_EXECUTION_DESKTOP:{symbol}")
        binding = item.get("source_binding")
        require(isinstance(binding, dict) and binding.get("current_state_id") and binding.get("current_state_hash"), f"OIS_EXECUTION_BINDING:{symbol}")
        require(binding.get("production_snapshot_id") == authoritative.get("production_snapshot_id"), f"OIS_EXECUTION_BINDING_SNAPSHOT:{symbol}")
        require(binding.get("source_run_id") == authoritative.get("run_id"), f"OIS_EXECUTION_BINDING_RUN:{symbol}")
        require(binding.get("source_commit_sha") == authoritative.get("commit_sha"), f"OIS_EXECUTION_BINDING_COMMIT:{symbol}")
        require(binding.get("technical_source_as_of") == authoritative.get("source_as_of"), f"OIS_EXECUTION_BINDING_SOURCE_AS_OF:{symbol}")
        require(binding.get("persistent_state_ssot_sha256") == authoritative.get("persistent_state_ssot_sha256"), f"OIS_EXECUTION_BINDING_SSOT_HASH:{symbol}")
        for field in ("trade_date", "market_timestamp", "last_price", "freshness_seconds", "decision_cadence", "tradable"):
            require(field in item, f"OIS_EXECUTION_FIELD_REQUIRED:{symbol}:{field}")
        if item.get("validation_status") == "PASS":
            pass_count += 1
            require(item.get("execution_gate") == "PASS", f"OIS_EXECUTION_PASS_GATE:{symbol}")
            require(item.get("trade_date") == doc.get("decision_trade_date"), f"OIS_EXECUTION_PASS_TRADE_DATE:{symbol}")
            require(isinstance(item.get("last_price"), (int, float)) and item.get("last_price") > 0, f"OIS_EXECUTION_PASS_PRICE:{symbol}")
            require(isinstance(item.get("freshness_seconds"), int) and item.get("freshness_seconds") >= 0, f"OIS_EXECUTION_PASS_FRESHNESS:{symbol}")
            require(item.get("freshness_seconds") <= item.get("freshness_threshold_seconds"), f"OIS_EXECUTION_PASS_FRESHNESS_THRESHOLD:{symbol}")
            require(isinstance(item.get("approved_source_metadata"), dict), f"OIS_EXECUTION_PASS_SOURCE:{symbol}")
        else:
            require(item.get("validation_status") == "BLOCKED", f"OIS_EXECUTION_BLOCKED_STATUS:{symbol}")
            require(item.get("fail_closed_reason"), f"OIS_EXECUTION_BLOCKED_REASON:{symbol}")
    require(doc.get("pass_count") == pass_count, "OIS_EXECUTION_PASS_COUNT")
    expected_status = "PASS" if pass_count == len(REQUIRED_EXECUTION_SYMBOLS) else "PARTIAL" if pass_count > 0 else "BLOCKED"
    require(doc.get("validation_status") == expected_status, "OIS_EXECUTION_LAYER_STATUS")
    require(doc.get("publishable") is (pass_count > 0), "OIS_EXECUTION_PUBLISHABLE_STATUS")


def write_execution_data_layer(root: Path) -> Path:
    doc = build_execution_data_layer(root)
    validate_execution_data_layer(doc)
    path = root.resolve() / EXECUTION_LAYER_PATH
    write_json(path, doc)
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--decision-cadence", default="OIS_0935_OPENING")
    parser.add_argument("--decision-trade-date")
    args = parser.parse_args(argv)
    try:
        if args.write:
            doc = build_execution_data_layer(args.root, decision_cadence=args.decision_cadence, decision_trade_date=args.decision_trade_date)
            validate_execution_data_layer(doc)
            path = args.root.resolve() / EXECUTION_LAYER_PATH
            write_json(path, doc)
            print(json.dumps(read_json(path), sort_keys=True))
        else:
            doc = build_execution_data_layer(args.root, decision_cadence=args.decision_cadence, decision_trade_date=args.decision_trade_date)
            validate_execution_data_layer(doc)
            print(json.dumps(doc, sort_keys=True))
        return 0
    except IntegrityError as exc:
        print(json.dumps({"validation_status": "FAIL", "error_code": str(exc)}, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
