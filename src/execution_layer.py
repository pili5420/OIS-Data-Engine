
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

from src.runtime.engine import write_json
from src.runtime.source import IntegrityError
from src.runtime.validation import read_json, require
from src.work_state import PERSISTENT_STATE_SSOT_PATH, SYSTEM, file_sha256, validate_production_persistent_state_ssot

EXECUTION_LAYER_PATH = Path("data/execution/ois/OIS_EXECUTION_DATA_LAYER.json")
EXECUTION_LAYER_SCHEMA_VERSION = "OIS-EXECUTION-DATA-LAYER-1.0"
REQUIRED_EXECUTION_SYMBOLS = ("00642U", "00715L", "00673R")


def execution_source_binding(ssot: Mapping[str, Any]) -> dict:
    state = ssot["current_state"]
    return {
        "persistent_state_ssot_path": PERSISTENT_STATE_SSOT_PATH.as_posix(),
        "persistent_state_ssot_sha256": ssot.get("manifest_sha256"),
        "current_state_id": state["state_id"],
        "current_state_hash": state["state_hash"],
        "production_snapshot_id": state["production_snapshot_id"],
        "source_run_id": state["source_run_id"],
        "source_commit_sha": state["source_commit_sha"],
        "source_as_of": state["source_as_of"],
    }


def validate_execution_market_record(record: Mapping[str, Any], *, symbol: str) -> None:
    require(record.get("symbol") == symbol, f"OIS_EXECUTION_SYMBOL_MISMATCH:{symbol}")
    require(record.get("source") == "APPROVED_EXECUTION_MARKET_DATA_SOURCE", f"OIS_EXECUTION_UNAPPROVED_SOURCE:{symbol}")
    require(isinstance(record.get("retrieval_timestamp"), str) and record.get("retrieval_timestamp"), f"OIS_EXECUTION_RETRIEVAL_TIMESTAMP:{symbol}")
    require(isinstance(record.get("trading_date"), str) and len(record.get("trading_date", "")) >= 10, f"OIS_EXECUTION_TRADING_DATE:{symbol}")
    for field in ("open", "high", "low", "close", "volume"):
        require(isinstance(record.get(field), (int, float)) and record.get(field) >= 0, f"OIS_EXECUTION_FIELD:{symbol}:{field}")


def build_execution_data_layer(root: Path, official_execution_market_data: Mapping[str, Mapping[str, Any]] | None = None) -> dict:
    root = root.resolve()
    ssot = validate_production_persistent_state_ssot(root)
    ssot_path = root / PERSISTENT_STATE_SSOT_PATH
    manifest_hash = file_sha256(ssot_path) if ssot_path.exists() else None
    ssot = dict(ssot)
    ssot["manifest_sha256"] = manifest_hash
    binding = execution_source_binding(ssot)
    official_execution_market_data = official_execution_market_data or {}
    instruments = []
    pass_count = 0
    for symbol in REQUIRED_EXECUTION_SYMBOLS:
        market_record = official_execution_market_data.get(symbol)
        status = "BLOCKED"
        gate = "OFFICIAL_EXECUTION_MARKET_DATA_MISSING"
        if market_record is not None:
            validate_execution_market_record(market_record, symbol=symbol)
            require(market_record.get("trading_date") == binding["source_as_of"][:10], f"OIS_EXECUTION_TRADING_DATE_BINDING:{symbol}")
            status = "PASS"
            gate = "PASS"
            pass_count += 1
        instruments.append({
            "symbol": symbol,
            "instrument_scope": "OIS_EXECUTION_TARGET",
            "validation_status": status,
            "execution_gate": gate,
            "source_binding": binding,
            "market_data": market_record,
            "fallback_used": False,
            "fixture_used": False,
            "desktop_data_used": False,
        })
    publishable = pass_count == len(REQUIRED_EXECUTION_SYMBOLS)
    return {
        "system": SYSTEM,
        "execution_layer_schema_version": EXECUTION_LAYER_SCHEMA_VERSION,
        "required_symbols": list(REQUIRED_EXECUTION_SYMBOLS),
        "validation_status": "PASS" if publishable else "BLOCKED",
        "publishable": publishable,
        "execution_market_data_source": "APPROVED_EXECUTION_MARKET_DATA_SOURCE" if publishable else None,
        "fail_closed_reason": None if publishable else "OFFICIAL_EXECUTION_MARKET_DATA_MISSING_FOR_REQUIRED_SYMBOLS",
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
    instruments = doc.get("instruments")
    require(isinstance(instruments, list) and len(instruments) == len(REQUIRED_EXECUTION_SYMBOLS), "OIS_EXECUTION_INSTRUMENT_COUNT")
    seen = [item.get("symbol") for item in instruments if isinstance(item, dict)]
    require(tuple(seen) == REQUIRED_EXECUTION_SYMBOLS, "OIS_EXECUTION_SYMBOL_ORDER")
    for item in instruments:
        require(item.get("fallback_used") is False, f"OIS_EXECUTION_FALLBACK:{item.get('symbol')}")
        require(item.get("fixture_used") is False, f"OIS_EXECUTION_FIXTURE:{item.get('symbol')}")
        require(item.get("desktop_data_used") is False, f"OIS_EXECUTION_DESKTOP:{item.get('symbol')}")
        binding = item.get("source_binding")
        require(isinstance(binding, dict) and binding.get("current_state_id") and binding.get("current_state_hash"), f"OIS_EXECUTION_BINDING:{item.get('symbol')}")
    if doc.get("publishable") is True:
        require(doc.get("validation_status") == "PASS", "OIS_EXECUTION_PUBLISHABLE_STATUS")
        for item in instruments:
            require(item.get("validation_status") == "PASS", f"OIS_EXECUTION_MARKET_DATA_NOT_PASS:{item.get('symbol')}")
    else:
        require(doc.get("validation_status") == "BLOCKED", "OIS_EXECUTION_BLOCKED_STATUS")
        require(doc.get("fail_closed_reason"), "OIS_EXECUTION_BLOCKED_REASON")


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
    args = parser.parse_args(argv)
    try:
        if args.write:
            path = write_execution_data_layer(args.root)
            print(json.dumps(read_json(path), sort_keys=True))
        else:
            doc = build_execution_data_layer(args.root)
            validate_execution_data_layer(doc)
            print(json.dumps(doc, sort_keys=True))
        return 0
    except IntegrityError as exc:
        print(json.dumps({"validation_status": "FAIL", "error_code": str(exc)}, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
