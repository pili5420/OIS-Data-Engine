from __future__ import annotations

from datetime import datetime, timezone

SCHEMA_VERSION = "RATE-PRODUCTION-1.0"
CONTRACT_VERSION = "1.0"

DATASETS = {
    "market_structure": {"date", "instrument", "price", "volume", "ma20", "ma60", "macd_dif", "macd_dea", "macd_histogram"},
    "weekly_structure": {"week", "instrument", "close", "ma20", "ma60", "trend"},
    "margin_financing": {"date", "margin_balance", "margin_change", "short_balance", "short_change"},
    "institutional_flow": {"date", "foreign_net", "trust_net", "dealer_net", "total_net"},
    "smart_money": {"date", "instrument", "large_order_net", "turnover", "concentration"},
    "fundamental_evidence": {"as_of", "instrument", "metric", "value", "unit", "source"},
    "top50_universe": {"as_of", "rank", "instrument", "name", "market_cap", "sector"},
    "top30_universe": {"as_of", "rank", "instrument", "name", "market_cap", "sector"},
    "stage_inputs": {"as_of", "instrument", "stage_code", "stage_score", "evidence_refs"},
    "rotation_inputs": {"as_of", "instrument", "group", "relative_strength", "flow_score", "evidence_refs"},
    "m7_inputs": {"as_of", "instrument", "input_code", "value", "evidence_refs"},
    "mhe_inputs": {"as_of", "instrument", "input_code", "value", "evidence_refs"},
    "portfolio_market_data": {"date", "instrument", "close", "volume", "return_1d", "volatility_20d"},
}

def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def common_metadata(*, data_as_of: str, source: str, source_timestamp: str, record_count: int,
                    validation_status: str, quality_flags: list[str], missing_fields: list[str],
                    duplicate_status: str, freshness_status: str) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "generated_at": utc_now(),
        "data_as_of": data_as_of,
        "source": source,
        "source_timestamp": source_timestamp,
        "record_count": record_count,
        "validation_status": validation_status,
        "quality_flags": sorted(set(quality_flags)),
        "missing_fields": sorted(set(missing_fields)),
        "duplicate_status": duplicate_status,
        "freshness_status": freshness_status,
    }
