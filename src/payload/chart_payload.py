from __future__ import annotations

from datetime import datetime, timezone


SCHEMA_VERSION = "OIS-CHART-1.0"


def build_chart_payload(
    wti_rows: list[dict],
    brent_rows: list[dict],
    wti_indicators: dict,
    brent_indicators: dict,
    validation: dict,
    max_points: int = 250,
) -> dict:
    wti_data = wti_indicators["data"][-max_points:]
    brent_data = brent_indicators["data"][-max_points:]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _now_utc(),
        "validation_status": validation["overall_validation"],
        "latest_complete_source_date": {
            "wti": wti_rows[-1]["date"],
            "brent": brent_rows[-1]["date"],
        },
        "wti": commodity_metadata("WTI", wti_rows, wti_indicators, validation["WTI"]),
        "brent": commodity_metadata("Brent", brent_rows, brent_indicators, validation["Brent"]),
        "datasets": {
            "wti_price_structure": [
                {"date": row["date"], "price": row["close"], "ma20": row["ma20"], "ma60": row["ma60"], "ma120": row["ma120"]}
                for row in wti_data
            ],
            "wti_macd": [
                {"date": row["date"], "dif": row["dif"], "dea": row["dea"], "histogram": row["histogram"], "zero": 0}
                for row in wti_data
            ],
            "wti_rsi": [
                {"date": row["date"], "rsi14": row["rsi14"], "upper70": 70, "middle50": 50, "lower30": 30}
                for row in wti_data
            ],
            "brent_price_structure": [
                {"date": row["date"], "price": row["close"], "ma20": row["ma20"], "ma60": row["ma60"], "ma120": row["ma120"]}
                for row in brent_data
            ],
            "brent_macd": [
                {"date": row["date"], "dif": row["dif"], "dea": row["dea"], "histogram": row["histogram"], "zero": 0}
                for row in brent_data
            ],
            "brent_rsi": [
                {"date": row["date"], "rsi14": row["rsi14"], "upper70": 70, "middle50": 50, "lower30": 30}
                for row in brent_data
            ],
        },
    }


def commodity_metadata(name: str, rows: list[dict], indicators: dict, validation: dict) -> dict:
    closes = [float(row["close"]) for row in rows]
    last_20 = closes[-20:] if len(closes) >= 20 else closes
    latest = indicators["latest"]
    return {
        "source": rows[-1]["source"],
        "source_date": rows[-1]["date"],
        "data_frequency": "daily",
        "rows": len(rows),
        "validation": validation["validation_status"],
        "support": round(min(last_20), 4),
        "resistance": round(max(last_20), 4),
        "support_resistance_method": "Trailing 20 trading-day close min/max.",
        "momentum": {
            "close_minus_20d_close": round(closes[-1] - closes[-21], 6) if len(closes) > 20 else None,
            "macd_histogram": latest.get("histogram"),
            "rsi14": latest.get("rsi14"),
        },
        "trigger_reference": {
            "close_above_ma20": latest.get("ma20"),
            "macd_histogram_above_zero": 0,
        },
        "invalidation_reference": {
            "trailing_20d_support": round(min(last_20), 4),
            "rsi_oversold_line": 30,
        },
    }


def validate_chart_payload_schema(payload: dict) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if payload.get("schema_version") != SCHEMA_VERSION:
        errors.append("invalid schema_version")
    required = {
        "wti_price_structure",
        "wti_macd",
        "wti_rsi",
        "brent_price_structure",
        "brent_macd",
        "brent_rsi",
    }
    datasets = payload.get("datasets", {})
    missing = sorted(required - set(datasets))
    if missing:
        errors.append(f"missing datasets: {missing}")
    for key in required & set(datasets):
        if not datasets[key]:
            errors.append(f"empty dataset: {key}")
    return not errors, errors


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

