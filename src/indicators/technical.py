from __future__ import annotations

from math import isnan


CALCULATION_CONVENTION = {
    "moving_averages": "Simple moving average over daily closes: MA20, MA60, MA120.",
    "ema": "EMA seed is first available close; smoothing alpha = 2 / (period + 1).",
    "macd": "DIF = EMA12 - EMA26; DEA = EMA9(DIF); Histogram = DIF - DEA.",
    "rsi": "Wilder RSI(14) using average gain/loss smoothing.",
}


def simple_moving_average(values: list[float], period: int) -> list[float | None]:
    output: list[float | None] = []
    for idx in range(len(values)):
        if idx < period - 1:
            output.append(None)
            continue
        window = values[idx - period + 1 : idx + 1]
        output.append(sum(window) / period)
    return output


def exponential_moving_average(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1.0)
    ema = values[0]
    output = []
    for value in values:
        ema = value if not output else (value * alpha) + (ema * (1.0 - alpha))
        output.append(ema)
    return output


def macd(values: list[float]) -> dict[str, list[float]]:
    ema12 = exponential_moving_average(values, 12)
    ema26 = exponential_moving_average(values, 26)
    dif = [a - b for a, b in zip(ema12, ema26)]
    dea = exponential_moving_average(dif, 9)
    histogram = [d - e for d, e in zip(dif, dea)]
    return {"dif": dif, "dea": dea, "histogram": histogram}


def rsi(values: list[float], period: int = 14) -> list[float | None]:
    output: list[float | None] = [None] * len(values)
    if len(values) <= period:
        return output
    gains = 0.0
    losses = 0.0
    for idx in range(1, period + 1):
        change = values[idx] - values[idx - 1]
        if change >= 0:
            gains += change
        else:
            losses += abs(change)
    avg_gain = gains / period
    avg_loss = losses / period
    output[period] = _rsi_value(avg_gain, avg_loss)
    for idx in range(period + 1, len(values)):
        change = values[idx] - values[idx - 1]
        gain = max(change, 0.0)
        loss = abs(min(change, 0.0))
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period
        output[idx] = _rsi_value(avg_gain, avg_loss)
    return output


def build_indicators(rows: list[dict]) -> dict:
    closes = [float(row["close"]) for row in rows]
    ma20 = simple_moving_average(closes, 20)
    ma60 = simple_moving_average(closes, 60)
    ma120 = simple_moving_average(closes, 120)
    macd_data = macd(closes)
    rsi14 = rsi(closes, 14)
    records = []
    for idx, row in enumerate(rows):
        records.append(
            {
                "date": row["date"],
                "close": _round(closes[idx]),
                "ma20": _round_optional(ma20[idx]),
                "ma60": _round_optional(ma60[idx]),
                "ma120": _round_optional(ma120[idx]),
                "dif": _round(macd_data["dif"][idx]),
                "dea": _round(macd_data["dea"][idx]),
                "histogram": _round(macd_data["histogram"][idx]),
                "rsi14": _round_optional(rsi14[idx]),
            }
        )
    latest = records[-1] if records else {}
    return {
        "schema_version": "OIS-INDICATORS-1.0",
        "calculation_convention": CALCULATION_CONVENTION,
        "rows": len(records),
        "latest": latest,
        "data": records,
    }


def _rsi_value(avg_gain: float, avg_loss: float) -> float:
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _round(value: float) -> float:
    if isnan(value):
        return value
    return round(value, 6)


def _round_optional(value: float | None) -> float | None:
    if value is None:
        return None
    return _round(value)

