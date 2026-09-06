from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable


@dataclass(frozen=True)
class PriceRow:
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    source: str
    contract_info: str = ""

    def to_csv_dict(self) -> dict[str, str | float | int]:
        return {
            "date": self.date,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "source": self.source,
        }


@dataclass(frozen=True)
class FetchResult:
    ticker: str
    source: str
    fetched_at: str
    rows: list[PriceRow]
    error: str | None = None


class SourceAdapter:
    name = "base"

    def fetch_history(self, ticker: str, range_: str = "2y") -> FetchResult:
        raise NotImplementedError


class YahooChartAdapter(SourceAdapter):
    """Yahoo Finance chart API adapter for continuous front-month futures."""

    name = "yahoo_chart"

    def __init__(self, host: str = "query1.finance.yahoo.com", timeout: int = 30):
        self.host = host
        self.timeout = timeout

    def fetch_history(self, ticker: str, range_: str = "2y") -> FetchResult:
        encoded = urllib.parse.quote(ticker, safe="")
        url = (
            f"https://{self.host}/v8/finance/chart/{encoded}"
            f"?range={urllib.parse.quote(range_)}&interval=1d&includePrePost=false"
        )
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
            chart = payload.get("chart", {})
            if chart.get("error"):
                raise RuntimeError(json.dumps(chart["error"], sort_keys=True))
            result = chart["result"][0]
            timestamps = result.get("timestamp") or []
            quote = result["indicators"]["quote"][0]
            rows: list[PriceRow] = []
            for idx, ts in enumerate(timestamps):
                close = _value_at(quote.get("close"), idx)
                if close is None:
                    continue
                row_date = datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat()
                rows.append(
                    PriceRow(
                        date=row_date,
                        open=float(_value_at(quote.get("open"), idx) or close),
                        high=float(_value_at(quote.get("high"), idx) or close),
                        low=float(_value_at(quote.get("low"), idx) or close),
                        close=float(close),
                        volume=int(_value_at(quote.get("volume"), idx) or 0),
                        source=self.name,
                        contract_info="continuous_front_month",
                    )
                )
            rows.sort(key=lambda item: item.date)
            return FetchResult(
                ticker=ticker,
                source=self.name,
                fetched_at=_now_utc(),
                rows=rows,
            )
        except Exception as exc:  # noqa: BLE001 - adapter boundary records all failures.
            return FetchResult(
                ticker=ticker,
                source=self.name,
                fetched_at=_now_utc(),
                rows=[],
                error=f"{type(exc).__name__}: {exc}",
            )


class YahooChartAlternativeAdapter(YahooChartAdapter):
    name = "yahoo_chart_query2"

    def __init__(self, timeout: int = 30):
        super().__init__(host="query2.finance.yahoo.com", timeout=timeout)


class StooqAdapter(SourceAdapter):
    """Stooq placeholder adapter.

    It is intentionally not enabled as an automatic futures replacement unless
    the returned series can be proven to match the continuous futures contract.
    """

    name = "stooq"

    def fetch_history(self, ticker: str, range_: str = "2y") -> FetchResult:
        return FetchResult(
            ticker=ticker,
            source=self.name,
            fetched_at=_now_utc(),
            rows=[],
            error="Stooq adapter not configured for contract-equivalent futures series.",
        )


class SourceRouter:
    def __init__(self, adapters: Iterable[SourceAdapter] | None = None):
        self.adapters = list(adapters or [YahooChartAdapter(), YahooChartAlternativeAdapter()])

    def fetch_history(self, ticker: str, min_rows: int = 250, range_: str = "2y") -> FetchResult:
        failures: list[str] = []
        for adapter in self.adapters:
            result = adapter.fetch_history(ticker, range_)
            if result.error:
                failures.append(f"{adapter.name}: {result.error}")
                time.sleep(0.5)
                continue
            if len(result.rows) >= min_rows:
                return result
            failures.append(f"{adapter.name}: only {len(result.rows)} rows")
        return FetchResult(
            ticker=ticker,
            source="none",
            fetched_at=_now_utc(),
            rows=[],
            error="; ".join(failures),
        )


def _value_at(values: list | None, idx: int):
    if values is None or idx >= len(values):
        return None
    return values[idx]


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

