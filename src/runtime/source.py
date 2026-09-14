from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import pandas_market_calendars as calendars


class IntegrityError(ValueError):
    """Non-retryable data integrity failure."""


class TransientError(RuntimeError):
    """Retry budget exhausted; production must remain unchanged."""


def stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def schedule(start: str, end: str) -> dict[str, datetime]:
    # Both current symbols are NYMEX contracts, including BZ=F (not ICE B).
    frame = calendars.get_calendar("CMEGlobex_EnergyAndMetals").schedule(start, end)
    return {str(day.date()): row.market_close.to_pydatetime() for day, row in frame.iterrows()}


def latest_completed(now: datetime) -> str:
    sessions = schedule((now - timedelta(days=14)).date().isoformat(), now.date().isoformat())
    # Allow vendor daily-bar finalization after the exchange session ends.
    return max(day for day, close in sessions.items() if close + timedelta(hours=6) <= now)


def get_json(url: str, *, opener=urlopen, sleep=time.sleep) -> dict:
    for attempt in range(3):
        try:
            req = Request(url, headers={"User-Agent": "OIS-Data-Engine/2.0", "Accept": "application/json"})
            with opener(req, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code not in (408, 429, 500, 502, 503, 504):
                raise IntegrityError(f"SOURCE_HTTP_{exc.code}") from None
            delay = min(60, int(exc.headers.get("Retry-After", "0"))) if exc.headers and exc.headers.get("Retry-After", "").isdigit() else 0
        except (URLError, TimeoutError, ConnectionError, OSError):
            delay = 0
        except (ValueError, UnicodeError):
            raise IntegrityError("SOURCE_INVALID_JSON") from None
        if attempt < 2:
            sleep(max(delay, 2 ** (attempt + 1)))
    raise TransientError("SOURCE_RETRY_EXHAUSTED")


def normalize(payload: dict, ticker: str, now: datetime) -> tuple[list[dict], list[str]]:
    try:
        chart = payload["chart"]
        if chart.get("error"):
            raise IntegrityError("SOURCE_REPORTED_ERROR")
        result = chart["result"][0]
        meta = result["meta"]
        if meta["symbol"] != ticker or meta["instrumentType"] != "FUTURE" or meta["exchangeName"] != "NYM":
            raise IntegrityError("SOURCE_CONTRACT_MISMATCH")
        zone = ZoneInfo(meta["exchangeTimezoneName"])
        timestamps = result["timestamp"]
        values = result["indicators"]["quote"][0]
        if not timestamps or any(len(values[key]) != len(timestamps) for key in ("open", "high", "low", "close", "volume")):
            raise IntegrityError("SOURCE_ARRAY_LENGTH")
        dates = [datetime.fromtimestamp(ts, zone).date().isoformat() for ts in timestamps]
        if len(set(dates)) != len(dates):
            raise IntegrityError("SOURCE_DUPLICATE_DATE")
        if dates != sorted(dates):
            raise IntegrityError("SOURCE_DATE_ORDER")
        sessions = schedule(dates[0], max(dates[-1], now.date().isoformat()))
        rows, flags = [], []
        for i, day in enumerate(dates):
            fields = {key: values[key][i] for key in ("open", "high", "low", "close", "volume")}
            if day > now.astimezone(zone).date().isoformat():
                raise IntegrityError("SOURCE_FUTURE_DATE")
            if day not in sessions:
                if all(value is None for value in fields.values()):
                    flags.append("NON_SESSION_EMPTY_BAR_EXCLUDED")
                    continue
                raise IntegrityError(f"SOURCE_NON_TRADING_DATE:{day}")
            if sessions[day] + timedelta(hours=6) > now:
                flags.append("INCOMPLETE_SESSION_EXCLUDED")
                continue
            # Some exchange holiday sessions trade without a vendor daily bar.
            # Only a wholly empty, calendar-confirmed shortened session may be
            # excluded. Partial missing bars and normal-session gaps still fail.
            if all(value is None for value in fields.values()) and sessions[day].astimezone(ZoneInfo("America/New_York")).hour < 17:
                flags.append("EMPTY_SHORTENED_SESSION_EXCLUDED")
                continue
            if any(value is None for value in fields.values()):
                raise IntegrityError(f"SOURCE_MISSING_VALUE:{day}")
            rows.append({"date": day, **fields, "source": "yahoo_chart",
                         "source_timestamp": stamp(datetime.fromtimestamp(timestamps[i], timezone.utc))})
        if not rows:
            raise IntegrityError("SOURCE_EMPTY_HISTORY")
        return rows, sorted(set(flags))
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        if isinstance(exc, IntegrityError):
            raise
        raise IntegrityError("SOURCE_STRUCTURE_INVALID") from None


def fetch(ticker: str, now: datetime) -> tuple[list[dict], list[str]]:
    for host in ("query1.finance.yahoo.com", "query2.finance.yahoo.com"):
        try:
            payload = get_json(f"https://{host}/v8/finance/chart/{quote(ticker, safe='')}?range=2y&interval=1d&includePrePost=false")
            return normalize(payload, ticker, now)
        except TransientError:
            if host.startswith("query2"):
                raise
    raise TransientError("SOURCE_UNAVAILABLE")
