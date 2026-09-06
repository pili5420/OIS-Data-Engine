from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from math import isfinite

from src.sources.adapters import PriceRow


@dataclass
class ValidationReport:
    source: str
    source_date: str | None
    rows: int
    unique_dates: int
    duplicate_dates: int
    missing_close: int
    validation_status: str
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    abnormal_price_jumps: list[dict] = field(default_factory=list)
    unexpected_gaps: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "source_date": self.source_date,
            "rows": self.rows,
            "unique_dates": self.unique_dates,
            "duplicate_dates": self.duplicate_dates,
            "missing_close": self.missing_close,
            "validation_status": self.validation_status,
            "errors": self.errors,
            "warnings": self.warnings,
            "abnormal_price_jumps": self.abnormal_price_jumps,
            "unexpected_gaps": self.unexpected_gaps,
        }


def validate_rows(rows: list[PriceRow], min_rows: int = 250, expected_source: str | None = None) -> ValidationReport:
    errors: list[str] = []
    warnings: list[str] = []
    abnormal_jumps: list[dict] = []
    unexpected_gaps: list[dict] = []
    dates = [row.date for row in rows]
    unique_dates = len(set(dates))
    duplicate_dates = len(dates) - unique_dates
    missing_close = sum(1 for row in rows if row.close is None)

    if len(rows) < min_rows:
        errors.append(f"rows {len(rows)} below minimum {min_rows}")
    if duplicate_dates:
        errors.append(f"duplicate trading dates detected: {duplicate_dates}")
    if missing_close:
        errors.append(f"missing close values detected: {missing_close}")
    if dates != sorted(dates):
        errors.append("dates are out of ascending chronological order")

    for row in rows:
        if not _valid_iso_date(row.date):
            errors.append(f"invalid ISO date: {row.date}")
        if not isfinite(float(row.close)) or float(row.close) <= 0:
            errors.append(f"invalid close on {row.date}: {row.close}")
        if expected_source and row.source != expected_source:
            errors.append(f"source mismatch on {row.date}: {row.source} != {expected_source}")

    for prev, cur in zip(rows, rows[1:]):
        if prev.close <= 0 or cur.close <= 0:
            continue
        pct_jump = abs(cur.close / prev.close - 1.0)
        if pct_jump >= 0.15:
            abnormal_jumps.append(
                {
                    "date": cur.date,
                    "previous_date": prev.date,
                    "previous_close": round(prev.close, 4),
                    "close": round(cur.close, 4),
                    "jump_pct": round(pct_jump * 100, 4),
                }
            )
        gap_days = (date.fromisoformat(cur.date) - date.fromisoformat(prev.date)).days
        if gap_days > 7:
            unexpected_gaps.append({"from": prev.date, "to": cur.date, "calendar_days": gap_days})

    if abnormal_jumps:
        warnings.append(f"{len(abnormal_jumps)} abnormal price jumps >= 15% require rollover/news review")
    if unexpected_gaps:
        warnings.append(f"{len(unexpected_gaps)} calendar gaps > 7 days detected")

    status = "FAIL" if errors else "PASS"
    source = rows[-1].source if rows else "none"
    source_date = rows[-1].date if rows else None
    return ValidationReport(
        source=source,
        source_date=source_date,
        rows=len(rows),
        unique_dates=unique_dates,
        duplicate_dates=duplicate_dates,
        missing_close=missing_close,
        validation_status=status,
        errors=errors,
        warnings=warnings,
        abnormal_price_jumps=abnormal_jumps,
        unexpected_gaps=unexpected_gaps,
    )


def _valid_iso_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
        return True
    except ValueError:
        return False
