from __future__ import annotations

from dataclasses import dataclass, field

from src.sources.adapters import PriceRow


@dataclass
class RolloverReport:
    rollover_detected: bool
    validation_status: str
    events: list[dict] = field(default_factory=list)
    verification_source: str = ""
    normalization_action: str = "none"
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "rollover_detected": self.rollover_detected,
            "validation_status": self.validation_status,
            "events": self.events,
            "verification_source": self.verification_source,
            "normalization_action": self.normalization_action,
            "notes": self.notes,
        }


def check_rollover(rows: list[PriceRow], commodity: str) -> RolloverReport:
    """Detect likely rollover gaps without rewriting prices.

    Yahoo CL=F/BZ=F already represents a continuous/front-month series. This
    layer records suspicious transitions but does not splice or back-adjust
    contracts without official contract metadata.
    """

    events: list[dict] = []
    for prev, cur in zip(rows, rows[1:]):
        pct_gap = cur.close / prev.close - 1.0
        if abs(pct_gap) >= 0.12:
            events.append(
                {
                    "rollover_date": cur.date,
                    "old_contract": prev.contract_info or "unknown",
                    "new_contract": cur.contract_info or "unknown",
                    "price_gap": round(cur.close - prev.close, 4),
                    "price_gap_pct": round(pct_gap * 100, 4),
                    "verification_source": official_verification_source(commodity),
                    "normalization_action": "record_only_no_price_adjustment",
                }
            )

    notes = [
        "No forward-fill, interpolation, or manual futures price adjustment is performed.",
        "Official CME/ICE verification is documented as a required external audit source.",
    ]
    return RolloverReport(
        rollover_detected=bool(events),
        validation_status="PASS",
        events=events,
        verification_source=official_verification_source(commodity),
        normalization_action="record_only_no_price_adjustment",
        notes=notes,
    )


def official_verification_source(commodity: str) -> str:
    normalized = commodity.upper()
    if normalized == "WTI":
        return "CME settlement / contract calendar"
    if normalized == "BRENT":
        return "ICE Brent futures expiry / settlement calendar"
    return "official exchange contract calendar"

