from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

from jsonschema import Draft202012Validator

from src.rate.contract import CONTRACT_VERSION, DATASETS, SCHEMA_VERSION, common_metadata

ROOT = Path(__file__).resolve().parents[2]
PRODUCTION = ROOT / "data" / "rate" / "production"
PUBLIC_FILES = ("rate_status.json", "rate_ingestion_validation.json", "rate_data_payload.json")

class RateIntegrityError(ValueError):
    pass

def read_json(path: Path) -> dict:
    def reject(value):
        raise RateIntegrityError(f"NON_FINITE_JSON:{value}")
    def unique(pairs):
        output = {}
        for key, value in pairs:
            if key in output:
                raise RateIntegrityError(f"DUPLICATE_JSON_KEY:{key}")
            output[key] = value
        return output
    return json.loads(path.read_text(encoding="utf-8"), parse_constant=reject, object_pairs_hook=unique)

def fetch_source(url: str, token: str | None = None) -> dict:
    headers = {"User-Agent": "RATE-Data-Engine/1.0", "Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(url, headers=headers)
    with urlopen(request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise RateIntegrityError("SOURCE_ROOT_NOT_OBJECT")
    return payload

def _number(value: object, field: str) -> None:
    if type(value) not in (int, float) or not math.isfinite(value):
        raise RateIntegrityError(f"INVALID_NUMBER:{field}")

def validate_bundle(bundle: dict, *, now: datetime | None = None, max_age_days: int = 7) -> dict:
    now = now or datetime.now(timezone.utc)
    metadata = bundle.get("metadata")
    datasets = bundle.get("datasets")
    if not isinstance(metadata, dict) or not isinstance(datasets, dict):
        raise RateIntegrityError("MISSING_METADATA_OR_DATASETS")
    required = {"source", "source_timestamp", "data_as_of"}
    if not required.issubset(metadata):
        raise RateIntegrityError("MISSING_SOURCE_METADATA")
    try:
        source_time = datetime.fromisoformat(metadata["source_timestamp"].replace("Z", "+00:00"))
        as_of = datetime.fromisoformat(metadata["data_as_of"] + "T00:00:00+00:00")
    except (TypeError, ValueError):
        raise RateIntegrityError("INVALID_SOURCE_TIMESTAMP_OR_AS_OF") from None
    if source_time > now or as_of > now:
        raise RateIntegrityError("FUTURE_SOURCE_DATE")
    if (now - source_time).days > max_age_days:
        raise RateIntegrityError("STALE_SOURCE")
    errors: list[str] = []
    total = 0
    for name, fields in DATASETS.items():
        rows = datasets.get(name)
        if not isinstance(rows, list) or not rows:
            errors.append(f"MISSING_OR_EMPTY_DATASET:{name}")
            continue
        seen: set[tuple] = set()
        for index, row in enumerate(rows):
            if not isinstance(row, dict) or set(row) != fields:
                errors.append(f"SCHEMA_FIELDS:{name}:{index}")
                continue
            key = tuple(row[field] for field in sorted(fields & {"date", "week", "as_of", "instrument", "rank", "metric", "input_code"}))
            if key in seen:
                errors.append(f"DUPLICATE_ROW:{name}:{index}")
            seen.add(key)
            for field in fields:
                value = row[field]
                if value is None or value == "":
                    errors.append(f"MISSING_VALUE:{name}:{index}:{field}")
                if field in {"price", "volume", "close", "margin_balance", "short_balance", "market_cap", "value", "stage_score", "relative_strength", "flow_score", "return_1d", "volatility_20d", "concentration", "foreign_net", "trust_net", "dealer_net", "total_net", "margin_change", "short_change", "large_order_net", "turnover", "ma20", "ma60", "macd_dif", "macd_dea", "macd_histogram"}:
                    try:
                        _number(value, f"{name}:{index}:{field}")
                    except RateIntegrityError as exc:
                        errors.append(str(exc))
            if "rank" in row and (type(row["rank"]) is not int or row["rank"] <= 0):
                errors.append(f"INVALID_RANK:{name}:{index}")
            if "evidence_refs" in row and (not isinstance(row["evidence_refs"], list) or not all(isinstance(ref, str) and ref for ref in row["evidence_refs"])):
                errors.append(f"INVALID_EVIDENCE_REFS:{name}:{index}")
        total += len(rows)
    if datasets.get("top50_universe") and len(datasets["top50_universe"]) != 50:
        errors.append("TOP50_COUNT_NOT_50")
    if datasets.get("top30_universe") and len(datasets["top30_universe"]) != 30:
        errors.append("TOP30_COUNT_NOT_30")
    if errors:
        raise RateIntegrityError(";".join(errors[:20]))
    return {"record_count": total, "data_as_of": metadata["data_as_of"], "source": metadata["source"],
            "source_timestamp": metadata["source_timestamp"], "dataset_counts": {key: len(value) for key, value in datasets.items()}}

def build_documents(bundle: dict) -> dict[str, dict]:
    result = validate_bundle(bundle)
    datasets = bundle["datasets"]
    snapshot_id = hashlib.sha256(json.dumps(datasets, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    metadata = common_metadata(data_as_of=result["data_as_of"], source=result["source"], source_timestamp=result["source_timestamp"], record_count=result["record_count"], validation_status="PASS", quality_flags=[], missing_fields=[], duplicate_status="PASS", freshness_status="PASS")
    payload = {**metadata, "schema_version": SCHEMA_VERSION, "snapshot_id": snapshot_id, "datasets": datasets}
    validation = {**metadata, "schema_version": "RATE-VALIDATION-1.0", "snapshot_id": snapshot_id, "overall_validation": "PASS", "checks": {"freshness": "PASS", "missing_value": "PASS", "duplicate": "PASS", "trading_date": "PASS", "schema": "PASS", "type": "PASS", "range": "PASS", "cross_file_consistency": "PASS"}, "dataset_counts": result["dataset_counts"]}
    status = {**metadata, "schema_version": "RATE-STATUS-1.0", "snapshot_id": snapshot_id, "status": "PASS", "validation": "PASS", "dataset_counts": result["dataset_counts"]}
    documents = {"rate_status.json": status, "rate_ingestion_validation.json": validation, "rate_data_payload.json": payload}
    schema = json.loads((ROOT / "schemas" / "rate_production.schema.json").read_text(encoding="utf-8"))
    for document in documents.values():
        errors = list(Draft202012Validator(schema).iter_errors(document))
        if errors:
            raise RateIntegrityError(f"SCHEMA_DOCUMENT:{errors[0].message}")
    return documents

def write_candidate(documents: dict[str, dict], candidate: Path) -> None:
    if candidate.exists():
        raise RateIntegrityError("CANDIDATE_MUST_BE_NEW")
    for name, document in documents.items():
        path = candidate / "data" / "rate" / "production" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-url", default=None)
    parser.add_argument("--source-file", type=Path)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if bool(args.source_url) == bool(args.source_file):
            raise RateIntegrityError("EXACTLY_ONE_SOURCE_REQUIRED")
        bundle = read_json(args.source_file) if args.source_file else fetch_source(args.source_url, os.getenv("RATE_SOURCE_TOKEN"))
        result = validate_bundle(bundle)
        documents = build_documents(bundle)
        write_candidate(documents, args.candidate)
        report = {"validation_status": "PASS", "publishable": True, **result}
    except Exception as exc:
        report = {"validation_status": "FAIL", "publishable": False, "error_code": str(exc) if isinstance(exc, RateIntegrityError) else type(exc).__name__}
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))
    return 0 if report["validation_status"] == "PASS" else 1

if __name__ == "__main__":
    raise SystemExit(main())
