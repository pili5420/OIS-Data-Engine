from __future__ import annotations

import argparse
import json
from pathlib import Path


PUBLIC_FILES = {
    "ois_chart_payload.json": ("OIS-CHART-1.0", "validation_status"),
    "ois_ingestion_validation.json": ("OIS-VALIDATION-1.0", "overall_validation"),
    "ois_status.json": ("OIS-STATUS-1.0", "validation"),
}


def load_pass_production(source: Path) -> dict[str, bytes]:
    contents = {name: (source / name).read_bytes() for name in PUBLIC_FILES}
    documents = {name: json.loads(content) for name, content in contents.items()}
    for name, (schema, status_key) in PUBLIC_FILES.items():
        document = documents[name]
        if document.get("schema_version") != schema or document.get(status_key) != "PASS":
            raise ValueError(f"Pages publication requires {schema} and PASS: {name}")

    payload = documents["ois_chart_payload.json"]
    validation = documents["ois_ingestion_validation.json"]
    status = documents["ois_status.json"]
    if validation.get("chart_payload_schema") != "PASS" or status.get("status") != "PASS":
        raise ValueError("Pages publication requires a validated PASS dataset")
    for name, key in (("WTI", "wti"), ("Brent", "brent")):
        if payload[key]["validation"] != "PASS" or validation[name]["validation_status"] != "PASS":
            raise ValueError(f"Pages publication requires PASS for {name}")
        dates = (
            payload[key]["source_date"],
            payload["latest_complete_source_date"][key],
            validation[name]["source_date"],
            status[f"{name}_source_date"],
        )
        if not dates[0] or len(set(dates)) != 1:
            raise ValueError(f"Inconsistent source dates for {name}")
        if payload[key]["rows"] != validation[name]["rows"]:
            raise ValueError(f"Inconsistent database row counts for {name}")

    return contents


def prepare_pages(source: Path, destination: Path) -> None:
    contents = load_pass_production(source)

    # Validate everything before creating an artifact; preserve the original JSON bytes.
    destination.mkdir(parents=True, exist_ok=False)
    for name, content in contents.items():
        (destination / name).write_bytes(content)


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish exact PASS production JSON to a Pages artifact.")
    parser.add_argument("--source", type=Path, default=Path("data/production"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    prepare_pages(args.source, args.output)
    print("Pages artifact PASS: three production JSON files copied without modification.")


if __name__ == "__main__":
    main()
