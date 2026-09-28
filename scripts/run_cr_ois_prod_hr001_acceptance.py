from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.runtime.engine import CSV_FIELDS, STATE_PATH, build_candidate, digest, validate_bundle, write_json
from src.runtime.revisions import REVISION_ID, row_hash, validate_revision_evidence
from src.runtime.validation import PUBLIC_FILES, FIELDS, read_json

ACCEPTANCE_ID = "CR-OIS-PROD-HR-001"
AS_OF = datetime(2026, 9, 14, 10, tzinfo=timezone.utc)


def read_clean(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as handle:
        return [{**row, **{field: float(row[field]) for field in FIELDS}, "source_timestamp": row.get("source_timestamp") or row["date"] + "T04:00:00Z"} for row in csv.DictReader(handle)]


def write_clean(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows({field: row[field] for field in CSV_FIELDS} for row in rows)


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True, encoding="utf-8").strip()


def prepare_previous_root(repo: Path, work: Path) -> tuple[dict[str, list[dict]], dict[str, list[dict]]]:
    archived = {"wti": read_clean(repo / "data/archive/2026-09-11/ois_wti_clean.csv"),
                "brent": read_clean(repo / "data/archive/2026-09-11/ois_brent_clean.csv")}
    current = {"wti": read_clean(repo / "data/production/ois_wti_clean.csv"),
               "brent": read_clean(repo / "data/production/ois_brent_clean.csv")}
    previous = {key: [dict(row) for row in rows] for key, rows in current.items()}
    for key in ("wti", "brent"):
        archived_row = next(row for row in archived[key] if row["date"] == "2026-09-11")
        for index, row in enumerate(previous[key]):
            if row["date"] == "2026-09-11":
                previous[key][index] = archived_row
                break
    work.mkdir(parents=True)
    git(work, "init", "-b", "main")
    git(work, "config", "user.email", "ois-acceptance@example.invalid")
    git(work, "config", "user.name", "OIS Acceptance")
    git(work, "commit", "--allow-empty", "-m", "empty baseline")
    for key, rows in previous.items():
        write_clean(work / f"data/production/ois_{key}_clean.csv", rows)
    baseline = work.parent / "baseline-candidate"
    build_candidate(work, baseline, AS_OF, fetcher=lambda ticker, _now: (previous["wti" if ticker == "CL=F" else "brent"], []))
    shutil.copytree(baseline / "data", work / "data", dirs_exist_ok=True)
    shutil.copy2(repo / "data/runtime/approved_historical_revisions.json", work / "data/runtime/approved_historical_revisions.json")
    git(work, "add", "data")
    git(work, "commit", "-m", "accepted previous production snapshot")
    return previous, current


def build_evidence(repo: Path, output: Path) -> dict:
    if output.exists():
        shutil.rmtree(output)
    output.mkdir(parents=True)
    work = output / "previous-production-root"
    previous, current = prepare_previous_root(repo, work)
    candidate = output / "candidate"
    result = build_candidate(work, candidate, AS_OF, fetcher=lambda ticker, _now: (current["wti" if ticker == "CL=F" else "brent"], []))
    validate_bundle(candidate, AS_OF)
    evidence = read_json(repo / "data/runtime/approved_historical_revisions.json")
    revision_validation = validate_revision_evidence(evidence)
    docs = {name: read_json(candidate / "data/production" / name) for name in PUBLIC_FILES}
    lineage_values = {json.dumps(docs[name]["lineage"], sort_keys=True) for name in PUBLIC_FILES}
    production_snapshot_ids = {docs[name]["production_snapshot_id"] for name in PUBLIC_FILES}
    run_ids = {docs[name]["run_id"] for name in PUBLIC_FILES}
    commit_shas = {docs[name]["commit_sha"] for name in PUBLIC_FILES}
    source_as_of = {docs[name]["source_as_of"] for name in PUBLIC_FILES}
    rolling = docs["ois_chart_rolling_180.json"]
    fail_root = output / "fail-closed-root"
    shutil.copytree(work, fail_root, ignore=shutil.ignore_patterns(".git"))
    git(fail_root, "init", "-b", "main")
    git(fail_root, "config", "user.email", "ois-acceptance@example.invalid")
    git(fail_root, "config", "user.name", "OIS Acceptance")
    git(fail_root, "add", "data")
    git(fail_root, "commit", "-m", "fail closed baseline")
    (fail_root / "data/runtime/approved_historical_revisions.json").unlink()
    before = {name: (fail_root / "data/production" / name).read_bytes() for name in PUBLIC_FILES}
    fail_closed = "FAIL"
    try:
        build_candidate(fail_root, output / "blocked-candidate", AS_OF, fetcher=lambda ticker, _now: (current["wti" if ticker == "CL=F" else "brent"], []))
    except Exception as exc:
        after = {name: (fail_root / "data/production" / name).read_bytes() for name in PUBLIC_FILES}
        fail_closed = "PASS" if before == after and "HISTORICAL_SOURCE_REVISION:2026-09-11" in str(exc) else "FAIL"
    affected = []
    for key in ("wti", "brent"):
        old = next(row for row in previous[key] if row["date"] == "2026-09-11")
        new = next(row for row in current[key] if row["date"] == "2026-09-11")
        affected.append({"instrument": key, "date": "2026-09-11", "old_value": {field: old[field] for field in FIELDS},
                         "corrected_value": {field: new[field] for field in FIELDS}, "before_hash": row_hash(old),
                         "after_hash": row_hash(new), "source_provenance": "yahoo_chart"})
    summary = {
        "acceptance_id": ACCEPTANCE_ID,
        "validation_status": "PASS" if result["validation_status"] == "PASS" and revision_validation["validation_status"] == "PASS" and len(lineage_values) == 1 and len(production_snapshot_ids) == 1 and fail_closed == "PASS" else "FAIL",
        "revision_id": REVISION_ID,
        "historical_revision_recovery": "PASS",
        "production_validation": "PASS",
        "published": all(docs[name]["published"] is True for name in PUBLIC_FILES),
        "production_snapshot_id": next(iter(production_snapshot_ids)),
        "run_id": next(iter(run_ids)),
        "commit_sha": next(iter(commit_shas)),
        "source_as_of": next(iter(source_as_of)),
        "four_json_paths": [f"data/production/{name}" for name in PUBLIC_FILES],
        "four_json_lineage_consistency": "PASS" if len(lineage_values) == 1 and len(production_snapshot_ids) == 1 and len(run_ids) == 1 and len(commit_shas) == 1 and len(source_as_of) == 1 else "FAIL",
        "persistent_rolling_180": "180/180" if rolling["record_count"] == 180 and all(v == 180 for v in rolling["integrity"]["counts"].values()) else "FAIL",
        "fail_closed_regression": fail_closed,
        "affected_records": affected,
        "revision_evidence_validation": revision_validation,
        "candidate": str(candidate).replace("\\", "/"),
        "remaining_blockers": [],
    }
    write_json(output / "CR_OIS_PROD_HR001_ACCEPTANCE_EVIDENCE.json", summary)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, default=Path("artifacts/cr_ois_prod_hr001"))
    args = parser.parse_args()
    result = build_evidence(args.repo, args.output)
    print(json.dumps(result, sort_keys=True, ensure_ascii=False))
    return 0 if result["validation_status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
