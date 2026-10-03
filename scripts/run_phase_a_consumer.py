from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.runtime.consumer import build_phase_a_consumer_evidence


def main() -> int:
    parser = argparse.ArgumentParser(description="Run OIS Production Bundle Phase A consumer preflight.")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--previous-state", required=True)
    parser.add_argument("--root", default=".")
    parser.add_argument("--expected-run-id", required=True)
    parser.add_argument("--expected-commit-sha", required=True)
    parser.add_argument("--expected-production-snapshot-id", required=True)
    parser.add_argument("--expected-previous-state-id", required=True)
    parser.add_argument("--expected-previous-state-hash", required=True)
    parser.add_argument("--render-preview-status", default="NOT_EXECUTED")
    parser.add_argument("--output")
    args = parser.parse_args()
    evidence = build_phase_a_consumer_evidence(
        manifest_path=args.manifest,
        previous_state_path=args.previous_state,
        root=args.root,
        expected_run_id=args.expected_run_id,
        expected_commit_sha=args.expected_commit_sha,
        expected_production_snapshot_id=args.expected_production_snapshot_id,
        expected_previous_state_id=args.expected_previous_state_id,
        expected_previous_state_hash=args.expected_previous_state_hash,
        render_preview_status=args.render_preview_status,
    )
    text = json.dumps(evidence, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(text, encoding="utf-8")
    print(text, end="")
    return 0 if evidence["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
