from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.runtime.shadow_manifest import build_shadow_manifest, validate_shadow_manifest


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Build an OIS production bundle shadow manifest without publishing.")
    parser.add_argument("--production-dir", default="data/production")
    parser.add_argument("--output", default="data/production/ois_production_bundle_manifest_v1.json")
    parser.add_argument("--cadence", required=True)
    parser.add_argument("--event", default="manual")
    parser.add_argument("--market-date", required=True)
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    manifest = build_shadow_manifest(
        production_dir=root / args.production_dir,
        cadence=args.cadence,
        event=args.event,
        market_date=args.market_date,
        reference_root=root,
    )
    validation = validate_shadow_manifest(manifest, root=root)
    output = root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({**manifest, "contract_validation": validation}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(validation, sort_keys=True))
    return 0 if validation["validation_status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
