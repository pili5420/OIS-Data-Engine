from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path

from src.rate.engine import PUBLIC_FILES, RateIntegrityError

def publish(root: Path, candidate: Path) -> str:
    for name in PUBLIC_FILES:
        path = candidate / "data" / "rate" / "production" / name
        if not path.exists():
            raise RateIntegrityError(f"MISSING_CANDIDATE:{name}")
        document = json.loads(path.read_text(encoding="utf-8"))
        if document.get("validation_status") != "PASS":
            raise RateIntegrityError("CANDIDATE_NOT_PASS")
    destination = root / "data" / "rate" / "production"
    destination.mkdir(parents=True, exist_ok=True)
    temporary = root / "data" / "rate" / ".publish-candidate"
    if temporary.exists():
        raise RateIntegrityError("PUBLISH_TEMP_EXISTS")
    shutil.copytree(candidate / "data" / "rate" / "production", temporary)
    try:
        for name in PUBLIC_FILES:
            temporary.joinpath(name).replace(destination / name)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
    subprocess.run(["git", "-C", str(root), "add", "data/rate/production"], check=True)
    if subprocess.run(["git", "-C", str(root), "diff", "--cached", "--quiet"], check=False).returncode == 0:
        return subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "-C", str(root), "commit", "-m", "Publish validated RATE production data"], check=True)
    subprocess.run(["git", "-C", str(root), "push", "origin", "main"], check=True)
    return subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--candidate", type=Path, required=True)
    args = parser.parse_args()
    print(publish(args.root, args.candidate))

if __name__ == "__main__":
    main()
