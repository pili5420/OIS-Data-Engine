from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from src.runtime.engine import STATE_PATH, git, validate_bundle
from src.runtime.validation import PUBLIC_FILES, read_json, require


ALLOWED_FILES = {f"data/production/{filename}" for filename in PUBLIC_FILES} | {STATE_PATH} | {
    f"data/production/ois_{key}_{suffix}" for key in ("wti", "brent") for suffix in ("clean.csv", "indicators.json")}


def publish(root: Path, candidate: Path, branch: str, *, dry_run: bool = False) -> str:
    manifest = read_json(candidate / "manifest.json")
    require(manifest["validation_status"] == "PASS" and manifest["publishable"] is True, "CANDIDATE_NOT_PUBLISHABLE")
    require(set(manifest["files"]) == ALLOWED_FILES, "PUBLISH_FILE_ALLOWLIST")
    require(git(root, "rev-parse", "HEAD") == manifest["base_commit"], "CANDIDATE_BASE_CHANGED")
    git(root, "check-ref-format", f"refs/heads/{branch}")
    for filename, expected in manifest["files"].items():
        require(hashlib.sha256((candidate / filename).read_bytes()).hexdigest() == expected, "CANDIDATE_HASH_MISMATCH")
    validate_bundle(candidate, datetime.now(timezone.utc))
    require(read_json(candidate / STATE_PATH)["snapshot_id"] == manifest["snapshot_id"], "MANIFEST_SNAPSHOT_MISMATCH")
    base = manifest["base_commit"]
    # Build a whole Git tree using a separate index. The checkout and production
    # files are never copied/replaced, even when push or the process fails.
    with tempfile.TemporaryDirectory(prefix="ois-index-") as temporary:
        env = {**os.environ, "GIT_INDEX_FILE": str(Path(temporary) / "index")}
        def command(*args: str, input: str | None = None) -> str:
            return subprocess.check_output(["git", "-C", str(root), *args], input=input, env=env, text=True).strip()
        command("read-tree", base)
        for filename in sorted(ALLOWED_FILES):
            blob = command("hash-object", "-w", "--no-filters", str((candidate / filename).resolve()))
            command("update-index", "--add", "--cacheinfo", "100644", blob, filename)
        tree = command("write-tree")
        if dry_run:
            return tree
        commit = command("commit-tree", tree, "-p", base, input=f"Publish validated OIS snapshot {manifest['snapshot_id'][:12]}\n")
    # A single non-forced ref update exposes all files atomically. A concurrent
    # writer makes this commit non-fast-forward; never rebase stale candidates.
    for attempt in range(3):
        remote = git(root, "ls-remote", "origin", f"refs/heads/{branch}").split()
        require(bool(remote), "PUBLISH_REMOTE_BRANCH_MISSING")
        if remote[0] == commit:
            return commit  # push succeeded but its network response was lost
        require(remote[0] == base, "PUBLISH_CONCURRENT_BRANCH_UPDATE")
        result = subprocess.run(["git", "-C", str(root), "push", "origin", f"{commit}:refs/heads/{branch}"], check=False)
        if result.returncode == 0:
            return commit
        if attempt < 2:
            time.sleep(2 ** (attempt + 1))
    # One last read resolves a lost final response without a duplicate commit.
    require(git(root, "ls-remote", "origin", f"refs/heads/{branch}").split()[0] == commit, "PUBLISH_PUSH_FAILED")
    return commit


def main():
    parser = argparse.ArgumentParser(description="Validate candidate and atomically publish one complete Git snapshot.")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--branch", default="main")
    parser.add_argument("--dry-run", action="store_true", help="Validate and build the Git tree without publishing")
    args = parser.parse_args()
    commit = publish(args.root, args.candidate, args.branch, dry_run=args.dry_run)
    print(commit)
    if not args.dry_run and os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as handle:
            handle.write(f"sha={commit}\n")


if __name__ == "__main__":
    main()
