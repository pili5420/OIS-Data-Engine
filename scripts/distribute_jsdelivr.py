from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen

from scripts.prepare_pages import PUBLIC_FILES, load_pass_production


ENDPOINT_NAMES = {
    "ois_chart_payload.json": "CHART_PAYLOAD",
    "ois_ingestion_validation.json": "VALIDATION",
    "ois_status.json": "STATUS",
}
RETRY_DELAYS = (0, 5, 15, 30)


def timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def make_report(repository: str, commit: str) -> dict:
    if not re.fullmatch(r"[A-Za-z0-9-]+/[A-Za-z0-9_.-]+", repository):
        raise ValueError("Expected a GitHub owner/repository")
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("Immutable endpoints require a full 40-character commit SHA")
    owner, repo = repository.split("/")
    endpoints = {}
    files = {}
    for filename, name in ENDPOINT_NAMES.items():
        path = f"data/production/{filename}"
        main_url = f"https://cdn.jsdelivr.net/gh/{repository}@main/{path}"
        immutable_url = f"https://cdn.jsdelivr.net/gh/{repository}@{commit}/{path}"
        endpoints[f"JSDELIVR_{name}_ENDPOINT"] = main_url
        endpoints[f"IMMUTABLE_{name}_ENDPOINT"] = immutable_url
        files[filename] = {
            "main_url": main_url,
            "immutable_url": immutable_url,
            "pages_fallback_url": f"https://{owner}.github.io/{repo}/{filename}",
            "raw_fallback_url": f"https://raw.githubusercontent.com/{repository}/main/{path}",
            "purge": {"status": "NOT_RUN"},
            "immutable": {"status": "NOT_RUN"},
            "main": {"status": "NOT_RUN"},
        }
    return {
        "generated_at": timestamp(),
        "repository": repository,
        "production_commit": commit,
        "overall_status": "FAIL",
        "endpoints": endpoints,
        "files": files,
        "errors": [],
    }


def request(url: str) -> tuple[int, str, bytes]:
    req = Request(url, headers={"User-Agent": "OIS-Data-Engine/1.0", "Accept": "application/json"})
    with urlopen(req, timeout=30) as response:
        return response.status, response.headers.get_content_type(), response.read()


def verify_endpoint(url: str, expected: bytes) -> dict:
    expected_hash = hashlib.sha256(expected).hexdigest()
    last_error = "Endpoint was not checked"
    for attempt, delay in enumerate(RETRY_DELAYS, 1):
        if delay:
            time.sleep(delay)
        try:
            status, content_type, body = request(url)
            if status != 200 or content_type != "application/json":
                raise ValueError(f"Expected HTTP 200 and application/json, got {status} and {content_type}")
            json.loads(body)
            received_hash = hashlib.sha256(body).hexdigest()
            if received_hash != expected_hash:
                raise ValueError(f"Stale or mismatched JSON: expected {expected_hash}, received {received_hash}")
            return {
                "status": "PASS", "http_status": status, "content_type": content_type,
                "sha256": received_hash, "attempts": attempt, "checked_at": timestamp(),
            }
        except (OSError, ValueError) as exc:
            last_error = str(exc)
    raise RuntimeError(f"GET verification failed for {url}: {last_error}")


def purge_endpoint(main_url: str) -> dict:
    path = urlsplit(main_url).path
    status, _, body = request("https://purge.jsdelivr.net" + path)
    if status not in (200, 202):
        raise RuntimeError(f"Purge request failed: HTTP {status}")
    result = json.loads(body)
    for _ in range(20):
        if result.get("status") == "finished":
            break
        request_id = result.get("id")
        if not isinstance(request_id, str) or not request_id:
            raise RuntimeError("Purge response did not contain a request ID")
        time.sleep(3)
        status, _, body = request("https://purge.jsdelivr.net/status/" + quote(request_id, safe=""))
        if status != 200:
            raise RuntimeError(f"Purge status request failed: HTTP {status}")
        result = json.loads(body)
    outcome = result.get("paths", {}).get(path, {})
    providers = outcome.get("providers", {})
    if (result.get("status") != "finished" or outcome.get("throttled") is not False
            or not providers or any(value is not True for value in providers.values())):
        raise RuntimeError(f"Purge did not complete successfully: {json.dumps(result)}")
    return {"status": "PASS", "request_id": result.get("id"), "providers": providers}


def distribute(repository: str, commit: str, source: Path) -> dict:
    report = make_report(repository, commit)
    try:
        contents = load_pass_production(source)
        head = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if head != commit:
            raise ValueError("Checkout must match the post-push production commit")
        # Git blobs avoid checkout line-ending conversion and bind verification to the pushed snapshot.
        committed = {}
        for filename in PUBLIC_FILES:
            body = subprocess.check_output(["git", "show", f"{commit}:data/production/{filename}"])
            if json.loads(body) != json.loads(contents[filename]):
                raise ValueError(f"Uncommitted production changes: {filename}")
            committed[filename] = body
        for filename, body in committed.items():
            entry = report["files"][filename]
            entry["expected_sha256"] = hashlib.sha256(body).hexdigest()
            checks = (
                ("immutable", lambda: verify_endpoint(entry["immutable_url"], body)),
                ("purge", lambda: purge_endpoint(entry["main_url"])),
                ("main", lambda: verify_endpoint(entry["main_url"], body)),
            )
            for name, check in checks:
                try:
                    entry[name] = check()
                except (OSError, ValueError, RuntimeError) as exc:
                    entry[name] = {"status": "FAIL", "error": str(exc)}
                    report["errors"].append(f"{filename} {name}: {exc}")
        if not report["errors"]:
            report["overall_status"] = "PASS"
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as exc:
        report["errors"].append(str(exc))
    report["completed_at"] = timestamp()
    return report


def write_report(report: dict, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    lines = ["## OIS jsDelivr Distribution", "", f"Result: **{report['overall_status']}**", "",
             f"Production commit: `{report['production_commit']}`", ""]
    for name, url in report["endpoints"].items():
        lines.append(f"- {name} = [{url}]({url})")
        print(f"{name}={url}")
    if report["errors"]:
        lines.extend(["", "Verification errors:", "", *[f"- {error}" for error in report["errors"]]])
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with Path(summary_path).open("a", encoding="utf-8") as handle:
            handle.write("\n".join(lines) + "\n")
    print(f"JSDELIVR_DISTRIBUTION={report['overall_status']}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Purge and verify jsDelivr URLs for the pushed PASS dataset.")
    parser.add_argument("--repository", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--source", type=Path, default=Path("data/production"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = distribute(args.repository, args.commit, args.source)
    write_report(report, args.output)
    return 0 if report["overall_status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
