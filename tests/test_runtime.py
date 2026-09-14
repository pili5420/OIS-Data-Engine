from __future__ import annotations

import copy
import csv
import io
import json
import math
import shutil
import subprocess
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

import yaml

from scripts.prepare_pages import prepare_pages
from src.indicators.technical import build_indicators
from src.runtime.engine import (CSV_FIELDS, STATE_PATH, build_candidate, encoded, git, load_previous,
                                merge_history, validate_bundle, write_json)
from src.runtime.publish import publish
from src.runtime.source import IntegrityError, TransientError, get_json, latest_completed, normalize, schedule, stamp
from src.runtime.validation import PUBLIC_FILES, read_json, validate_history, validate_indicators


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime.now(timezone.utc)


def fixture_rows():
    end = latest_completed(NOW)
    days = list(schedule((NOW - timedelta(days=720)).date().isoformat(), end))[-410:]
    return [{"date": day, "open": 80 + math.sin(i / 5), "high": 83 + math.sin(i / 5),
             "low": 78 + math.sin(i / 5), "close": 81 + math.sin(i / 5), "volume": 1000 + i,
             "source": "yahoo_chart", "source_timestamp": day + "T04:00:00Z"} for i, day in enumerate(days)]


class RuntimeUnitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows = fixture_rows()

    def test_reject_missing_type_nan_range_duplicate_weekend_future_and_stale(self):
        changes = [lambda r: r[-1].update(close=None), lambda r: r[-1].update(close=float("nan")),
                   lambda r: r[-1].update(volume=True), lambda r: r[-1].update(volume=-1),
                   lambda r: r[-1].update(high=1), lambda r: r[-1].update(date=r[-2]["date"]),
                   lambda r: r[-1].update(date="2026-09-12"), lambda r: r[-1].update(date="2099-01-01"),
                   lambda r: r.pop()]
        for change in changes:
            rows = copy.deepcopy(self.rows)
            change(rows)
            with self.subTest(change=change), self.assertRaises(ValueError):
                validate_history(rows, NOW)

    def test_requires_warmup_for_all_250_chart_points(self):
        with self.assertRaisesRegex(IntegrityError, "WARMUP"):
            validate_history(self.rows[-300:], NOW)

    def test_independent_decimal_historical_regression(self):
        validate_indicators(self.rows, build_indicators(self.rows))
        for key in ("wti", "brent"):
            with (ROOT / f"data/production/ois_{key}_clean.csv").open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            validate_indicators(rows, read_json(ROOT / f"data/production/ois_{key}_indicators.json"))

    def test_indicator_formula_mutation_detected(self):
        actual = build_indicators(self.rows)
        actual["data"][-20]["dea"] += 0.01
        with self.assertRaisesRegex(IntegrityError, "INDICATOR_DRIFT"):
            validate_indicators(self.rows, actual)

    def test_history_seed_stable_and_revisions_fail(self):
        merged, revisions = merge_history(self.rows, self.rows[20:], True)
        self.assertEqual(merged, self.rows)
        self.assertEqual(revisions, 0)
        changed = copy.deepcopy(self.rows)
        changed[-2]["close"] += .1
        with self.assertRaisesRegex(IntegrityError, "SOURCE_REVISION"):
            merge_history(self.rows, changed, True)

    def test_freshness_weekend_and_six_hour_finalization(self):
        self.assertEqual(latest_completed(datetime(2026, 9, 14, 10, tzinfo=timezone.utc)), "2026-09-11")
        self.assertEqual(latest_completed(datetime(2026, 9, 12, 0, tzinfo=timezone.utc)), "2026-09-10")
        self.assertEqual(latest_completed(datetime(2026, 9, 12, 4, tzinfo=timezone.utc)), "2026-09-11")

    def test_holiday_calendar_and_dst(self):
        self.assertNotIn("2025-12-25", schedule("2025-12-24", "2025-12-26"))
        self.assertEqual(schedule("2026-03-06", "2026-03-09")["2026-03-06"].hour, 22)
        self.assertEqual(schedule("2026-03-06", "2026-03-09")["2026-03-09"].hour, 21)

    def test_transient_retry_then_success(self):
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=None)
        response.read.return_value = b'{"ok": true}'
        opener = Mock(side_effect=[URLError("network"), TimeoutError(), response])
        sleep = Mock()
        self.assertEqual(get_json("https://example.test", opener=opener, sleep=sleep), {"ok": True})
        self.assertEqual(opener.call_count, 3)
        self.assertEqual([c.args[0] for c in sleep.call_args_list], [2, 4])

    def test_rate_limit_retry_budget_and_non_retryable_403(self):
        for code, expected, calls in ((429, TransientError, 3), (503, TransientError, 3), (403, IntegrityError, 1)):
            opener = Mock(side_effect=HTTPError("https://example.test", code, "error", {"Retry-After": "1"}, None))
            with self.subTest(code=code), self.assertRaises(expected):
                get_json("https://example.test", opener=opener, sleep=Mock())
            self.assertEqual(opener.call_count, calls)

    def test_invalid_json_not_retried(self):
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=None)
        response.read.return_value = b'not json'
        opener = Mock(return_value=response)
        with self.assertRaises(IntegrityError):
            get_json("https://example.test", opener=opener, sleep=Mock())
        self.assertEqual(opener.call_count, 1)

    def test_normalization_excludes_live_bar_but_rejects_missing_completed_bar(self):
        def payload(days, values):
            return {"chart": {"result": [{"meta": {"symbol": "CL=F", "instrumentType": "FUTURE", "exchangeName": "NYM", "exchangeTimezoneName": "America/New_York"},
                 "timestamp": [int(datetime.fromisoformat(day+"T04:00:00+00:00").timestamp()) for day in days],
                 "indicators": {"quote": [{field: values for field in ("open", "high", "low", "close", "volume")}]}}]}}
        now = datetime(2026, 9, 14, 10, tzinfo=timezone.utc)
        rows, flags = normalize(payload(["2026-09-11", "2026-09-14"], [80, 81]), "CL=F", now)
        self.assertEqual(len(rows), 1)
        self.assertIn("INCOMPLETE_SESSION_EXCLUDED", flags)
        for days, values in [(["2026-09-11"], [None]), (["2026-09-11", "2026-09-11"], [80, 81]), (["2026-09-12"], [80])]:
            with self.assertRaises(IntegrityError):
                normalize(payload(days, values), "CL=F", now)
        rows, flags = normalize(payload(["2025-05-23", "2025-05-26"], [80, None]), "CL=F", now)
        self.assertEqual(len(rows), 1)
        self.assertIn("EMPTY_SHORTENED_SESSION_EXCLUDED", flags)


class RuntimeIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="ois-runtime-tests-")
        cls.base = Path(cls.temp.name)
        cls.repo = cls.base / "repo"
        cls.repo.mkdir()
        git(cls.repo, "init", "-b", "main")
        git(cls.repo, "config", "user.email", "ois-test@example.invalid")
        git(cls.repo, "config", "user.name", "OIS Test")
        git(cls.repo, "commit", "--allow-empty", "-m", "Test baseline")
        cls.rows = fixture_rows()
        production = cls.repo / "data/production"
        production.mkdir(parents=True)
        for key in ("wti", "brent"):
            with (production / f"ois_{key}_clean.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
                writer.writeheader()
                writer.writerows({field: row[field] for field in CSV_FIELDS} for row in cls.rows)
        cls.candidate = cls.base / "candidate"
        build_candidate(cls.repo, cls.candidate, NOW, fetcher=lambda *_: (copy.deepcopy(cls.rows), []))

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def copy_candidate(self, label):
        path = self.base / label
        shutil.copytree(self.candidate, path)
        # TemporaryDirectory's cleanup handles read-only Git objects on Windows.
        return path

    def test_full_candidate_schema_and_four_file_pages(self):
        validate_bundle(self.candidate, NOW)
        pages = self.base / "pages"
        prepare_pages(self.candidate / "data/production", pages)
        self.addCleanup(shutil.rmtree, pages)
        self.assertEqual(set(PUBLIC_FILES), {f.name for f in pages.iterdir()})
        for name in PUBLIC_FILES:
            self.assertEqual((pages / name).read_bytes(), (self.candidate / "data/production" / name).read_bytes())

    def test_failures_leave_entire_previous_production_unchanged(self):
        root = self.copy_candidate("failed-attempts-root")
        before = {str(p): p.read_bytes() for p in (root / "data/production").glob("*")}
        for mutation in ("network", "null", "duplicate", "short"):
            rows = copy.deepcopy(self.rows)
            if mutation == "null": rows[-1]["close"] = None
            if mutation == "duplicate": rows[-1]["date"] = rows[-2]["date"]
            if mutation == "short": rows = rows[-179:]
            fetcher = Mock(side_effect=TransientError("SOURCE_RETRY_EXHAUSTED")) if mutation == "network" else lambda *_: (rows, [])
            with self.subTest(mutation=mutation), self.assertRaises((IntegrityError, TransientError)):
                build_candidate(root, self.base / ("fail-"+mutation), NOW, fetcher=fetcher)
            self.assertEqual(before, {str(p): p.read_bytes() for p in (root / "data/production").glob("*")})

    def test_incremental_append_drop_and_indicator_history_do_not_drift(self):
        root = self.copy_candidate("incremental-root")
        git(root, "init", "-b", "main")
        git(root, "config", "user.email", "ois-test@example.invalid")
        git(root, "config", "user.name", "OIS Test")
        git(root, "commit", "--allow-empty", "-m", "runtime baseline")
        last = self.rows[-1]["date"]
        next_date = next(day for day in schedule(last, (NOW+timedelta(days=10)).date().isoformat()) if day > last)
        next_now = schedule(next_date, next_date)[next_date] + timedelta(hours=7)
        new = {**self.rows[-1], "date": next_date, "source_timestamp": next_date+"T04:00:00Z", "close": 81.5}
        candidate = self.base / "incremental-candidate"
        self.addCleanup(lambda: shutil.rmtree(candidate) if candidate.exists() else None)
        build_candidate(root, candidate, next_now, fetcher=lambda *_: (self.rows[1:]+[new], []))
        old = read_json(root / "data/production/ois_chart_rolling_180.json")
        updated = read_json(candidate / "data/production/ois_chart_rolling_180.json")
        self.assertEqual(updated["appended_trading_days"], 1)
        self.assertEqual(updated["dropped_trading_days"], 1)
        for key in old["datasets"]:
            self.assertEqual(old["datasets"][key][1:], updated["datasets"][key][:-1])

    def test_schema_cross_file_and_count_tampering_rejected(self):
        mutations = [("ois_status.json", lambda d: d.update(record_count="2")),
                     ("ois_chart_payload.json", lambda d: d["datasets"]["wti_macd"][-1].update(dif=999)),
                     ("ois_chart_rolling_180.json", lambda d: d["datasets"]["wti_rsi"].pop()),
                     ("ois_ingestion_validation.json", lambda d: d["checks"].update(freshness="FAIL")),
                     ("ois_status.json", lambda d: d.update(data_as_of="2020-01-01"))]
        root = self.copy_candidate("tamper")
        for filename, change in mutations:
            path = root / "data/production" / filename
            original = path.read_bytes()
            document = read_json(path)
            change(document)
            write_json(path, document)
            with self.subTest(filename=filename), self.assertRaises(IntegrityError):
                validate_bundle(root, NOW)
            path.write_bytes(original)

    def test_no_new_day_preserves_rows_and_revision_stops_established_runtime(self):
        root = self.copy_candidate("no-new-day-root")
        git(root, "init", "-b", "main")
        git(root, "config", "user.email", "ois-test@example.invalid")
        git(root, "config", "user.name", "OIS Test")
        git(root, "commit", "--allow-empty", "-m", "runtime baseline")
        candidate = self.base / "no-new-day-candidate"
        build_candidate(root, candidate, NOW, fetcher=lambda *_: (self.rows, []))
        before = {name: (root / "data/production" / name).read_bytes() for name in PUBLIC_FILES}
        for name in ("ois_chart_payload.json", "ois_chart_rolling_180.json"):
            self.assertEqual(read_json(root / "data/production" / name)["datasets"],
                             read_json(candidate / "data/production" / name)["datasets"])
        changed = copy.deepcopy(self.rows)
        changed[-1]["volume"] += 1
        with self.assertRaisesRegex(IntegrityError, "SOURCE_REVISION"):
            build_candidate(root, self.base / "revision-fail", NOW, fetcher=lambda *_: (changed, []))
        self.assertEqual(before, {name: (root / "data/production" / name).read_bytes() for name in PUBLIC_FILES})

    def test_offline_fixture_is_never_publishable(self):
        fixture = self.base / "fixture.json"
        write_json(fixture, {"commodities": dict.fromkeys(("wti", "brent"), self.rows)})
        root = self.base / "offline-candidate"
        result = build_candidate(self.repo, root, NOW, fixture=fixture)
        self.assertFalse(result["publishable"])
        with self.assertRaisesRegex(IntegrityError, "NOT_PUBLISHABLE"):
            publish(self.repo, root, "main")

    def test_git_tree_dry_run_has_all_outputs_without_changing_checkout(self):
        before = git(self.repo, "rev-parse", "HEAD")
        tree = publish(self.repo, self.candidate, "main", dry_run=True)
        for name in PUBLIC_FILES:
            document = json.loads(git(self.repo, "show", f"{tree}:data/production/{name}"))
            self.assertEqual(document["validation_status"], "PASS")
        self.assertEqual(before, git(self.repo, "rev-parse", "HEAD"))
        self.assertFalse((self.repo / STATE_PATH).exists())

    def test_tampered_candidate_hash_is_not_published(self):
        root = self.copy_candidate("hash-tamper")
        (root / "data/production/ois_status.json").write_bytes(b'{}')
        with self.assertRaisesRegex(IntegrityError, "HASH_MISMATCH"):
            publish(self.repo, root, "main")

    def test_atomic_remote_publish_and_concurrent_writer_rejection(self):
        remote = self.base / "remote.git"
        git(self.base, "init", "--bare", str(remote))
        git(self.repo, "remote", "add", "origin", str(remote))
        self.addCleanup(lambda: git(self.repo, "remote", "remove", "origin"))
        git(self.repo, "push", "origin", "main")
        baseline = git(self.repo, "rev-parse", "HEAD")
        commit = publish(self.repo, self.candidate, "main")
        self.assertEqual(commit, git(remote, "rev-parse", "main"))
        for name in PUBLIC_FILES:
            actual = git(remote, "show", f"main:data/production/{name}")
            self.assertEqual(json.loads(actual), read_json(self.candidate / "data/production" / name))
        writer = git(remote, "-c", "user.name=Other writer", "-c", "user.email=other@example.invalid",
                     "commit-tree", f"{commit}^{{tree}}", "-p", commit, "-m", "Concurrent writer")
        git(remote, "update-ref", "refs/heads/main", writer, commit)
        with self.assertRaisesRegex(IntegrityError, "CONCURRENT"):
            publish(self.repo, self.candidate, "main")
        self.assertEqual(writer, git(remote, "rev-parse", "main"))
        self.assertEqual(baseline, git(self.repo, "rev-parse", "HEAD"))

    def test_duplicate_json_keys_rejected(self):
        path = self.base / "duplicate.json"
        path.write_text('{"status":"PASS","status":"FAIL"}', encoding="utf-8")
        with self.assertRaisesRegex(IntegrityError, "DUPLICATE_JSON_KEY"):
            read_json(path)


class RuntimeWorkflowTests(unittest.TestCase):
    def test_single_schedule_and_publish_gates(self):
        workflow = yaml.load((ROOT / ".github/workflows/ois_production.yml").read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
        self.assertEqual(workflow["on"]["schedule"], [{"cron": "30 10 * * *"}])
        self.assertEqual(workflow["permissions"]["contents"], "read")
        self.assertEqual(workflow["concurrency"]["cancel-in-progress"], "false")
        self.assertFalse((ROOT / ".github/workflows/ois-data-update.yml").exists())
        steps = workflow["jobs"]["production"]["steps"]
        engine = next(i for i,s in enumerate(steps) if "src.runtime.engine" in s.get("run", ""))
        publish_index = next(i for i,s in enumerate(steps) if s.get("id") == "publish")
        self.assertLess(engine, publish_index)
        self.assertIn("inputs.dry_run", steps[publish_index]["if"])
        self.assertFalse(any(s.get("continue-on-error") == "true" for s in steps))


if __name__ == "__main__":
    unittest.main()
