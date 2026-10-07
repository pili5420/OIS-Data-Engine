from __future__ import annotations

import copy
import csv
import io
import json
import math
import os
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
from src.runtime.engine import (CSV_FIELDS, PRODUCTION_BUNDLE_MANIFEST_PATH, STATE_PATH, build_candidate, encoded,
                                git, load_previous, merge_history, validate_bundle, validate_legacy_bundle, write_json)
from src.runtime.publish import publish
from src.runtime.revisions import REVISION_ID, match_approved_revision, revision_id_for_date, row_hash
from src.runtime.shadow_manifest import CONTRACT_VERSION, validate_shadow_manifest
from src.runtime.source import IntegrityError, TransientError, get_json, latest_completed, normalize, schedule, stamp
from src.runtime.validation import FIELDS, PUBLIC_FILES, read_json, validate_history, validate_indicators


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime.now(timezone.utc)




def fixture_rows_for(now):
    end = latest_completed(now)
    days = list(schedule((now - timedelta(days=720)).date().isoformat(), end))[-410:]
    return [{"date": day, "open": 80 + math.sin(i / 5), "high": 83 + math.sin(i / 5),
             "low": 78 + math.sin(i / 5), "close": 81 + math.sin(i / 5), "volume": 1000 + i,
             "source": "yahoo_chart", "source_timestamp": day + "T04:00:00Z"} for i, day in enumerate(days)]

def fixture_rows():
    return fixture_rows_for(NOW)


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
        merged, revisions, accepted = merge_history(self.rows, self.rows[20:], True)
        self.assertEqual(merged, self.rows)
        self.assertEqual(revisions, 0)
        self.assertEqual(accepted, [])
        changed = copy.deepcopy(self.rows)
        changed[-2]["close"] += .1
        with self.assertRaisesRegex(IntegrityError, "SOURCE_REVISION"):
            merge_history(self.rows, changed, True)

    def test_revision_matcher_scans_superseded_same_day_evidence(self):
        old_row = copy.deepcopy(self.rows[-2])
        old_row["date"] = "2026-09-11"
        old_row["source_timestamp"] = "2026-09-11T04:00:00Z"
        incoming = copy.deepcopy(old_row)
        incoming["volume"] = float(incoming["volume"]) + 17
        stale_old = copy.deepcopy(old_row)
        stale_old["close"] = float(stale_old["close"]) - 1
        evidence = {"revisions": [
            {
                "revision_id": REVISION_ID, "source_provenance": "yahoo_chart",
                "upstream_revision_timestamp": old_row["date"] + "T04:00:00Z",
                "approval_status": "APPROVED", "revision_reason": "superseded upstream correction",
                "before_hash": row_hash(stale_old), "after_hash": row_hash(incoming),
                "affected_record": {"dataset": "historical_prices", "instrument": "wti", "date": old_row["date"],
                                    "fields": ["volume"],
                                    "old_values": {field: stale_old[field] for field in FIELDS},
                                    "corrected_values": {field: incoming[field] for field in FIELDS}},
            },
            {
                "revision_id": REVISION_ID, "source_provenance": "yahoo_chart",
                "upstream_revision_timestamp": old_row["date"] + "T04:00:00Z",
                "approval_status": "APPROVED", "revision_reason": "current upstream correction",
                "before_hash": row_hash(old_row), "after_hash": row_hash(incoming),
                "affected_record": {"dataset": "historical_prices", "instrument": "wti", "date": old_row["date"],
                                    "fields": ["volume"],
                                    "old_values": {field: old_row[field] for field in FIELDS},
                                    "corrected_values": {field: incoming[field] for field in FIELDS}},
            },
        ]}
        accepted = match_approved_revision(evidence=evidence, instrument="wti", old_row=old_row, incoming_row=incoming)
        self.assertIsNotNone(accepted)
        self.assertEqual(accepted["before_hash"], row_hash(old_row))
        merged, revisions, accepted_rows = merge_history([old_row], [incoming], True, instrument="wti", revision_evidence=evidence)
        self.assertEqual(revisions, 1)
        self.assertEqual(accepted_rows[0]["after_hash"], row_hash(incoming))
        self.assertEqual(merged[0], incoming)

    def test_revision_matcher_accepts_dynamic_revision_id_date(self):
        old_row = copy.deepcopy(self.rows[-2])
        old_row["date"] = "2026-09-25"
        old_row["source_timestamp"] = "2026-09-25T04:00:00Z"
        incoming = copy.deepcopy(old_row)
        incoming["volume"] = float(incoming["volume"]) + 29
        evidence = {"revisions": [{
            "revision_id": revision_id_for_date(old_row["date"]),
            "source_provenance": "yahoo_chart",
            "upstream_revision_timestamp": "2026-09-29T13:25:36Z",
            "approval_status": "APPROVED",
            "revision_reason": "dynamic upstream correction",
            "before_hash": row_hash(old_row),
            "after_hash": row_hash(incoming),
            "affected_record": {"dataset": "historical_prices", "instrument": "brent", "date": old_row["date"],
                                "fields": ["volume"],
                                "old_values": {field: old_row[field] for field in FIELDS},
                                "corrected_values": {field: incoming[field] for field in FIELDS}},
        }]}
        accepted = match_approved_revision(evidence=evidence, instrument="brent", old_row=old_row, incoming_row=incoming)
        self.assertIsNotNone(accepted)
        self.assertEqual(accepted["revision_id"], "HISTORICAL_SOURCE_REVISION:2026-09-25")
        merged, revisions, accepted_rows = merge_history([old_row], [incoming], True, instrument="brent", revision_evidence=evidence)
        self.assertEqual(revisions, 1)
        self.assertEqual(accepted_rows[0]["date"], "2026-09-25")
        self.assertEqual(merged[0], incoming)

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
        cls.temp = None
        cls.base = ROOT / "artifacts" / f"ois-runtime-tests-local-{os.getpid()}"
        shutil.rmtree(cls.base, ignore_errors=True)
        cls.base.mkdir(parents=True, exist_ok=True)
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
        shutil.rmtree(cls.base, ignore_errors=True)

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

    def test_candidate_materializes_production_bundle_manifest(self):
        manifest_path = self.candidate / PRODUCTION_BUNDLE_MANIFEST_PATH
        self.assertTrue(manifest_path.is_file())
        manifest = read_json(manifest_path)
        self.assertEqual(manifest["version"], CONTRACT_VERSION)
        self.assertEqual(manifest["validation"]["status"], "PASS")
        self.assertEqual(manifest["freshness"]["status"], "PASS")
        self.assertEqual(manifest["blocked_dependencies"], [])
        self.assertEqual({Path(ref["path"]).name for ref in manifest["payload_references"]}, set(PUBLIC_FILES))
        self.assertEqual({Path(ref["path"]).as_posix().split("/")[0] for ref in manifest["payload_references"]}, {"data"})
        validation = validate_shadow_manifest(manifest, root=self.candidate, now=NOW)
        self.assertEqual(validation["validation_status"], "PASS")

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


    def revision_root(self, label, *, evidence=True):
        now = datetime(2026, 9, 14, 10, tzinfo=timezone.utc)
        rows = fixture_rows_for(now)
        old_rows = copy.deepcopy(rows)
        new_rows = copy.deepcopy(rows)
        old_rows[-1]["low"] += 0.45
        old_rows[-1]["close"] -= 0.92
        old_rows[-1]["volume"] = 137938.0
        old_rows[-1]["source_timestamp"] = "2026-09-11T04:00:00Z"
        new_rows[-1]["source_timestamp"] = "2026-09-11T04:00:00Z"
        root = self.base / label
        root.mkdir()
        git(root, "init", "-b", "main")
        git(root, "config", "user.email", "ois-test@example.invalid")
        git(root, "config", "user.name", "OIS Test")
        git(root, "commit", "--allow-empty", "-m", "empty baseline")
        production = root / "data/production"
        production.mkdir(parents=True)
        for key, source_rows in (("wti", old_rows), ("brent", rows)):
            with (production / f"ois_{key}_clean.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
                writer.writeheader()
                writer.writerows({field: row[field] for field in CSV_FIELDS} for row in source_rows)
        baseline = self.base / (label + "-baseline")
        build_candidate(root, baseline, now, fetcher=lambda ticker, _: (copy.deepcopy(old_rows if ticker == "CL=F" else rows), []))
        shutil.copytree(baseline / "data", root / "data", dirs_exist_ok=True)
        git(root, "add", "data")
        git(root, "commit", "-m", "runtime baseline")
        if evidence:
            ev = {
                "schema_version": "OIS-HISTORICAL-REVISION-EVIDENCE-1.0",
                "validation_status": "PASS",
                "revisions": [{
                    "revision_id": REVISION_ID,
                    "source_provenance": "yahoo_chart",
                    "upstream_revision_timestamp": "2026-09-11T04:00:00Z",
                    "approval_status": "APPROVED",
                    "approved_by": "CR-OIS-PROD-HR-001",
                    "revision_reason": "Approved upstream historical correction test evidence.",
                    "before_hash": row_hash(old_rows[-1]),
                    "after_hash": row_hash(new_rows[-1]),
                    "affected_record": {"dataset": "historical_prices", "instrument": "wti", "date": "2026-09-11", "fields": ["low", "close", "volume"],
                                        "old_values": {field: old_rows[-1][field] for field in FIELDS},
                                        "corrected_values": {field: new_rows[-1][field] for field in FIELDS}}
                }]
            }
            write_json(root / "data/runtime/approved_historical_revisions.json", ev)
        return root, now, rows, old_rows, new_rows

    def test_approved_historical_revision_rebuilds_and_publishes_consistent_lineage(self):
        root, now, rows, old_rows, new_rows = self.revision_root("approved-revision-root")
        candidate = self.base / "approved-revision-candidate"
        result = build_candidate(root, candidate, now, fetcher=lambda ticker, _: (copy.deepcopy(new_rows if ticker == "CL=F" else rows), []))
        self.assertEqual(result["validation_status"], "PASS")
        self.assertEqual(result["accepted_historical_revisions"][0]["revision_id"], REVISION_ID)
        docs = {name: read_json(candidate / "data/production" / name) for name in PUBLIC_FILES}
        lineage = docs["ois_status.json"]["lineage"]
        self.assertEqual(lineage["historical_revision_recovery"], "PASS")
        self.assertEqual(lineage["approved_revision_count"], 1)
        self.assertTrue(all(doc["published"] is True for doc in docs.values()))
        self.assertEqual(len({doc["production_snapshot_id"] for doc in docs.values()}), 1)
        self.assertEqual(len({doc["run_id"] for doc in docs.values()}), 1)
        self.assertEqual(len({doc["commit_sha"] for doc in docs.values()}), 1)
        self.assertEqual(len({doc["source_as_of"] for doc in docs.values()}), 1)
        rolling = docs["ois_chart_rolling_180.json"]
        self.assertEqual(rolling["record_count"], 180)
        self.assertTrue(all(count == 180 for count in rolling["integrity"]["counts"].values()))
        self.assertEqual(rolling["update_mode"], "CONTROLLED_HISTORICAL_REBUILD")

    def test_unapproved_revision_before_after_hash_mismatch_fail_closed(self):
        for mutation in ("missing", "before", "after", "approval"):
            root, now, rows, old_rows, new_rows = self.revision_root("revision-" + mutation, evidence=(mutation != "missing"))
            if mutation != "missing":
                ev_path = root / "data/runtime/approved_historical_revisions.json"
                ev = read_json(ev_path)
                if mutation == "before": ev["revisions"][0]["before_hash"] = "0" * 64
                if mutation == "after": ev["revisions"][0]["after_hash"] = "0" * 64
                if mutation == "approval": ev["revisions"][0]["approval_status"] = "HOLD"
                write_json(ev_path, ev)
            before = {name: (root / "data/production" / name).read_bytes() for name in PUBLIC_FILES}
            with self.subTest(mutation=mutation), self.assertRaisesRegex(IntegrityError, "SOURCE_REVISION"):
                build_candidate(root, self.base / ("revision-blocked-" + mutation), now, fetcher=lambda ticker, _: (copy.deepcopy(new_rows if ticker == "CL=F" else rows), []))
            self.assertEqual(before, {name: (root / "data/production" / name).read_bytes() for name in PUBLIC_FILES})

    def test_approved_revision_deterministic_replay_hash_identical(self):
        root, now, rows, old_rows, new_rows = self.revision_root("revision-replay-root")
        first = self.base / "revision-replay-a"
        second = self.base / "revision-replay-b"
        build_candidate(root, first, now, fetcher=lambda ticker, _: (copy.deepcopy(new_rows if ticker == "CL=F" else rows), []))
        build_candidate(root, second, now, fetcher=lambda ticker, _: (copy.deepcopy(new_rows if ticker == "CL=F" else rows), []))
        self.assertEqual(read_json(first / STATE_PATH)["snapshot_id"], read_json(second / STATE_PATH)["snapshot_id"])
        self.assertEqual(read_json(first / "data/production/ois_chart_rolling_180.json")["datasets"], read_json(second / "data/production/ois_chart_rolling_180.json")["datasets"])


    def legacy_candidate_root(self, label):
        root = self.copy_candidate(label)
        for name in PUBLIC_FILES:
            doc_path = root / "data/production" / name
            doc = read_json(doc_path)
            for key in ("production_snapshot_id", "run_id", "commit_sha", "source_as_of", "published", "lineage"):
                doc.pop(key, None)
            write_json(doc_path, doc)
        return root

    def test_valid_legacy_snapshot_metadata_migration_passes_private_candidate_only(self):
        root = self.legacy_candidate_root("legacy-migration-root")
        before = {name: read_json(root / "data/production" / name) for name in PUBLIC_FILES}
        validate_legacy_bundle(root, NOW)
        candidate = self.base / "legacy-migration-candidate"
        result = build_candidate(root, candidate, NOW, fetcher=lambda *_: (copy.deepcopy(self.rows), []))
        self.assertTrue(result["legacy_metadata_migration"])
        docs = {name: read_json(candidate / "data/production" / name) for name in PUBLIC_FILES}
        self.assertTrue(all(doc["production_snapshot_id"] == doc["snapshot_id"] for doc in docs.values()))
        self.assertEqual(len({json.dumps(doc["lineage"], sort_keys=True) for doc in docs.values()}), 1)
        self.assertTrue(all(doc["lineage"]["migration_from_legacy"] is True for doc in docs.values()))
        self.assertTrue(all(doc["published"] is True for doc in docs.values()))
        self.assertEqual(read_json(candidate / STATE_PATH)["snapshot_id"], read_json(root / STATE_PATH)["snapshot_id"])
        self.assertEqual(read_json(candidate / "data/production/ois_chart_rolling_180.json")["datasets"], read_json(root / "data/production/ois_chart_rolling_180.json")["datasets"])
        validate_bundle(candidate, NOW)
        self.assertEqual(before, {name: read_json(root / "data/production" / name) for name in PUBLIC_FILES})

    def test_legacy_unknown_schema_partial_metadata_conflict_and_corruption_fail_closed(self):
        mutations = {
            "unknown_schema": lambda docs: docs["ois_status.json"].update(schema_version="OIS-STATUS-0.9"),
            "partial_metadata": lambda docs: docs["ois_status.json"].update(production_snapshot_id=docs["ois_status.json"]["snapshot_id"]),
            "conflicting_metadata": lambda docs: docs["ois_status.json"].update(production_snapshot_id=docs["ois_status.json"]["snapshot_id"], run_id="old", commit_sha="old", source_as_of=docs["ois_status.json"]["data_as_of"], published=True, lineage={"production_snapshot_id":"bad"}),
            "corrupt_cross_file": lambda docs: docs["ois_ingestion_validation.json"].update(snapshot_id="0" * 64),
        }
        for name, mutate in mutations.items():
            root = self.legacy_candidate_root("legacy-fail-" + name)
            docs = {filename: read_json(root / "data/production" / filename) for filename in PUBLIC_FILES}
            mutate(docs)
            for filename, doc in docs.items():
                write_json(root / "data/production" / filename, doc)
            before = {filename: (root / "data/production" / filename).read_bytes() for filename in PUBLIC_FILES}
            with self.subTest(name=name), self.assertRaises(IntegrityError):
                build_candidate(root, self.base / ("legacy-blocked-" + name), NOW, fetcher=lambda *_: (copy.deepcopy(self.rows), []))
            self.assertEqual(before, {filename: (root / "data/production" / filename).read_bytes() for filename in PUBLIC_FILES})

    def test_atomic_publisher_rejects_legacy_candidate_until_current_schema_passes(self):
        root = self.legacy_candidate_root("legacy-publish-root")
        candidate = self.base / "legacy-publish-candidate"
        build_candidate(root, candidate, NOW, fetcher=lambda *_: (copy.deepcopy(self.rows), []))
        manifest = read_json(candidate / "manifest.json")
        self.assertTrue(manifest["publishable"])
        status = read_json(candidate / "data/production/ois_status.json")
        status.pop("production_snapshot_id")
        write_json(candidate / "data/production/ois_status.json", status)
        with self.assertRaisesRegex(IntegrityError, "HASH_MISMATCH|SCHEMA"):
            publish(root, candidate, "main", dry_run=True)

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
        manifest = json.loads(git(self.repo, "show", f"{tree}:{PRODUCTION_BUNDLE_MANIFEST_PATH}"))
        self.assertEqual(manifest["version"], CONTRACT_VERSION)
        self.assertEqual(manifest["contract_validation"]["validation_status"], "PASS")
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
