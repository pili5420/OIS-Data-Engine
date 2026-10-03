from __future__ import annotations

import copy
import json
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from src.runtime.engine import write_json
from src.runtime.shadow_manifest import CONTRACT_VERSION, build_shadow_manifest, validate_shadow_manifest
from src.runtime.validation import PUBLIC_FILES


ROOT = Path(__file__).resolve().parents[1]


class OisShadowManifestTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.production = self.tmp / "data" / "production"
        self.production.mkdir(parents=True)
        self.now = datetime(2026, 9, 15, 1, 0, tzinfo=timezone.utc)
        common = {
            "runtime_contract_version": "1.0",
            "generated_at": "2026-09-15T00:30:00Z",
            "data_as_of": "2026-09-14",
            "source_as_of": "2026-09-14",
            "source": {"wti": "fixture", "brent": "fixture"},
            "source_timestamp": {"wti": "2026-09-14T00:00:00Z", "brent": "2026-09-14T00:00:00Z"},
            "snapshot_id": "a" * 64,
            "production_snapshot_id": "a" * 64,
            "run_id": "run-1",
            "commit_sha": "b" * 40,
            "published": True,
            "validation_status": "PASS",
            "quality_flags": [],
            "missing_fields": [],
            "duplicate_status": "PASS",
            "freshness_status": "PASS",
            "lineage": {"production_snapshot_id": "a" * 64, "run_id": "run-1", "commit_sha": "b" * 40, "source_as_of": "2026-09-14"},
        }
        docs = {
            "ois_status.json": {**common, "schema_version": "OIS-STATUS-1.0", "status": "PASS", "validation": "PASS"},
            "ois_ingestion_validation.json": {**common, "schema_version": "OIS-VALIDATION-1.0", "overall_validation": "PASS"},
            "ois_chart_payload.json": {**common, "schema_version": "OIS-CHART-1.0", "datasets": {}},
            "ois_chart_rolling_180.json": {**common, "schema_version": "OIS-ROLLING-180-1.0", "window_size": 180, "datasets": {}},
        }
        for name, doc in docs.items():
            write_json(self.production / name, doc)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def manifest(self, **overrides):
        values = {
            "production_dir": self.production,
            "cadence": "OIS_0735_PREMARKET",
            "event": "schedule",
            "market_date": "2026-09-15",
            "generated_at": "2026-09-15T00:45:00Z",
        }
        values.update(overrides)
        return build_shadow_manifest(**values)

    def test_contract_binds_only_four_public_artifacts(self):
        manifest = self.manifest()
        self.assertEqual(manifest["version"], CONTRACT_VERSION)
        self.assertEqual({Path(item["path"]).name for item in manifest["payload_references"]}, set(PUBLIC_FILES))
        result = validate_shadow_manifest(manifest, root=self.tmp, expected_run_id="run-1", expected_commit_sha="b" * 40, now=self.now)
        self.assertEqual(result["validation_status"], "PASS")
        self.assertFalse(result["state_mutation_allowed"])
        self.assertFalse(result["portfolio_mutation_allowed"])
        self.assertFalse(result["ledger_mutation_allowed"])

    def test_empty_blocked_dependencies_can_pass(self):
        manifest = self.manifest(blocked_dependencies=[])
        result = validate_shadow_manifest(manifest, root=self.tmp, now=self.now)
        self.assertEqual(result["validation_status"], "PASS")

    def test_non_empty_blocked_dependencies_fail_closed(self):
        manifest = self.manifest(blocked_dependencies=["UPSTREAM_DEPENDENCY_HOLD"])
        result = validate_shadow_manifest(manifest, root=self.tmp, now=self.now)
        self.assertEqual(result["validation_status"], "FAIL_CLOSED")
        self.assertIn("BLOCKED_DEPENDENCIES_PRESENT", result["errors"])
        self.assertFalse(result["state_mutation_allowed"])
        self.assertFalse(result["portfolio_mutation_allowed"])
        self.assertFalse(result["ledger_mutation_allowed"])

    def test_missing_artifact_fails_before_manifest_creation(self):
        (self.production / "ois_status.json").unlink()
        with self.assertRaisesRegex(ValueError, "MISSING_ARTIFACT"):
            self.manifest()

    def test_negative_paths_fail_closed(self):
        base = self.manifest()
        cases = {}
        cases["invalid_snapshot_binding"] = (copy.deepcopy(base), {"expected_production_snapshot_id": "c" * 64})
        cases["commit_mismatch"] = (copy.deepcopy(base), {"expected_commit_sha": "d" * 40})
        cases["run_id_mismatch"] = (copy.deepcopy(base), {"expected_run_id": "other"})
        future = copy.deepcopy(base)
        future["generated_at"] = "2026-09-16T00:45:00Z"
        future["manifest_sha256"] = "bad"
        cases["future_dated_artifact"] = (future, {})
        stale = copy.deepcopy(base)
        stale["generated_at"] = (self.now - timedelta(days=3)).isoformat().replace("+00:00", "Z")
        stale["manifest_sha256"] = "bad"
        cases["stale_artifact"] = (stale, {})
        failed = copy.deepcopy(base)
        failed["validation"] = {"status": "FAIL", "source": "bound_public_artifacts"}
        failed["manifest_sha256"] = "bad"
        cases["validation_fail"] = (failed, {})
        previous = copy.deepcopy(base)
        previous["previous_state_requirement"] = "OPTIONAL"
        previous["manifest_sha256"] = "bad"
        cases["invalid_previous_state_requirement"] = (previous, {})
        corrupted = copy.deepcopy(base)
        corrupted["manifest_sha256"] = "0" * 64
        cases["corrupted_manifest"] = (corrupted, {})
        missing_ref = copy.deepcopy(base)
        missing_ref["payload_references"] = missing_ref["payload_references"][1:]
        missing_ref["manifest_sha256"] = "bad"
        cases["missing_artifact_reference"] = (missing_ref, {})
        for name, (manifest, expectations) in cases.items():
            with self.subTest(name=name):
                result = validate_shadow_manifest(manifest, root=self.tmp, now=self.now, **expectations)
                self.assertEqual(result["validation_status"], "FAIL_CLOSED")
                self.assertFalse(result["state_mutation_allowed"])
                self.assertFalse(result["portfolio_mutation_allowed"])
                self.assertFalse(result["ledger_mutation_allowed"])

    def test_payload_hash_and_binding_mismatch_fail_closed(self):
        manifest = self.manifest()
        status = json.loads((self.production / "ois_status.json").read_text(encoding="utf-8"))
        status["run_id"] = "other"
        write_json(self.production / "ois_status.json", status)
        result = validate_shadow_manifest(manifest, root=self.tmp, now=self.now)
        self.assertEqual(result["validation_status"], "FAIL_CLOSED")
        self.assertTrue(any("HASH_MISMATCH" in error or "BINDING_MISMATCH" in error for error in result["errors"]))

    def test_existing_scheduler_file_is_not_migrated(self):
        workflow = yaml.load((ROOT / ".github/workflows/ois_production.yml").read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
        self.assertEqual(workflow["on"]["schedule"], [{"cron": "30 10 * * *"}])


if __name__ == "__main__":
    unittest.main()
