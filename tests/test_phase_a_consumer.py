from __future__ import annotations

import copy
import hashlib
import json
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from src.runtime.consumer import build_phase_a_consumer_evidence
from src.runtime.engine import write_json
from src.runtime.shadow_manifest import build_shadow_manifest
from src.runtime.validation import PUBLIC_FILES


class OisPhaseAConsumerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.now = datetime(2026, 9, 15, 1, 0, tzinfo=timezone.utc)
        self.production = self.tmp / "data" / "production"
        self.production.mkdir(parents=True)
        self.previous_state_path = self.tmp / "state" / "previous.json"
        self.previous_state_path.parent.mkdir(parents=True)
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
        self.write_json(self.previous_state_path, {"current_state_id": "ois-state-prev-1", "current_state_hash": "h1"})
        self.manifest_path = self.tmp / "manifest.json"
        self.write_json(self.manifest_path, self.manifest())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write_json(self, path, value):
        Path(path).write_text(json.dumps(value, ensure_ascii=False, sort_keys=True), encoding="utf-8")

    def resign(self, manifest):
        manifest["manifest_sha256"] = hashlib.sha256(json.dumps({k: v for k, v in manifest.items() if k != "manifest_sha256"}, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
        return manifest

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

    def evidence(self, **overrides):
        args = {
            "manifest_path": self.manifest_path,
            "previous_state_path": self.previous_state_path,
            "root": self.tmp,
            "expected_run_id": "run-1",
            "expected_commit_sha": "b" * 40,
            "expected_production_snapshot_id": "a" * 64,
            "expected_previous_state_id": "ois-state-prev-1",
            "now": self.now,
        }
        args.update(overrides)
        return build_phase_a_consumer_evidence(**args)

    def assert_fail(self, evidence, reason):
        self.assertEqual(evidence["status"], "FAIL_CLOSED")
        self.assertIn(reason, evidence["fail_closed_reason"])
        self.assertFalse(evidence["state_mutation_allowed"])
        self.assertFalse(evidence["portfolio_mutation_allowed"])
        self.assertFalse(evidence["ledger_mutation_allowed"])
        self.assertFalse(evidence["production_mutation_allowed"])
        self.assertFalse(evidence["fallback_used"])

    def test_positive_shadow_acceptance_preview_only(self):
        evidence = self.evidence()
        self.assertEqual(evidence["status"], "PASS")
        self.assertEqual(evidence["data_gate"]["status"], "PASS")
        self.assertEqual(evidence["render_gate"]["status"], "PASS")
        self.assertEqual(evidence["six_chart_preview_status"], "ALLOWED")
        self.assertEqual({Path(item["path"]).name for item in json.loads(self.manifest_path.read_text(encoding="utf-8"))["payload_references"]}, set(PUBLIC_FILES))
        self.assertIn("WTI", evidence["no_recalculation_evidence"]["not_calculated"])
        self.assertIn("rolling-180", evidence["no_recalculation_evidence"]["not_calculated"])

    def test_negative_manifest_paths(self):
        self.assert_fail(self.evidence(manifest_path=self.tmp / "missing.json"), "MISSING_MANIFEST")
        self.manifest_path.write_text("{", encoding="utf-8")
        self.assert_fail(self.evidence(), "CORRUPTED_MANIFEST")

    def test_negative_manifest_gates(self):
        cases = {
            "missing one official artifact": ("PUBLIC_ARTIFACT_SET_MISMATCH", lambda m: m.update({"payload_references": m["payload_references"][1:]})),
            "four-file snapshot mismatch": ("PRODUCTION_SNAPSHOT_BINDING_MISMATCH", lambda m: m.update({"production_snapshot_id": "c" * 64})),
            "run mismatch": ("RUN_ID_MISMATCH", lambda m: m.update({"run_id": "other"})),
            "commit mismatch": ("COMMIT_MISMATCH", lambda m: m.update({"commit_sha": "d" * 40})),
            "validation non-pass": ("VALIDATION_FAIL", lambda m: m.update({"validation": {"status": "FAIL", "source": "bound_public_artifacts"}})),
            "freshness non-pass": ("FRESHNESS_FAIL", lambda m: m.update({"freshness": {"status": "STALE"}})),
            "blocked dependency": ("BLOCKED_DEPENDENCIES_PRESENT", lambda m: m.update({"blocked_dependencies": ["UPSTREAM_BLOCK"]})),
        }
        for name, (reason, mutate) in cases.items():
            with self.subTest(name=name):
                manifest = copy.deepcopy(self.manifest())
                mutate(manifest)
                self.write_json(self.manifest_path, self.resign(manifest))
                self.assert_fail(self.evidence(), reason)

    def test_previous_state_negative_paths_and_continuity(self):
        self.assert_fail(self.evidence(previous_state_path=self.tmp / "missing-state.json"), "MISSING_PREVIOUS_STATE")
        self.previous_state_path.write_text("{", encoding="utf-8")
        self.assert_fail(self.evidence(), "CORRUPTED_PREVIOUS_STATE")
        self.write_json(self.previous_state_path, {"current_state_id": "other"})
        self.assert_fail(self.evidence(), "PREVIOUS_STATE_ID_MISMATCH")

    def test_render_gate_fail_with_data_gate_pass_uses_no_fallback(self):
        evidence = self.evidence(render_preview_status="FAIL")
        self.assertEqual(evidence["data_gate"]["status"], "PASS")
        self.assertEqual(evidence["render_gate"]["status"], "FAIL_CLOSED")
        self.assertEqual(evidence["six_chart_preview_status"], "BLOCKED")
        self.assertFalse(evidence["fallback_used"])
        self.assertIn("RENDER_GATE_FAIL", evidence["fail_closed_reason"])


if __name__ == "__main__":
    unittest.main()
