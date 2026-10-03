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
from src.work_state import STATE_ROOT, SYSTEM, STATE_VERSION, build_initial_state, calculate_state_hash, ledger_source_for_state, state_id


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
        self.previous_state = self.previous_state_fixture()
        self.write_json(self.previous_state_path, self.previous_state)
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
            "expected_previous_state_id": self.previous_state["current_state_id"],
            "expected_previous_state_hash": self.previous_state["current_state_hash"],
            "render_preview_status": "PASS",
            "now": self.now,
        }
        args.update(overrides)
        return build_phase_a_consumer_evidence(**args)

    def previous_state_fixture(self):
        production = {
            "production_snapshot_id": "a" * 64,
            "source_run_id": "run-1",
            "source_commit_sha": "b" * 40,
            "source_as_of": "2026-09-14",
            "production_lineage": {
                "production_snapshot_id": "a" * 64,
                "run_id": "run-1",
                "commit_sha": "b" * 40,
                "source_as_of": "2026-09-14",
            },
        }
        bundle = build_initial_state(work_execution_id="phase-a-prev-001", production=production, created_at="2026-09-15T00:00:00Z")
        store = self.tmp / STATE_ROOT
        (store / "history").mkdir(parents=True)
        self.write_json(store / "current_state.json", bundle["state"])
        self.write_json(store / "history" / f"{bundle['state']['current_state_id']}.json", bundle["state"])
        self.write_json(store / "portfolio_ledger.json", bundle["portfolio_ledger"])
        self.write_json(store / "transaction_ledger.json", bundle["transaction_ledger"])
        return bundle["state"]

    def refresh_state_identity(self, state):
        state["current_state_hash"] = calculate_state_hash(state)
        state["current_state_id"] = state_id(system=SYSTEM, state_version=STATE_VERSION, production_snapshot_id=state["production_snapshot_id"], work_execution_id=state["work_execution_id"], state_hash=state["current_state_hash"])
        state["lineage"]["current_state_hash"] = state["current_state_hash"]
        state["lineage"]["current_state_id"] = state["current_state_id"]
        state["current_state_hash"] = calculate_state_hash(state)
        state["current_state_id"] = state_id(system=SYSTEM, state_version=STATE_VERSION, production_snapshot_id=state["production_snapshot_id"], work_execution_id=state["work_execution_id"], state_hash=state["current_state_hash"])
        state["lineage"]["current_state_hash"] = state["current_state_hash"]
        state["lineage"]["current_state_id"] = state["current_state_id"]
        return state

    def rebind_ledgers(self, state):
        source = ledger_source_for_state(state)
        for name in ("portfolio_ledger.json", "transaction_ledger.json"):
            path = self.tmp / STATE_ROOT / name
            ledger = json.loads(path.read_text(encoding="utf-8"))
            ledger["current_state_id"] = state["current_state_id"]
            ledger["current_state_hash"] = state["current_state_hash"]
            ledger["source"] = source
            self.write_json(path, ledger)

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
        self.assert_fail(self.evidence(expected_previous_state_id="other"), "PREVIOUS_STATE_ID_MISMATCH")

    def test_previous_state_semantic_and_ledger_corruption_fail_closed(self):
        state = copy.deepcopy(self.previous_state)
        state["decision_state"]["signals"].append({"symbol": "FAKE"})
        self.write_json(self.previous_state_path, state)
        self.assert_fail(self.evidence(), "WORK_STATE_HASH_MISMATCH")

        state = copy.deepcopy(self.previous_state)
        state["lineage"]["current_state_id"] = "stale"
        self.write_json(self.previous_state_path, state)
        self.assert_fail(self.evidence(), "WORK_STATE_LINEAGE_ID")
        self.write_json(self.previous_state_path, self.previous_state)
        self.rebind_ledgers(self.previous_state)

        portfolio_path = self.tmp / STATE_ROOT / "portfolio_ledger.json"
        portfolio = json.loads(portfolio_path.read_text(encoding="utf-8"))
        portfolio["current_state_hash"] = "0" * 64
        self.write_json(portfolio_path, portfolio)
        self.assert_fail(self.evidence(), "WORK_STATE_SSOT_PORTFOLIO_LEDGER_STATE_HASH")
        source = ledger_source_for_state(self.previous_state)
        portfolio["current_state_hash"] = self.previous_state["current_state_hash"]
        portfolio["source"] = source
        self.write_json(portfolio_path, portfolio)

        transaction_path = self.tmp / STATE_ROOT / "transaction_ledger.json"
        transactions = json.loads(transaction_path.read_text(encoding="utf-8"))
        transactions["source"]["source_run_id"] = "stale-run"
        self.write_json(transaction_path, transactions)
        self.assert_fail(self.evidence(), "WORK_STATE_SSOT_TRANSACTION_LEDGER_SOURCE")

    def test_previous_state_reset_and_rollback_fail_closed(self):
        state = copy.deepcopy(self.previous_state)
        state["decision_state"]["state_reset_detected"] = True
        self.refresh_state_identity(state)
        self.rebind_ledgers(state)
        self.write_json(self.previous_state_path, state)
        self.assert_fail(self.evidence(expected_previous_state_hash=state["current_state_hash"]), "WORK_STATE_PRIOR_STATE_RESET")

        self.write_json(self.previous_state_path, self.previous_state)
        self.rebind_ledgers(self.previous_state)
        portfolio_path = self.tmp / STATE_ROOT / "portfolio_ledger.json"
        portfolio = json.loads(portfolio_path.read_text(encoding="utf-8"))
        portfolio["ledger_version"] = 0
        self.write_json(portfolio_path, portfolio)
        self.assert_fail(self.evidence(), "WORK_STATE_PORTFOLIO_LEDGER_ROLLBACK")

    def test_render_gate_defaults_to_not_executed_fail_closed(self):
        evidence = build_phase_a_consumer_evidence(
            manifest_path=self.manifest_path,
            previous_state_path=self.previous_state_path,
            root=self.tmp,
            expected_run_id="run-1",
            expected_commit_sha="b" * 40,
            expected_production_snapshot_id="a" * 64,
            expected_previous_state_id=self.previous_state["current_state_id"],
            expected_previous_state_hash=self.previous_state["current_state_hash"],
            now=self.now,
        )
        self.assertEqual(evidence["data_gate"]["status"], "PASS")
        self.assert_fail(evidence, "RENDER_GATE_NOT_EXECUTED")

    def test_missing_expected_bindings_fail_closed(self):
        cases = {
            "expected_run_id": "MISSING_EXPECTED_RUN_ID",
            "expected_commit_sha": "MISSING_EXPECTED_COMMIT_SHA",
            "expected_production_snapshot_id": "MISSING_EXPECTED_PRODUCTION_SNAPSHOT_ID",
            "expected_previous_state_id": "MISSING_EXPECTED_PREVIOUS_STATE_ID",
            "expected_previous_state_hash": "MISSING_EXPECTED_PREVIOUS_STATE_HASH",
        }
        for arg, reason in cases.items():
            with self.subTest(arg=arg):
                self.assert_fail(self.evidence(**{arg: None}), reason)

    def test_render_gate_fail_with_data_gate_pass_uses_no_fallback(self):
        evidence = self.evidence(render_preview_status="FAIL")
        self.assertEqual(evidence["data_gate"]["status"], "PASS")
        self.assertEqual(evidence["render_gate"]["status"], "FAIL_CLOSED")
        self.assertEqual(evidence["six_chart_preview_status"], "BLOCKED")
        self.assertFalse(evidence["fallback_used"])
        self.assertIn("RENDER_GATE_FAIL", evidence["fail_closed_reason"])


if __name__ == "__main__":
    unittest.main()
