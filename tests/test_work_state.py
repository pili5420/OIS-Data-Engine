
from __future__ import annotations

import copy
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from src.runtime.source import IntegrityError
from src.runtime.validation import PUBLIC_FILES, read_json
from src.runtime.engine import write_json
from src.work_state import (EXPECTED_BOOTSTRAP_SNAPSHOT_ID, EXPECTED_WFA_INFRA_EXECUTION_ID,
                            EXPECTED_WFA_INFRA_STATE_HASH, EXPECTED_WFA_INFRA_STATE_ID,
                            STATE_ROOT, STATE_TYPE_INCREMENTAL, STATE_TYPE_INITIAL,
                            bootstrap_initial_state, calculate_state_hash, load_current_state,
                            transition_work_state, validate_state_document, validate_state_file,
                            write_w2_evidence)

ROOT = Path(__file__).resolve().parents[1]


class WorkStateBootstrapTests(unittest.TestCase):
    def setUp(self):
        base = ROOT.parent / ".work-state-tests"
        base.mkdir(parents=True, exist_ok=True)
        self.tmp = Path(tempfile.mkdtemp(prefix="case-", dir=base))
        (self.tmp / "data/production").mkdir(parents=True)
        for name in PUBLIC_FILES:
            shutil.copy2(ROOT / "data/production" / name, self.tmp / "data/production" / name)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def doc(self, name):
        return read_json(self.tmp / "data/production" / name)

    def write_doc(self, name, doc):
        write_json(self.tmp / "data/production" / name, doc)

    def test_bootstrap_with_valid_production_snapshot_passes(self):
        result = bootstrap_initial_state(self.tmp, work_execution_id="wfa-infra-test-001")
        self.assertEqual(result["final_result"], "PASS")
        self.assertEqual(result["state_type"], STATE_TYPE_INITIAL)
        self.assertTrue(result["bootstrap"])
        self.assertIsNone(result["previous_state_id"])
        self.assertIsNone(result["previous_state_hash"])
        self.assertEqual(result["production_snapshot_id"], EXPECTED_BOOTSTRAP_SNAPSHOT_ID)
        self.assertEqual(result["portfolio_ledger_version"], 1)
        self.assertEqual(result["transaction_ledger_version"], 1)
        current = load_current_state(self.tmp)
        self.assertEqual(current["current_state_id"], result["current_state_id"])
        self.assertEqual(current["current_state_hash"], result["current_state_hash"])

    def test_previous_state_null_only_allowed_for_initial_state(self):
        result = bootstrap_initial_state(self.tmp, work_execution_id="wfa-infra-test-002")
        state = load_current_state(self.tmp)
        state["state_type"] = "NORMAL_WORK_STATE"
        with self.assertRaisesRegex(IntegrityError, "NULL_PREVIOUS_ONLY_INITIAL"):
            validate_state_document(state)

    def test_second_genesis_attempt_fails_but_same_execution_replay_is_idempotent(self):
        first = bootstrap_initial_state(self.tmp, work_execution_id="wfa-infra-test-003")
        replay = bootstrap_initial_state(self.tmp, work_execution_id="wfa-infra-test-003")
        self.assertEqual(replay["idempotency_status"], "IDEMPOTENT_REPLAY")
        self.assertEqual(replay["current_state_id"], first["current_state_id"])
        with self.assertRaisesRegex(IntegrityError, "GENESIS_ALREADY_EXISTS"):
            bootstrap_initial_state(self.tmp, work_execution_id="wfa-infra-test-003b")

    def test_corrupt_persisted_state_hash_fails(self):
        result = bootstrap_initial_state(self.tmp, work_execution_id="wfa-infra-test-004")
        path = self.tmp / STATE_ROOT / "history" / f"{result['current_state_id']}.json"
        state = read_json(path)
        state["decision_state"]["signals"].append({"symbol": "FAKE"})
        write_json(path, state)
        with self.assertRaisesRegex(IntegrityError, "HASH_MISMATCH|POINTER_HISTORY_MISMATCH"):
            load_current_state(self.tmp)

    def test_partial_state_commit_rolls_back(self):
        with self.assertRaisesRegex(IntegrityError, "INJECTED_PARTIAL_COMMIT_FAILURE"):
            bootstrap_initial_state(self.tmp, work_execution_id="wfa-infra-test-005", fail_after_stage="candidate")
        self.assertFalse((self.tmp / STATE_ROOT / "current_state.json").exists())
        self.assertFalse((self.tmp / STATE_ROOT / "history").exists())

    def test_invalid_production_snapshot_fails(self):
        status = self.doc("ois_status.json")
        status["production_snapshot_id"] = "bad"
        self.write_doc("ois_status.json", status)
        with self.assertRaisesRegex(IntegrityError, "UNEXPECTED_SNAPSHOT|CROSS_FILE_METADATA|SNAPSHOT_MISMATCH"):
            bootstrap_initial_state(self.tmp, work_execution_id="wfa-infra-test-006")

    def test_fallback_source_fails(self):
        for name in PUBLIC_FILES:
            doc = self.doc(name)
            doc["source"] = {"wti": "fixture", "brent": "fixture"}
            self.write_doc(name, doc)
        with self.assertRaisesRegex(IntegrityError, "UNAPPROVED_SOURCE"):
            bootstrap_initial_state(self.tmp, work_execution_id="wfa-infra-test-007")

    def test_ledger_versions_initialized_correctly(self):
        result = bootstrap_initial_state(self.tmp, work_execution_id="wfa-infra-test-008")
        portfolio = read_json(self.tmp / STATE_ROOT / "portfolio_ledger.json")
        transactions = read_json(self.tmp / STATE_ROOT / "transaction_ledger.json")
        self.assertEqual(portfolio["ledger_version"], result["portfolio_ledger_version"])
        self.assertEqual(transactions["ledger_version"], result["transaction_ledger_version"])
        self.assertTrue(portfolio["ledger_bootstrap"])
        self.assertTrue(transactions["ledger_bootstrap"])
        self.assertEqual(portfolio["events"], [])
        self.assertEqual(transactions["transactions"], [])

    def test_state_history_immutable(self):
        result = bootstrap_initial_state(self.tmp, work_execution_id="wfa-infra-test-009")
        history_path = self.tmp / STATE_ROOT / "history" / f"{result['current_state_id']}.json"
        before = history_path.read_bytes()
        with self.assertRaisesRegex(IntegrityError, "IDEMPOTENCY_POINTER_CONFLICT|HASH_MISMATCH|POINTER_HISTORY_MISMATCH"):
            # Mutating the current pointer must not allow replay to overwrite immutable history.
            current = read_json(self.tmp / STATE_ROOT / "current_state.json")
            current["current_state_hash"] = "0" * 64
            write_json(self.tmp / STATE_ROOT / "current_state.json", current)
            bootstrap_initial_state(self.tmp, work_execution_id="wfa-infra-test-009")
        self.assertEqual(history_path.read_bytes(), before)

    def test_current_pointer_and_execution_reference_persisted_state(self):
        result = bootstrap_initial_state(self.tmp, work_execution_id="wfa-infra-test-010")
        current = validate_state_file(self.tmp / STATE_ROOT / "current_state.json")
        history = validate_state_file(self.tmp / STATE_ROOT / "history" / f"{result['current_state_id']}.json")
        execution = read_json(self.tmp / STATE_ROOT / "executions" / "wfa-infra-test-010.json")
        self.assertEqual(current, history)
        self.assertEqual(execution["current_state_id"], current["current_state_id"])
        self.assertEqual(execution["current_state_hash"], current["current_state_hash"])
        self.assertEqual(calculate_state_hash(current), current["current_state_hash"])


class WorkStateContinuityTests(unittest.TestCase):
    def setUp(self):
        base = ROOT.parent / ".work-state-tests"
        base.mkdir(parents=True, exist_ok=True)
        self.tmp = Path(tempfile.mkdtemp(prefix="case-", dir=base))
        (self.tmp / "data/production").mkdir(parents=True)
        for name in PUBLIC_FILES:
            shutil.copy2(ROOT / "data/production" / name, self.tmp / "data/production" / name)
        shutil.copytree(ROOT / STATE_ROOT, self.tmp / STATE_ROOT)
        prior_path = self.tmp / STATE_ROOT / "history" / f"{EXPECTED_WFA_INFRA_STATE_ID}.json"
        shutil.copy2(prior_path, self.tmp / STATE_ROOT / "current_state.json")
        for ledger_name in ("portfolio_ledger.json", "transaction_ledger.json"):
            ledger_path = self.tmp / STATE_ROOT / ledger_name
            ledger = read_json(ledger_path)
            ledger["ledger_bootstrap"] = True
            ledger["current_state_id"] = EXPECTED_WFA_INFRA_STATE_ID
            ledger["current_state_hash"] = EXPECTED_WFA_INFRA_STATE_HASH
            ledger.pop("last_work_execution_id", None)
            write_json(ledger_path, ledger)
        self.prior = load_current_state(self.tmp)
        self.assertEqual(self.prior["current_state_id"], EXPECTED_WFA_INFRA_STATE_ID)
        self.assertEqual(self.prior["current_state_hash"], EXPECTED_WFA_INFRA_STATE_HASH)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_non_bootstrap_transition_links_to_persisted_prior_state(self):
        result = transition_work_state(self.tmp, work_execution_id="wfa-w2-test-001")
        self.assertEqual(result["final_result"], "PASS")
        self.assertEqual(result["execution_type"], "STATE_CONTINUITY_ACCEPTANCE")
        self.assertEqual(result["state_type"], STATE_TYPE_INCREMENTAL)
        self.assertFalse(result["bootstrap"])
        self.assertEqual(result["previous_state_id"], self.prior["current_state_id"])
        self.assertEqual(result["previous_state_hash"], self.prior["current_state_hash"])
        self.assertEqual(result["portfolio_ledger_version"], self.prior["portfolio_ledger_version"])
        self.assertEqual(result["transaction_ledger_version"], self.prior["transaction_ledger_version"])
        current = load_current_state(self.tmp)
        self.assertFalse(current["decision_state"]["state_reset_detected"])
        self.assertFalse(current["ledger_reset_detected"])
        self.assertEqual(current["decision_state"]["decision_update"], "NO_CHANGE")

    def test_transition_replay_is_idempotent(self):
        first = transition_work_state(self.tmp, work_execution_id="wfa-w2-test-002")
        replay = transition_work_state(self.tmp, work_execution_id="wfa-w2-test-002")
        self.assertEqual(replay["idempotency_status"], "IDEMPOTENT_REPLAY")
        self.assertEqual(replay["current_state_id"], first["current_state_id"])
        self.assertEqual(replay["current_state_hash"], first["current_state_hash"])
        executions = list((self.tmp / STATE_ROOT / "executions").glob("wfa-w2-test-002.json"))
        self.assertEqual(len(executions), 1)

    def test_w2_evidence_contains_required_acceptance_fields(self):
        result = transition_work_state(self.tmp, work_execution_id="wfa-w2-test-003")
        path = write_w2_evidence(self.tmp, result)
        evidence = read_json(path)
        self.assertEqual(evidence["wfa_id"], "WFA-001 OIS W2")
        self.assertEqual(evidence["prior_work_execution_id"], EXPECTED_WFA_INFRA_EXECUTION_ID)
        self.assertEqual(evidence["next_previous_state_id"], self.prior["current_state_id"])
        self.assertEqual(evidence["next_previous_state_hash"], self.prior["current_state_hash"])
        self.assertEqual(evidence["state_id_continuity"], "PASS")
        self.assertEqual(evidence["state_hash_continuity"], "PASS")
        self.assertEqual(evidence["portfolio_ledger_continuity"], "PASS")
        self.assertEqual(evidence["transaction_ledger_continuity"], "PASS")
        self.assertFalse(evidence["state_reset_detected"])
        self.assertFalse(evidence["ledger_reset_detected"])
        self.assertEqual(evidence["final_result"], "PASS")

    def test_transition_requires_persisted_prior_state(self):
        shutil.rmtree(self.tmp / STATE_ROOT)
        with self.assertRaisesRegex(IntegrityError, "PRIOR_MISSING"):
            transition_work_state(self.tmp, work_execution_id="wfa-w2-test-004")

    def test_transition_stops_on_corrupt_prior_hash(self):
        current_path = self.tmp / STATE_ROOT / "current_state.json"
        current = read_json(current_path)
        current["decision_state"]["signals"].append({"symbol": "BAD"})
        write_json(current_path, current)
        with self.assertRaisesRegex(IntegrityError, "HASH_MISMATCH|POINTER_HISTORY_MISMATCH"):
            transition_work_state(self.tmp, work_execution_id="wfa-w2-test-005")

    def test_transition_partial_commit_preserves_prior_pointer(self):
        before = (self.tmp / STATE_ROOT / "current_state.json").read_bytes()
        with self.assertRaisesRegex(IntegrityError, "INJECTED_PARTIAL_COMMIT_FAILURE"):
            transition_work_state(self.tmp, work_execution_id="wfa-w2-test-006", fail_after_stage="candidate")
        after = (self.tmp / STATE_ROOT / "current_state.json").read_bytes()
        self.assertEqual(after, before)
        self.assertEqual(load_current_state(self.tmp)["current_state_id"], self.prior["current_state_id"])


if __name__ == "__main__":
    unittest.main()
