
from __future__ import annotations

import copy
import json
import shutil
import os
import tempfile
import unittest
import uuid
from pathlib import Path

from src.runtime.source import IntegrityError
from src.runtime.validation import PUBLIC_FILES, read_json
from src.runtime.engine import write_json
from src.work_state import (EXPECTED_BOOTSTRAP_SNAPSHOT_ID, EXPECTED_WFA_INFRA_EXECUTION_ID,
                            EXPECTED_WFA_INFRA_STATE_HASH, EXPECTED_WFA_INFRA_STATE_ID,
                            STATE_ROOT, STATE_TYPE_INCREMENTAL, STATE_TYPE_INITIAL,
                            bootstrap_initial_state, calculate_state_hash, load_current_state,
                            render_gate_separation_evidence, run_failure_scenario,
                            run_four_cadence_acceptance, run_w4_failure_recovery_acceptance,
                            run_w5_3day_e2e_soak_acceptance, store_bytes_hashes,
                            transition_recovery_state, transition_work_cadence,
                            transition_work_state, validate_market_trading_date,
                            validate_state_document, validate_state_file,
                            write_w2_evidence, write_w3_evidence, state_id, SYSTEM, STATE_VERSION,
                            validate_authoritative_production_snapshot, load_work_ledgers,
                            build_incremental_state, atomic_commit_incremental_state,
                            EXECUTION_TYPE_RECOVERY, EXECUTION_TYPE_SOAK, WORK_CADENCE_ORDER,
                            ledger_source_for_state, validate_production_persistent_state_ssot,
                            write_production_persistent_state_ssot)
from src.execution_layer import (REQUIRED_EXECUTION_SYMBOLS, build_execution_data_layer,
                                 validate_execution_data_layer, write_execution_data_layer)

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_W2_STATE_ID = "ois-work-state-v1-5a8145e8-4f3ed4f408d1-1400758a-270-5b8f32d66add733bc3131199"
EXPECTED_W2_STATE_HASH = "5b8f32d66add733bc3131199b41a7aa1f4c35bf57d266e4970d2e867cb4f9962"
EXPECTED_W3_STATE_ID = "ois-work-state-v1-5a8145e8-4f3ed4f408d1-ec3109ab-cda-0b33394847edbab10cb9d707"
EXPECTED_W3_STATE_HASH = "0b33394847edbab10cb9d7072789072e5550ca8aad53f3d8776a58acf7ad5d5f"
EXPECTED_W4_STATE_ID = "ois-work-state-v1-5a8145e8-4f3ed4f408d1-w4-recovery-41c2bea2fad6cacc4c4e815d"
EXPECTED_W4_STATE_HASH = "41c2bea2fad6cacc4c4e815df066f54f4d8fdc3603139f591d5c7e707ef1a192"


class WorkStateBootstrapTests(unittest.TestCase):
    def setUp(self):
        base = Path(os.environ.get("TMP", tempfile.gettempdir())) / "ois-work-state-tests"
        base.mkdir(parents=True, exist_ok=True)
        self.tmp = base / f"case-{uuid.uuid4().hex}"
        self.tmp.mkdir(parents=True)
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
        base = Path(os.environ.get("TMP", tempfile.gettempdir())) / "ois-work-state-tests"
        base.mkdir(parents=True, exist_ok=True)
        self.tmp = base / f"case-{uuid.uuid4().hex}"
        self.tmp.mkdir(parents=True)
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


class WorkStateFourCadenceTests(unittest.TestCase):
    def setUp(self):
        base = Path(os.environ.get("TMP", tempfile.gettempdir())) / "ois-work-state-tests"
        base.mkdir(parents=True, exist_ok=True)
        self.tmp = base / f"case-{uuid.uuid4().hex}"
        self.tmp.mkdir(parents=True)
        (self.tmp / "data/production").mkdir(parents=True)
        for name in PUBLIC_FILES:
            shutil.copy2(ROOT / "data/production" / name, self.tmp / "data/production" / name)
        shutil.copytree(ROOT / STATE_ROOT, self.tmp / STATE_ROOT)
        for path in (self.tmp / STATE_ROOT / "executions").glob("*.json"):
            execution = read_json(path)
            if str(execution.get("idempotency_key", "")).startswith("WFA001-W3:"):
                path.unlink()
        for path in (self.tmp / STATE_ROOT / "history").glob("*.json"):
            state = read_json(path)
            if state.get("execution_type") == "FOUR_CADENCE_INCREMENTAL_ACCEPTANCE":
                path.unlink()
        shutil.copy2(self.tmp / STATE_ROOT / "history" / f"{EXPECTED_W2_STATE_ID}.json", self.tmp / STATE_ROOT / "current_state.json")
        for ledger_name in ("portfolio_ledger.json", "transaction_ledger.json"):
            ledger_path = self.tmp / STATE_ROOT / ledger_name
            ledger = read_json(ledger_path)
            ledger["ledger_version"] = 1
            ledger["ledger_bootstrap"] = False
            ledger["ledger_reset_detected"] = False
            ledger["current_state_id"] = EXPECTED_W2_STATE_ID
            ledger["current_state_hash"] = EXPECTED_W2_STATE_HASH
            ledger.pop("daily_chain_id", None)
            ledger.pop("trading_date", None)
            ledger.pop("last_work_execution_id", None)
            if ledger_name == "transaction_ledger.json":
                ledger["transactions"] = []
            write_json(ledger_path, ledger)
        self.prior = load_current_state(self.tmp)
        self.assertEqual(self.prior["current_state_id"], EXPECTED_W2_STATE_ID)
        self.assertEqual(self.prior["state_type"], STATE_TYPE_INCREMENTAL)
        self.assertFalse(self.prior["bootstrap"])

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def bump_snapshot(self, suffix):
        for name in PUBLIC_FILES:
            doc = read_json(self.tmp / "data/production" / name)
            new_snapshot = doc["production_snapshot_id"][:-8] + suffix
            doc["production_snapshot_id"] = new_snapshot
            doc["snapshot_id"] = new_snapshot
            doc["run_id"] = str(int(doc["run_id"]) + 1)
            doc["commit_sha"] = "f" * 40
            doc["lineage"]["production_snapshot_id"] = new_snapshot
            doc["lineage"]["run_id"] = doc["run_id"]
            doc["lineage"]["commit_sha"] = doc["commit_sha"]
            write_json(self.tmp / "data/production" / name, doc)

    def test_0735_to_0935_continuity_pass(self):
        first = transition_work_cadence(self.tmp, "OIS_0735_PREMARKET", work_execution_id="w3-0735-a")
        second = transition_work_cadence(self.tmp, "OIS_0935_OPENING", work_execution_id="w3-0935-a")
        self.assertEqual(second["previous_state_id"], first["current_state_id"])
        self.assertEqual(second["previous_state_hash"], first["current_state_hash"])

    def test_0935_to_1205_continuity_pass(self):
        transition_work_cadence(self.tmp, "OIS_0735_PREMARKET", work_execution_id="w3-0735-b")
        second = transition_work_cadence(self.tmp, "OIS_0935_OPENING", work_execution_id="w3-0935-b")
        third = transition_work_cadence(self.tmp, "OIS_1205_MIDDAY", work_execution_id="w3-1205-b")
        self.assertEqual(third["previous_state_id"], second["current_state_id"])
        self.assertEqual(third["previous_state_hash"], second["current_state_hash"])

    def test_1205_to_1935_continuity_pass(self):
        summaries = run_four_cadence_acceptance(self.tmp)
        self.assertEqual(summaries[3]["previous_state_id"], summaries[2]["current_state_id"])
        self.assertEqual(summaries[3]["previous_state_hash"], summaries[2]["current_state_hash"])

    def test_wrong_cadence_order_fails(self):
        with self.assertRaisesRegex(IntegrityError, "CADENCE_ORDER"):
            transition_work_cadence(self.tmp, "OIS_0935_OPENING", work_execution_id="w3-wrong-order")

    def test_skipped_cadence_fails(self):
        transition_work_cadence(self.tmp, "OIS_0735_PREMARKET", work_execution_id="w3-0735-skip")
        with self.assertRaisesRegex(IntegrityError, "CADENCE_ORDER"):
            transition_work_cadence(self.tmp, "OIS_1205_MIDDAY", work_execution_id="w3-1205-skip")

    def test_null_previous_after_bootstrap_fails(self):
        state = load_current_state(self.tmp)
        state["previous_state_id"] = None
        state["previous_state_hash"] = None
        state["bootstrap"] = False
        with self.assertRaisesRegex(IntegrityError, "NULL_PREVIOUS_ONLY_INITIAL"):
            validate_state_document(state)

    def test_ledger_version_regression_fails(self):
        ledger = read_json(self.tmp / STATE_ROOT / "portfolio_ledger.json")
        ledger["ledger_version"] = 0
        write_json(self.tmp / STATE_ROOT / "portfolio_ledger.json", ledger)
        with self.assertRaisesRegex(IntegrityError, "PORTFOLIO_LEDGER_ROLLBACK|PORTFOLIO_LEDGER_VERSION"):
            transition_work_cadence(self.tmp, "OIS_0735_PREMARKET", work_execution_id="w3-ledger-regression")

    def test_duplicate_cadence_replay_is_idempotent(self):
        first = transition_work_cadence(self.tmp, "OIS_0735_PREMARKET", work_execution_id="w3-0735-replay")
        replay = transition_work_cadence(self.tmp, "OIS_0735_PREMARKET")
        self.assertEqual(replay["idempotency_status"], "IDEMPOTENT_REPLAY")
        self.assertEqual(replay["current_state_id"], first["current_state_id"])

    def test_duplicate_cadence_replay_after_full_chain_does_not_move_pointer(self):
        summaries = run_four_cadence_acceptance(self.tmp)
        final_state = load_current_state(self.tmp)
        replay = transition_work_cadence(self.tmp, "OIS_0735_PREMARKET")
        self.assertEqual(replay["idempotency_status"], "IDEMPOTENT_REPLAY")
        self.assertEqual(replay["current_state_id"], summaries[0]["current_state_id"])
        self.assertEqual(load_current_state(self.tmp)["current_state_id"], final_state["current_state_id"])

    def test_same_snapshot_four_cadence_chain_passes(self):
        summaries = run_four_cadence_acceptance(self.tmp)
        path = write_w3_evidence(self.tmp, summaries)
        evidence = read_json(path)
        self.assertEqual(evidence["final_result"], "PASS")
        self.assertEqual(evidence["cadence_order"], "PASS")
        self.assertEqual(evidence["state_chain_continuity"], "PASS")
        self.assertEqual(len({item["production_snapshot_id"] for item in evidence["cadences"]}), 1)

    def test_newer_snapshot_midday_incremental_transition_passes(self):
        first = transition_work_cadence(self.tmp, "OIS_0735_PREMARKET", work_execution_id="w3-0735-new")
        second = transition_work_cadence(self.tmp, "OIS_0935_OPENING", work_execution_id="w3-0935-new")
        self.bump_snapshot("abcd1234")
        third = transition_work_cadence(self.tmp, "OIS_1205_MIDDAY", work_execution_id="w3-1205-new")
        self.assertEqual(third["previous_state_id"], second["current_state_id"])
        self.assertNotEqual(third["production_snapshot_id"], second["production_snapshot_id"])
        current = load_current_state(self.tmp)
        self.assertEqual(current["decision_state"]["decision_update"], "AUTHORITATIVE_PRODUCTION_SNAPSHOT_CHANGED")
        self.assertEqual(first["daily_chain_id"], third["daily_chain_id"])

    def test_state_reset_attempt_fails(self):
        state = load_current_state(self.tmp)
        state["decision_state"]["state_reset_detected"] = True
        state["current_state_hash"] = calculate_state_hash(state)
        state["current_state_id"] = state_id(system=SYSTEM, state_version=STATE_VERSION, production_snapshot_id=state["production_snapshot_id"], work_execution_id=state["work_execution_id"], state_hash=state["current_state_hash"])
        state["lineage"]["current_state_hash"] = state["current_state_hash"]
        state["lineage"]["current_state_id"] = state["current_state_id"]
        write_json(self.tmp / STATE_ROOT / "current_state.json", state)
        write_json(self.tmp / STATE_ROOT / "history" / f"{state['current_state_id']}.json", state)
        execution = read_json(self.tmp / STATE_ROOT / "executions" / f"{state['work_execution_id']}.json")
        execution["current_state_id"] = state["current_state_id"]
        execution["current_state_hash"] = state["current_state_hash"]
        write_json(self.tmp / STATE_ROOT / "executions" / f"{state['work_execution_id']}.json", execution)
        with self.assertRaisesRegex(IntegrityError, "PRIOR_STATE_RESET"):
            transition_work_cadence(self.tmp, "OIS_0735_PREMARKET", work_execution_id="w3-reset")

    def test_duplicate_transaction_append_fails(self):
        ledger = read_json(self.tmp / STATE_ROOT / "transaction_ledger.json")
        ledger["transactions"] = [{"transaction_id": "dup"}, {"transaction_id": "dup"}]
        write_json(self.tmp / STATE_ROOT / "transaction_ledger.json", ledger)
        with self.assertRaisesRegex(IntegrityError, "DUPLICATE_TRANSACTION"):
            transition_work_cadence(self.tmp, "OIS_0735_PREMARKET", work_execution_id="w3-dup-txn")


class WorkStateFailureRecoveryTests(unittest.TestCase):
    def setUp(self):
        base = Path(os.environ.get("TMP", tempfile.gettempdir())) / "ois-work-state-tests"
        base.mkdir(parents=True, exist_ok=True)
        self.tmp = base / f"case-{uuid.uuid4().hex}"
        self.tmp.mkdir(parents=True)
        (self.tmp / "data/production").mkdir(parents=True)
        for name in PUBLIC_FILES:
            shutil.copy2(ROOT / "data/production" / name, self.tmp / "data/production" / name)
        shutil.copytree(ROOT / STATE_ROOT, self.tmp / STATE_ROOT)
        for path in (self.tmp / STATE_ROOT / "executions").glob("*.json"):
            doc = read_json(path)
            if str(doc.get("work_execution_id", "")).startswith("w4") or doc.get("execution_type") in {"FAILURE_ATTEMPT", EXECUTION_TYPE_RECOVERY}:
                path.unlink()
        failure_dir = self.tmp / STATE_ROOT / "failures"
        if failure_dir.exists():
            shutil.rmtree(failure_dir)
        for path in (self.tmp / STATE_ROOT / "history").glob("*.json"):
            state = read_json(path)
            if state.get("execution_type") == EXECUTION_TYPE_RECOVERY:
                path.unlink()
        shutil.copy2(self.tmp / STATE_ROOT / "history" / f"{EXPECTED_W3_STATE_ID}.json", self.tmp / STATE_ROOT / "current_state.json")
        for ledger_name in ("portfolio_ledger.json", "transaction_ledger.json"):
            ledger_path = self.tmp / STATE_ROOT / ledger_name
            ledger = read_json(ledger_path)
            ledger["ledger_version"] = 1
            ledger["ledger_bootstrap"] = False
            ledger["ledger_reset_detected"] = False
            ledger["current_state_id"] = EXPECTED_W3_STATE_ID
            ledger["current_state_hash"] = EXPECTED_W3_STATE_HASH
            ledger["last_work_execution_id"] = "ec3109ab-cdaa-457e-88a5-039ce9fd2591"
            if ledger_name == "transaction_ledger.json":
                ledger["transactions"] = []
            write_json(ledger_path, ledger)
        self.baseline = load_current_state(self.tmp)
        self.assertEqual(self.baseline["current_state_id"], EXPECTED_W3_STATE_ID)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def assert_failure_preserves_store(self, scenario):
        before = store_bytes_hashes(self.tmp)
        failure = run_failure_scenario(self.tmp, scenario, failure_execution_id=f"test-{scenario}")
        after = store_bytes_hashes(self.tmp)
        self.assertEqual(after, before)
        self.assertFalse(failure["accepted_state_created"])
        self.assertEqual(failure["rollback_status"], "PASS")
        self.assertFalse(failure["state_mutated_on_failure"])
        self.assertFalse(failure["portfolio_ledger_mutated_on_failure"])
        self.assertFalse(failure["transaction_ledger_mutated_on_failure"])
        self.assertEqual(load_current_state(self.tmp)["current_state_id"], self.baseline["current_state_id"])
        return failure

    def test_production_validation_fail_current_state_unchanged(self):
        self.assert_failure_preserves_store("PRODUCTION_DATA_VALIDATION_FAIL")

    def test_production_validation_fail_ledgers_unchanged(self):
        failure = self.assert_failure_preserves_store("PRODUCTION_DATA_VALIDATION_FAIL")
        self.assertEqual(failure["portfolio_ledger_hash_before"], failure["portfolio_ledger_hash_after"])
        self.assertEqual(failure["transaction_ledger_hash_before"], failure["transaction_ledger_hash_after"])

    def test_candidate_validation_fail_no_accepted_state(self):
        failure = self.assert_failure_preserves_store("STATE_CANDIDATE_VALIDATION_FAIL")
        self.assertFalse(failure["accepted_state_created"])

    def test_wrong_previous_state_id_hash_fails(self):
        failure = self.assert_failure_preserves_store("STATE_CANDIDATE_VALIDATION_FAIL")
        self.assertRegex(failure["error_code"], "WORK_STATE_LINEAGE|WORK_STATE_HASH_MISMATCH")

    def test_atomic_commit_partial_failure_rolls_back(self):
        failure = self.assert_failure_preserves_store("ATOMIC_COMMIT_FAIL")
        self.assertEqual(failure["atomic_rollback_status"], "PASS")

    def test_current_pointer_update_failure_rolls_back(self):
        before = store_bytes_hashes(self.tmp)
        production = validate_authoritative_production_snapshot(self.tmp)
        portfolio, transactions = load_work_ledgers(self.tmp)
        bundle = build_incremental_state(work_execution_id="w4-pointer-fail", prior=self.baseline, production=production, portfolio=portfolio, transactions=transactions, created_at="2026-09-29T00:00:00Z")
        with self.assertRaisesRegex(IntegrityError, "CURRENT_POINTER_FAILURE"):
            atomic_commit_incremental_state(self.tmp, bundle, fail_after_stage="current_pointer", execution_type=EXECUTION_TYPE_RECOVERY)
        self.assertEqual(store_bytes_hashes(self.tmp), before)
        self.assertEqual(load_current_state(self.tmp)["current_state_id"], self.baseline["current_state_id"])

    def test_portfolio_ledger_mutation_failure_rolls_back(self):
        ledger = read_json(self.tmp / STATE_ROOT / "portfolio_ledger.json")
        ledger["ledger_version"] = 0
        write_json(self.tmp / STATE_ROOT / "portfolio_ledger.json", ledger)
        with self.assertRaisesRegex(IntegrityError, "PORTFOLIO_LEDGER"):
            transition_recovery_state(self.tmp, work_execution_id="w4-bad-portfolio")

    def test_transaction_ledger_mutation_failure_rolls_back(self):
        self.assert_failure_preserves_store("LEDGER_MUTATION_FAIL")

    def test_duplicate_transaction_fails(self):
        failure = self.assert_failure_preserves_store("LEDGER_MUTATION_FAIL")
        self.assertIn("DUPLICATE_TRANSACTION", failure["error_code"])

    def test_data_gate_fail_render_gate_separate(self):
        failure = self.assert_failure_preserves_store("PRODUCTION_DATA_VALIDATION_FAIL")
        gate = render_gate_separation_evidence(self.tmp)
        self.assertEqual(gate["data_gate_fail_case"]["data_gate_status"], "FAIL")
        self.assertEqual(gate["data_gate_fail_case"]["render_gate_status"], "NOT_RUN")
        self.assertFalse(failure["accepted_state_created"])

    def test_data_pass_render_fail_separation(self):
        gate = render_gate_separation_evidence(self.tmp)
        self.assertEqual(gate["render_fail_case"]["data_gate_status"], "PASS")
        self.assertEqual(gate["render_fail_case"]["render_gate_status"], "FAIL")
        self.assertFalse(gate["render_fail_case"]["static_fallback_used"])

    def test_recovery_resumes_baseline_state(self):
        self.assert_failure_preserves_store("ATOMIC_COMMIT_FAIL")
        recovery = transition_recovery_state(self.tmp, work_execution_id="w4-recovery-test")
        self.assertEqual(recovery["previous_state_id"], self.baseline["current_state_id"])
        self.assertEqual(recovery["previous_state_hash"], self.baseline["current_state_hash"])

    def test_recovery_ledger_continuity_pass(self):
        recovery = transition_recovery_state(self.tmp, work_execution_id="w4-recovery-ledger")
        self.assertGreaterEqual(recovery["portfolio_ledger_version"], self.baseline["portfolio_ledger_version"])
        self.assertGreaterEqual(recovery["transaction_ledger_version"], self.baseline["transaction_ledger_version"])

    def test_failed_candidate_never_becomes_prior_state(self):
        self.assert_failure_preserves_store("STATE_CANDIDATE_VALIDATION_FAIL")
        recovery = transition_recovery_state(self.tmp, work_execution_id="w4-recovery-prior")
        self.assertEqual(recovery["previous_state_id"], self.baseline["current_state_id"])

    def test_failure_retry_idempotent(self):
        first = run_failure_scenario(self.tmp, "PRODUCTION_DATA_VALIDATION_FAIL", failure_execution_id="w4-failure-retry")
        replay = run_failure_scenario(self.tmp, "PRODUCTION_DATA_VALIDATION_FAIL", failure_execution_id="w4-failure-retry")
        self.assertEqual(replay["idempotency_status"], "IDEMPOTENT_REPLAY")
        self.assertEqual(replay["failure_execution_id"], first["failure_execution_id"])

    def test_recovery_replay_idempotent(self):
        first = transition_recovery_state(self.tmp, work_execution_id="w4-recovery-replay")
        replay = transition_recovery_state(self.tmp, work_execution_id="w4-recovery-replay")
        self.assertEqual(replay["idempotency_status"], "IDEMPOTENT_REPLAY")
        self.assertEqual(replay["current_state_id"], first["current_state_id"])

    def test_w4_full_acceptance_passes(self):
        evidence = run_w4_failure_recovery_acceptance(self.tmp)
        self.assertEqual(evidence["final_result"], "PASS")
        self.assertEqual(evidence["data_gate_status"], "PASS")
        self.assertEqual(evidence["render_gate_separation_status"], "PASS")
        self.assertEqual(evidence["recovery"]["recovery_lineage_status"], "PASS")


class WorkStateThreeDaySoakTests(unittest.TestCase):
    def setUp(self):
        base = Path(os.environ.get("TMP", tempfile.gettempdir())) / "ois-work-state-tests"
        base.mkdir(parents=True, exist_ok=True)
        self.tmp = base / f"case-{uuid.uuid4().hex}"
        self.tmp.mkdir(parents=True)
        (self.tmp / "data/production").mkdir(parents=True)
        (self.tmp / "data/acceptance").mkdir(parents=True)
        for name in PUBLIC_FILES:
            shutil.copy2(ROOT / "data/production" / name, self.tmp / "data/production" / name)
        shutil.copytree(ROOT / STATE_ROOT, self.tmp / STATE_ROOT)
        shutil.copy2(ROOT / "data/acceptance/WFA001_OIS_W4_FAILURE_RECOVERY_EVIDENCE.json", self.tmp / "data/acceptance/WFA001_OIS_W4_FAILURE_RECOVERY_EVIDENCE.json")
        for path in (self.tmp / STATE_ROOT / "executions").glob("*.json"):
            execution = read_json(path)
            if execution.get("execution_type") == EXECUTION_TYPE_SOAK:
                path.unlink()
        for path in (self.tmp / STATE_ROOT / "history").glob("*.json"):
            state = read_json(path)
            if state.get("execution_type") == EXECUTION_TYPE_SOAK:
                path.unlink()
        shutil.copy2(self.tmp / STATE_ROOT / "history" / f"{EXPECTED_W4_STATE_ID}.json", self.tmp / STATE_ROOT / "current_state.json")
        for ledger_name in ("portfolio_ledger.json", "transaction_ledger.json"):
            ledger_path = self.tmp / STATE_ROOT / ledger_name
            ledger = read_json(ledger_path)
            ledger["ledger_version"] = 1
            ledger["ledger_bootstrap"] = False
            ledger["ledger_reset_detected"] = False
            ledger["current_state_id"] = EXPECTED_W4_STATE_ID
            ledger["current_state_hash"] = EXPECTED_W4_STATE_HASH
            ledger["last_work_execution_id"] = "w4-recovery"
            if ledger_name == "transaction_ledger.json":
                ledger["transactions"] = []
            write_json(ledger_path, ledger)
        self.baseline = load_current_state(self.tmp)
        self.assertEqual(self.baseline["current_state_id"], EXPECTED_W4_STATE_ID)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def set_production_day(self, trading_date, suffix):
        for name in PUBLIC_FILES:
            doc = read_json(self.tmp / "data/production" / name)
            snapshot = (doc["production_snapshot_id"][:-8] + suffix)[:64]
            doc["production_snapshot_id"] = snapshot
            doc["snapshot_id"] = snapshot
            doc["run_id"] = f"w5-run-{trading_date.replace('-', '')}"
            doc["commit_sha"] = (suffix * 10)[:40]
            doc["source_as_of"] = trading_date
            doc["source_timestamp"] = trading_date + "T23:59:59Z"
            doc["lineage"]["production_snapshot_id"] = snapshot
            doc["lineage"]["run_id"] = doc["run_id"]
            doc["lineage"]["commit_sha"] = doc["commit_sha"]
            doc["lineage"]["source_as_of"] = trading_date
            write_json(self.tmp / "data/production" / name, doc)

    def test_first_real_trading_day_records_in_progress(self):
        evidence = run_w5_3day_e2e_soak_acceptance(self.tmp)
        self.assertEqual(evidence["final_result"], "IN_PROGRESS")
        self.assertEqual(evidence["accepted_trading_days"], 1)
        self.assertEqual(evidence["accepted_executions"], 4)
        self.assertEqual(evidence["starting_baseline"]["starting_state_id"], EXPECTED_W4_STATE_ID)
        self.assertTrue((self.tmp / "data/acceptance/w5/OIS_W5_2026-09-25.json").exists())

    def test_three_real_trading_days_pass_with_cross_day_continuity(self):
        self.set_production_day("2026-09-25", "11111111")
        first = run_w5_3day_e2e_soak_acceptance(self.tmp)
        self.assertEqual(first["accepted_trading_days"], 1)
        self.set_production_day("2026-09-28", "22222222")
        second = run_w5_3day_e2e_soak_acceptance(self.tmp)
        self.assertEqual(second["accepted_trading_days"], 2)
        self.set_production_day("2026-09-29", "33333333")
        final = run_w5_3day_e2e_soak_acceptance(self.tmp)
        self.assertEqual(final["final_result"], "PASS")
        self.assertEqual(final["accepted_trading_days"], 3)
        self.assertEqual(final["accepted_executions"], 12)
        self.assertEqual(final["cross_day_continuity"], "PASS")
        self.assertFalse(final["state_reset_detected"])
        self.assertFalse(final["ledger_reset_detected"])
        self.assertFalse(final["fallback_detected"])
        day1 = read_json(self.tmp / "data/acceptance/w5/OIS_W5_2026-09-25.json")
        day2 = read_json(self.tmp / "data/acceptance/w5/OIS_W5_2026-09-28.json")
        day3 = read_json(self.tmp / "data/acceptance/w5/OIS_W5_2026-09-29.json")
        self.assertEqual(day2["cadences"][0]["previous_state_id"], day1["cadences"][-1]["current_state_id"])
        self.assertEqual(day3["cadences"][0]["previous_state_hash"], day2["cadences"][-1]["current_state_hash"])

    def test_non_trading_day_rejected(self):
        with self.assertRaisesRegex(IntegrityError, "NON_TRADING_DATE"):
            validate_market_trading_date("2025-12-25")
        self.set_production_day("2025-12-25", "44444444")
        with self.assertRaisesRegex(IntegrityError, "NON_TRADING_DATE"):
            run_w5_3day_e2e_soak_acceptance(self.tmp)

    def test_replay_current_w5_day_is_idempotent(self):
        first = run_w5_3day_e2e_soak_acceptance(self.tmp)
        current = load_current_state(self.tmp)
        replay = run_w5_3day_e2e_soak_acceptance(self.tmp)
        self.assertEqual(replay["accepted_executions"], first["accepted_executions"])
        self.assertEqual(load_current_state(self.tmp)["current_state_id"], current["current_state_id"])

    def test_w5_executions_use_separate_idempotency_namespace_from_w3(self):
        evidence = run_w5_3day_e2e_soak_acceptance(self.tmp)
        self.assertEqual(evidence["accepted_executions"], 4)
        for cadence in WORK_CADENCE_ORDER:
            path = self.tmp / STATE_ROOT / "executions" / f"w5-2026-09-25-{cadence.split('_')[1][:4]}.json"
            self.assertTrue(path.exists())
            execution = read_json(path)
            self.assertTrue(execution["idempotency_key"].startswith("WFA001-W5:"))
            self.assertEqual(execution["execution_type"], EXECUTION_TYPE_SOAK)



class ProductionPersistentStateSsotTests(unittest.TestCase):
    def setUp(self):
        base = Path(os.environ.get("TMP", tempfile.gettempdir())) / "ois-work-state-tests"
        base.mkdir(parents=True, exist_ok=True)
        self.tmp = base / f"case-{uuid.uuid4().hex}"
        self.tmp.mkdir(parents=True)
        (self.tmp / "data/production").mkdir(parents=True)
        for name in PUBLIC_FILES:
            shutil.copy2(ROOT / "data/production" / name, self.tmp / "data/production" / name)
        shutil.copytree(ROOT / STATE_ROOT, self.tmp / STATE_ROOT)
        self.state = load_current_state(self.tmp)
        source = ledger_source_for_state(self.state)
        for ledger_name in ("portfolio_ledger.json", "transaction_ledger.json"):
            ledger_path = self.tmp / STATE_ROOT / ledger_name
            ledger = read_json(ledger_path)
            ledger["current_state_id"] = self.state["current_state_id"]
            ledger["current_state_hash"] = self.state["current_state_hash"]
            ledger["last_work_execution_id"] = self.state["work_execution_id"]
            ledger["source"] = source
            write_json(ledger_path, ledger)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_persistent_state_ssot_binds_current_state_and_ledgers(self):
        manifest = validate_production_persistent_state_ssot(self.tmp, self.state)
        self.assertEqual(manifest["validation_status"], "PASS")
        self.assertEqual(manifest["current_state"]["state_id"], self.state["current_state_id"])
        self.assertEqual(manifest["portfolio_ledger"]["current_state_hash"], self.state["current_state_hash"])
        self.assertEqual(manifest["transaction_ledger"]["source"], ledger_source_for_state(self.state))
        self.assertFalse(manifest["strategy_modified"])
        self.assertFalse(manifest["rolling_180_modified"])
        self.assertFalse(manifest["six_chart_renderer_modified"])

    def test_stale_ledger_source_fails_persistent_state_ssot(self):
        ledger_path = self.tmp / STATE_ROOT / "portfolio_ledger.json"
        ledger = read_json(ledger_path)
        ledger["source"]["source_run_id"] = "stale-run"
        write_json(ledger_path, ledger)
        with self.assertRaisesRegex(IntegrityError, "SSOT_PORTFOLIO_LEDGER_SOURCE"):
            validate_production_persistent_state_ssot(self.tmp, self.state)

    def test_write_persistent_state_ssot_manifest(self):
        path = write_production_persistent_state_ssot(self.tmp)
        manifest = read_json(path)
        self.assertEqual(manifest["state_ledger_binding"], "PASS")
        self.assertEqual(manifest["transaction_ledger"]["current_state_id"], self.state["current_state_id"])

    def test_incremental_transition_updates_ledger_source_to_new_state_source(self):
        production = validate_authoritative_production_snapshot(self.tmp)
        production = dict(production)
        production["source_run_id"] = "new-authoritative-run"
        production["source_commit_sha"] = "a" * 40
        production["production_lineage"] = dict(production["production_lineage"])
        production["production_lineage"].update({"run_id": production["source_run_id"], "commit_sha": production["source_commit_sha"]})
        portfolio, transactions = load_work_ledgers(self.tmp)
        bundle = build_incremental_state(work_execution_id="ssot-transition-source", prior=self.state, production=production, portfolio=portfolio, transactions=transactions, created_at="2026-09-29T00:00:00Z")
        expected = ledger_source_for_state(bundle["state"])
        self.assertEqual(bundle["portfolio_ledger"]["source"], expected)
        self.assertEqual(bundle["transaction_ledger"]["source"], expected)


class ExecutionDataLayerTests(unittest.TestCase):
    def setUp(self):
        base = Path(os.environ.get("TMP", tempfile.gettempdir())) / "ois-work-state-tests"
        base.mkdir(parents=True, exist_ok=True)
        self.tmp = base / f"case-{uuid.uuid4().hex}"
        self.tmp.mkdir(parents=True)
        (self.tmp / "data/production").mkdir(parents=True)
        for name in PUBLIC_FILES:
            shutil.copy2(ROOT / "data/production" / name, self.tmp / "data/production" / name)
        shutil.copytree(ROOT / STATE_ROOT, self.tmp / STATE_ROOT)
        state = load_current_state(self.tmp)
        source = ledger_source_for_state(state)
        for ledger_name in ("portfolio_ledger.json", "transaction_ledger.json"):
            ledger_path = self.tmp / STATE_ROOT / ledger_name
            ledger = read_json(ledger_path)
            ledger["current_state_id"] = state["current_state_id"]
            ledger["current_state_hash"] = state["current_state_hash"]
            ledger["last_work_execution_id"] = state["work_execution_id"]
            ledger["source"] = source
            write_json(ledger_path, ledger)
        write_production_persistent_state_ssot(self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def approved_quote(self, symbol, *, trade_date="2026-09-25", market_timestamp="2026-09-25T09:30:00+08:00", last_price=10.5, source_id="TWSE_INTRADAY_EXECUTION_PRICE", tradable=True):
        return {
            "symbol": symbol,
            "trade_date": trade_date,
            "market_timestamp": market_timestamp,
            "last_price": last_price,
            "tradable": tradable,
            "source_metadata": {
                "source_id": source_id,
                "source_name": "Approved Taiwan intraday execution price feed",
                "source_url": "https://approved.example.invalid/twse/intraday",
                "retrieval_timestamp": market_timestamp,
                "approved": True,
            },
        }

    def quote_set(self, **overrides):
        return {symbol: self.approved_quote(symbol, **overrides) for symbol in REQUIRED_EXECUTION_SYMBOLS}

    def test_execution_layer_requires_three_target_symbols_and_fails_closed_without_source(self):
        doc = build_execution_data_layer(self.tmp, decision_cadence="OIS_0935_OPENING", decision_trade_date="2026-09-25")
        validate_execution_data_layer(doc)
        self.assertEqual(tuple(doc["required_symbols"]), REQUIRED_EXECUTION_SYMBOLS)
        self.assertEqual([item["symbol"] for item in doc["instruments"]], list(REQUIRED_EXECUTION_SYMBOLS))
        self.assertEqual(doc["validation_status"], "BLOCKED")
        self.assertFalse(doc["publishable"])
        self.assertEqual(doc["fail_closed_reason"], "NO_SYMBOL_PASSED_EXECUTION_PRICE_GATE")
        self.assertTrue(all(item["fallback_used"] is False for item in doc["instruments"]))
        self.assertTrue(all("trade_date" in item and "market_timestamp" in item and "last_price" in item for item in doc["instruments"]))

    def test_fresh_valid_quote_passes_execution_price_gate(self):
        doc = build_execution_data_layer(self.tmp, self.quote_set(), decision_cadence="OIS_0935_OPENING", decision_trade_date="2026-09-25")
        validate_execution_data_layer(doc)
        self.assertEqual(doc["validation_status"], "PASS")
        self.assertTrue(doc["publishable"])
        self.assertEqual(doc["pass_count"], 3)
        for item in doc["instruments"]:
            self.assertEqual(item["validation_status"], "PASS")
            self.assertEqual(item["trade_date"], "2026-09-25")
            self.assertEqual(item["decision_cadence"], "OIS_0935_OPENING")
            self.assertEqual(item["freshness_seconds"], 300)
            self.assertGreater(item["last_price"], 0)
            self.assertTrue(item["tradable"])
            self.assertEqual(item["approved_source_metadata"]["source_id"], "TWSE_INTRADAY_EXECUTION_PRICE")
            self.assertEqual(item["source_binding"]["technical_source_as_of"], "2026-09-25")
        self.assertFalse(doc["technical_source_as_of_binding_required"])

    def test_stale_day_quote_fails_closed(self):
        doc = build_execution_data_layer(self.tmp, self.quote_set(trade_date="2026-09-24", market_timestamp="2026-09-24T09:34:00+08:00"), decision_cadence="OIS_0935_OPENING", decision_trade_date="2026-09-25")
        validate_execution_data_layer(doc)
        self.assertEqual(doc["validation_status"], "BLOCKED")
        self.assertEqual(doc["pass_count"], 0)
        self.assertTrue(all(item["execution_gate"] == "STALE_DAY" for item in doc["instruments"]))
        self.assertTrue(all(item["fail_closed_reason"] == "TRADE_DATE_MISMATCH" for item in doc["instruments"]))

    def test_future_timestamp_quote_fails_closed(self):
        doc = build_execution_data_layer(self.tmp, self.quote_set(market_timestamp="2026-09-25T09:36:00+08:00"), decision_cadence="OIS_0935_OPENING", decision_trade_date="2026-09-25")
        validate_execution_data_layer(doc)
        self.assertEqual(doc["validation_status"], "BLOCKED")
        self.assertTrue(all(item["execution_gate"] == "FUTURE_TIMESTAMP" for item in doc["instruments"]))
        self.assertTrue(all(item["freshness_seconds"] == -60 for item in doc["instruments"]))

    def test_stale_quote_fails_closed(self):
        doc = build_execution_data_layer(self.tmp, self.quote_set(market_timestamp="2026-09-25T09:00:00+08:00"), decision_cadence="OIS_0935_OPENING", decision_trade_date="2026-09-25")
        validate_execution_data_layer(doc)
        self.assertEqual(doc["validation_status"], "BLOCKED")
        self.assertTrue(all(item["execution_gate"] == "STALE_QUOTE" for item in doc["instruments"]))
        self.assertTrue(all(item["freshness_seconds"] == 2100 for item in doc["instruments"]))

    def test_missing_price_fails_closed(self):
        doc = build_execution_data_layer(self.tmp, self.quote_set(last_price=None), decision_cadence="OIS_0935_OPENING", decision_trade_date="2026-09-25")
        validate_execution_data_layer(doc)
        self.assertEqual(doc["validation_status"], "BLOCKED")
        self.assertTrue(all(item["execution_gate"] == "PRICE_MISSING" for item in doc["instruments"]))

    def test_unapproved_source_fails_closed(self):
        doc = build_execution_data_layer(self.tmp, self.quote_set(source_id="UNAPPROVED_VENDOR"), decision_cadence="OIS_0935_OPENING", decision_trade_date="2026-09-25")
        validate_execution_data_layer(doc)
        self.assertEqual(doc["validation_status"], "BLOCKED")
        self.assertTrue(all(item["execution_gate"] == "UNAPPROVED_SOURCE" for item in doc["instruments"]))

    def test_non_tradable_quote_fails_closed(self):
        doc = build_execution_data_layer(self.tmp, self.quote_set(tradable=False), decision_cadence="OIS_0935_OPENING", decision_trade_date="2026-09-25")
        validate_execution_data_layer(doc)
        self.assertEqual(doc["validation_status"], "BLOCKED")
        self.assertFalse(doc["publishable"])
        self.assertEqual(doc["pass_count"], 0)
        self.assertTrue(all(item["validation_status"] == "BLOCKED" for item in doc["instruments"]))
        self.assertTrue(all(item["execution_gate"] == "NOT_TRADABLE" for item in doc["instruments"]))
        self.assertTrue(all(item["fail_closed_reason"] == "INSTRUMENT_NOT_TRADABLE" for item in doc["instruments"]))
        self.assertTrue(all(item["tradable"] is False for item in doc["instruments"]))

    def test_1205_midday_uses_own_decision_time(self):
        doc = build_execution_data_layer(self.tmp, self.quote_set(market_timestamp="2026-09-25T12:00:00+08:00"), decision_cadence="OIS_1205_MIDDAY", decision_trade_date="2026-09-25")
        validate_execution_data_layer(doc)
        self.assertEqual(doc["validation_status"], "PASS")
        self.assertEqual(doc["decision_cadence"], "OIS_1205_MIDDAY")
        self.assertTrue(all(item["freshness_seconds"] == 300 for item in doc["instruments"]))

    def test_partial_layer_allows_individual_symbol_pass(self):
        market = {"00642U": self.approved_quote("00642U")}
        doc = build_execution_data_layer(self.tmp, market, decision_cadence="OIS_0935_OPENING", decision_trade_date="2026-09-25")
        validate_execution_data_layer(doc)
        self.assertEqual(doc["validation_status"], "PARTIAL")
        self.assertTrue(doc["publishable"])
        self.assertEqual(doc["pass_count"], 1)
        self.assertEqual(doc["instruments"][0]["validation_status"], "PASS")
        self.assertEqual(doc["instruments"][1]["validation_status"], "BLOCKED")
        self.assertEqual(doc["instruments"][2]["validation_status"], "BLOCKED")

    def test_trade_date_is_not_bound_to_production_technical_source_as_of(self):
        state = load_current_state(self.tmp)
        self.assertEqual(state["source_as_of"], "2026-09-25")
        doc = build_execution_data_layer(self.tmp, self.quote_set(trade_date="2026-09-29", market_timestamp="2026-09-29T09:30:00+08:00"), decision_cadence="OIS_0935_OPENING", decision_trade_date="2026-09-29")
        validate_execution_data_layer(doc)
        self.assertEqual(doc["validation_status"], "PASS")
        self.assertEqual(doc["decision_trade_date"], "2026-09-29")
        self.assertEqual(doc["instruments"][0]["source_binding"]["technical_source_as_of"], "2026-09-25")
        self.assertFalse(doc["technical_source_as_of_binding_required"])

    def test_write_execution_data_layer_preserves_fail_closed_status(self):
        path = write_execution_data_layer(self.tmp)
        doc = read_json(path)
        self.assertEqual(doc["validation_status"], "BLOCKED")
        self.assertEqual(doc["state_ledger_ssot_binding"], "PASS")
        self.assertEqual([item["symbol"] for item in doc["instruments"]], list(REQUIRED_EXECUTION_SYMBOLS))



if __name__ == "__main__":
    unittest.main()
