from __future__ import annotations

import copy
import json
import unittest
from datetime import datetime, timezone
from pathlib import Path

from src.rate.contract import DATASETS
from src.rate.engine import RateIntegrityError, build_documents, validate_bundle, write_candidate

ROOT = Path(__file__).resolve().parents[1]


def bundle():
    datasets = {}
    for name, fields in DATASETS.items():
        rows = []
        count = 50 if name == "top50_universe" else 30 if name == "top30_universe" else 1
        for index in range(count):
            row = {field: f"{field}-{index}" for field in fields}
            for field in {"price", "volume", "close", "margin_balance", "short_balance", "market_cap", "value", "stage_score", "relative_strength", "flow_score", "return_1d", "volatility_20d", "concentration", "foreign_net", "trust_net", "dealer_net", "total_net", "margin_change", "short_change", "large_order_net", "turnover", "ma20", "ma60", "macd_dif", "macd_dea", "macd_histogram"} & fields:
                row[field] = float(index + 1)
            if "rank" in row: row["rank"] = index + 1
            if "evidence_refs" in row: row["evidence_refs"] = [f"source://{name}/{index}"]
            rows.append(row)
        datasets[name] = rows
    return {"metadata": {"source": "test-fixture", "source_timestamp": "2026-09-14T00:00:00Z", "data_as_of": "2026-09-14"}, "datasets": datasets}


class RateTests(unittest.TestCase):
    def test_valid_bundle_builds_fixed_machine_outputs(self):
        document = build_documents(bundle())
        self.assertEqual(set(document), {"rate_status.json", "rate_ingestion_validation.json", "rate_data_payload.json"})
        self.assertTrue(all(value["validation_status"] == "PASS" for value in document.values()))
        self.assertEqual(document["rate_data_payload.json"]["datasets"]["top50_universe"].__len__(), 50)

    def test_missing_duplicate_type_range_and_count_fail(self):
        mutations = [
            lambda b: b["datasets"]["market_structure"][0].pop("price"),
            lambda b: b["datasets"]["market_structure"].append(copy.deepcopy(b["datasets"]["market_structure"][0])),
            lambda b: b["datasets"]["market_structure"][0].update(price=float("nan")),
            lambda b: b["datasets"]["top50_universe"].pop(),
            lambda b: b["datasets"]["stage_inputs"][0].update(evidence_refs=[""]),
        ]
        for mutate in mutations:
            candidate = bundle(); mutate(candidate)
            with self.subTest(mutate=mutate), self.assertRaises(RateIntegrityError):
                validate_bundle(candidate)

    def test_stale_future_source_and_missing_dataset_fail(self):
        for timestamp in ("2020-01-01T00:00:00Z", "2099-01-01T00:00:00Z"):
            candidate = bundle(); candidate["metadata"]["source_timestamp"] = timestamp
            with self.assertRaises(RateIntegrityError): validate_bundle(candidate)
        candidate = bundle(); candidate["datasets"].pop("m7_inputs")
        with self.assertRaises(RateIntegrityError): validate_bundle(candidate)

    def test_candidate_isolated_and_reproducible(self):
        candidate = ROOT / "data" / "staging" / "rate-test-candidate"
        if candidate.exists():
            import shutil; shutil.rmtree(candidate)
        write_candidate(build_documents(bundle()), candidate)
        try:
            self.assertEqual(sorted(path.name for path in (candidate / "data/rate/production").iterdir()), ["rate_data_payload.json", "rate_ingestion_validation.json", "rate_status.json"])
            first = (candidate / "data/rate/production/rate_data_payload.json").read_bytes()
            second = json.dumps(build_documents(bundle())["rate_data_payload.json"], indent=2, ensure_ascii=False).encode() + b"\n"
            first_document = json.loads(first); second_document = json.loads(second)
            first_document.pop("generated_at"); second_document.pop("generated_at")
            self.assertEqual(first_document, second_document)
        finally:
            import shutil; shutil.rmtree(candidate, ignore_errors=True)

    def test_no_source_is_fail_closed(self):
        report = ROOT / "data" / "staging" / "rate-test-report.json"
        candidate = ROOT / "data" / "staging" / "rate-test-no-source"
        from src.rate.engine import main
        self.assertEqual(main(["--candidate", str(candidate), "--report", str(report)]), 1)
        self.assertEqual(json.loads(report.read_text())["validation_status"], "FAIL")
        report.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
