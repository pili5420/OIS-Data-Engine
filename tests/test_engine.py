from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

import ois_update
from src.indicators.technical import build_indicators, macd, rsi, simple_moving_average
from src.payload.chart_payload import build_chart_payload, validate_chart_payload_schema
from src.rollover.normalization import check_rollover
from src.sources.adapters import FetchResult, PriceRow
from src.validation.quality import validate_rows


TMP_ROOT = Path(__file__).resolve().parents[1] / ".tmp-tests"


def temp_dir():
    TMP_ROOT.mkdir(exist_ok=True)
    return tempfile.TemporaryDirectory(dir=TMP_ROOT)


def rows(count: int = 260, source: str = "test") -> list[PriceRow]:
    output = []
    for idx in range(count):
        output.append(
            PriceRow(
                date=f"2025-{(idx // 22) % 12 + 1:02d}-{(idx % 22) + 1:02d}",
                open=70 + idx * 0.1,
                high=71 + idx * 0.1,
                low=69 + idx * 0.1,
                close=70 + idx * 0.1,
                volume=1000 + idx,
                source=source,
            )
        )
    return output


class EngineTests(unittest.TestCase):
    def test_duplicate_detection(self):
        sample = rows()
        sample[10] = PriceRow(**{**sample[10].__dict__, "date": sample[9].date})
        report = validate_rows(sample)
        self.assertEqual(report.validation_status, "FAIL")
        self.assertGreater(report.duplicate_dates, 0)

    def test_missing_close(self):
        sample = rows()
        bad = PriceRow(**{**sample[0].__dict__, "close": 0})
        sample[0] = bad
        report = validate_rows(sample)
        self.assertEqual(report.validation_status, "FAIL")

    def test_date_order(self):
        sample = rows()
        sample[1], sample[2] = sample[2], sample[1]
        report = validate_rows(sample)
        self.assertIn("dates are out of ascending chronological order", report.errors)

    def test_indicator_calculation(self):
        values = [float(i) for i in range(1, 131)]
        ma20 = simple_moving_average(values, 20)
        self.assertIsNone(ma20[18])
        self.assertEqual(ma20[19], 10.5)

    def test_macd(self):
        values = [float(i) for i in range(1, 80)]
        data = macd(values)
        self.assertEqual(set(data), {"dif", "dea", "histogram"})
        self.assertEqual(len(data["dif"]), len(values))

    def test_rsi(self):
        values = [44, 45, 46, 45, 47, 49, 48, 50, 52, 51, 53, 54, 55, 56, 57, 58]
        data = rsi(values, 14)
        self.assertIsNone(data[13])
        self.assertIsNotNone(data[14])
        self.assertGreaterEqual(data[14], 0)
        self.assertLessEqual(data[14], 100)

    def test_ma120(self):
        sample = [row.to_csv_dict() for row in rows(130)]
        indicators = build_indicators(sample)
        self.assertIsNone(indicators["data"][118]["ma120"])
        self.assertIsNotNone(indicators["data"][119]["ma120"])

    def test_rollover_detection(self):
        sample = rows()
        sample[20] = PriceRow(**{**sample[20].__dict__, "close": sample[19].close * 1.2})
        report = check_rollover(sample, "WTI")
        self.assertTrue(report.rollover_detected)
        self.assertEqual(report.validation_status, "PASS")

    def test_no_new_trading_day(self):
        with temp_dir() as tmp:
            old_production = ois_update.PRODUCTION_DIR
            old_staging = ois_update.STAGING_DIR
            old_archive = ois_update.ARCHIVE_DIR
            old_log = ois_update.LOG_DIR
            old_router = ois_update.SourceRouter
            try:
                root = Path(tmp)
                ois_update.PRODUCTION_DIR = root / "production"
                ois_update.STAGING_DIR = root / "staging"
                ois_update.ARCHIVE_DIR = root / "archive"
                ois_update.LOG_DIR = root / "logs"
                ois_update.ensure_dirs()
                data = [row.to_csv_dict() for row in rows()]
                ois_update.write_csv(ois_update.PRODUCTION_DIR / "ois_wti_clean.csv", data)
                ois_update.write_csv(ois_update.PRODUCTION_DIR / "ois_brent_clean.csv", data)

                class FakeRouter:
                    def fetch_history(self, ticker, min_rows=250, range_="2y"):
                        return FetchResult(ticker=ticker, source="test", fetched_at="2026-01-01T00:00:00Z", rows=rows())

                ois_update.SourceRouter = FakeRouter
                result = ois_update.run_update(initialize=False, force=False)
                self.assertEqual(result["overall_validation"], "NO_UPDATE")
            finally:
                ois_update.PRODUCTION_DIR = old_production
                ois_update.STAGING_DIR = old_staging
                ois_update.ARCHIVE_DIR = old_archive
                ois_update.LOG_DIR = old_log
                ois_update.SourceRouter = old_router

    def test_atomic_publish(self):
        with temp_dir() as tmp:
            old_production = ois_update.PRODUCTION_DIR
            old_staging = ois_update.STAGING_DIR
            try:
                root = Path(tmp)
                ois_update.PRODUCTION_DIR = root / "production"
                ois_update.STAGING_DIR = root / "staging"
                ois_update.ensure_dirs()
                ois_update.publish_atomic({"sample.json": {"ok": True}})
                self.assertTrue((ois_update.PRODUCTION_DIR / "sample.json").exists())
            finally:
                ois_update.PRODUCTION_DIR = old_production
                ois_update.STAGING_DIR = old_staging

    def test_failed_update_keeps_previous_pass(self):
        with temp_dir() as tmp:
            production = Path(tmp) / "production"
            production.mkdir()
            path = production / "ois_wti_clean.csv"
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=ois_update.CSV_FIELDS)
                writer.writeheader()
                writer.writerow(rows(1)[0].to_csv_dict())
            before = path.read_text(encoding="utf-8")
            bad_report = validate_rows([], min_rows=250)
            self.assertEqual(bad_report.validation_status, "FAIL")
            after = path.read_text(encoding="utf-8")
            self.assertEqual(before, after)

    def test_chart_payload_schema(self):
        clean = [row.to_csv_dict() for row in rows()]
        indicators = build_indicators(clean)
        validation = {
            "overall_validation": "PASS",
            "WTI": {"validation_status": "PASS"},
            "Brent": {"validation_status": "PASS"},
        }
        payload = build_chart_payload(clean, clean, indicators, indicators, validation)
        ok, errors = validate_chart_payload_schema(payload)
        self.assertTrue(ok, errors)
        self.assertEqual(len(payload["datasets"]), 6)


if __name__ == "__main__":
    unittest.main()
