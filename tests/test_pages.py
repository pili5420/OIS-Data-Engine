from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.prepare_pages import PUBLIC_FILES, prepare_pages


ROOT = Path(__file__).resolve().parents[1]


class PagesPublicationTests(unittest.TestCase):
    def setUp(self):
        temporary_root = ROOT / ".tmp-tests"
        temporary_root.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=temporary_root)
        self.addCleanup(self.temporary.cleanup)
        self.source = Path(self.temporary.name) / "production"
        self.source.mkdir()
        self.destination = Path(self.temporary.name) / "pages"
        day = "2026-09-04"
        payload = {
            "schema_version": "OIS-CHART-1.0",
            "validation_status": "PASS",
            "latest_complete_source_date": {"wti": day, "brent": day},
        }
        validation = {
            "schema_version": "OIS-VALIDATION-1.0",
            "overall_validation": "PASS",
            "chart_payload_schema": "PASS",
        }
        status = {"schema_version": "OIS-STATUS-1.0", "status": "PASS", "validation": "PASS"}
        for name, key in (("WTI", "wti"), ("Brent", "brent")):
            payload[key] = {"rows": 504, "source_date": day, "validation": "PASS"}
            validation[name] = {"rows": 504, "source_date": day, "validation_status": "PASS"}
            status[f"{name}_source_date"] = day
        documents = {
            "ois_chart_payload.json": payload,
            "ois_ingestion_validation.json": validation,
            "ois_status.json": status,
        }
        for name, document in documents.items():
            (self.source / name).write_text(json.dumps(document, indent=2), encoding="utf-8")

    def change_document(self, filename, change):
        path = self.source / filename
        document = json.loads(path.read_bytes())
        change(document)
        path.write_text(json.dumps(document), encoding="utf-8")

    def test_copies_only_public_files_byte_for_byte(self):
        (self.source / "private.txt").write_text("not part of publication", encoding="utf-8")
        before = {name: (self.source / name).read_bytes() for name in PUBLIC_FILES}
        prepare_pages(self.source, self.destination)
        self.assertEqual(set(PUBLIC_FILES), {path.name for path in self.destination.iterdir()})
        for name, content in before.items():
            self.assertEqual(content, (self.destination / name).read_bytes())
            self.assertEqual(content, (self.source / name).read_bytes())

    def test_rejects_non_pass_documents_before_creating_artifact(self):
        for name, (_, status_key) in PUBLIC_FILES.items():
            original = (self.source / name).read_bytes()
            for result in ("FAIL", "NO_UPDATE"):
                with self.subTest(file=name, result=result):
                    self.change_document(name, lambda document: document.update({status_key: result}))
                    with self.assertRaises(ValueError):
                        prepare_pages(self.source, self.destination)
                    self.assertFalse(self.destination.exists())
            (self.source / name).write_bytes(original)

    def test_rejects_changed_schema(self):
        self.change_document("ois_chart_payload.json", lambda document: document.update(schema_version="OIS-CHART-2.0"))
        with self.assertRaises(ValueError):
            prepare_pages(self.source, self.destination)
        self.assertFalse(self.destination.exists())

    def test_rejects_inconsistent_source_date(self):
        self.change_document("ois_status.json", lambda document: document.update(WTI_source_date="2000-01-01"))
        with self.assertRaises(ValueError):
            prepare_pages(self.source, self.destination)
        self.assertFalse(self.destination.exists())

    def test_rejects_inconsistent_row_count(self):
        self.change_document("ois_chart_payload.json", lambda document: document["wti"].update(rows=1))
        with self.assertRaises(ValueError):
            prepare_pages(self.source, self.destination)
        self.assertFalse(self.destination.exists())

    def test_rejects_missing_or_malformed_json(self):
        path = self.source / "ois_status.json"
        path.write_text("invalid JSON", encoding="utf-8")
        with self.assertRaises(ValueError):
            prepare_pages(self.source, self.destination)
        path.unlink()
        with self.assertRaises(FileNotFoundError):
            prepare_pages(self.source, self.destination)
        self.assertFalse(self.destination.exists())


if __name__ == "__main__":
    unittest.main()
