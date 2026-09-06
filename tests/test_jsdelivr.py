from __future__ import annotations

import json
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import distribute_jsdelivr as cdn


COMMIT = "a" * 40
REPOSITORY = "pili5420/OIS-Data-Engine"
URL = f"https://cdn.jsdelivr.net/gh/{REPOSITORY}@main/data/production/ois_chart_payload.json"
PURGE_PATH = "/gh/pili5420/OIS-Data-Engine@main/data/production/ois_chart_payload.json"


def purge_result():
    return {"id": "request-1", "status": "finished", "paths": {
        PURGE_PATH: {"throttled": False, "providers": {"CF": True, "FY": True}},
    }}


class JsDelivrTests(unittest.TestCase):
    def test_generates_six_endpoints_bound_to_full_production_commit(self):
        report = cdn.make_report(REPOSITORY, COMMIT)
        self.assertEqual(6, len(report["endpoints"]))
        self.assertEqual(COMMIT, report["production_commit"])
        for entry in report["files"].values():
            self.assertIn("@main/data/production/", entry["main_url"])
            self.assertIn(f"@{COMMIT}/data/production/", entry["immutable_url"])
            self.assertIn("pili5420.github.io/OIS-Data-Engine/", entry["pages_fallback_url"])

    def test_rejects_branch_short_sha_and_invalid_repository(self):
        for commit in ("main", "a" * 7, "x" * 40):
            with self.subTest(commit=commit), self.assertRaises(ValueError):
                cdn.make_report(REPOSITORY, commit)
        with self.assertRaises(ValueError):
            cdn.make_report("owner/repo?query=1", COMMIT)

    @patch.object(cdn.time, "sleep")
    @patch.object(cdn, "request")
    def test_requires_http_200_json_type_valid_json_and_exact_bytes(self, request, sleep):
        expected = b'{"value": 1}'
        failures = ((503, "application/json", expected), (200, "text/plain", expected),
                    (200, "application/json", b"not JSON"), (200, "application/json", b'{"value": 0}'))
        for response in failures:
            request.return_value = response
            with self.subTest(response=response), self.assertRaises(RuntimeError):
                cdn.verify_endpoint(URL, expected)
        request.return_value = (200, "application/json", expected)
        self.assertEqual("PASS", cdn.verify_endpoint(URL, expected)["status"])

    @patch.object(cdn.time, "sleep")
    @patch.object(cdn, "request")
    def test_retries_stale_cache_without_changing_canonical_url(self, request, sleep):
        request.side_effect = [(200, "application/json", b'{"value": 0}'),
                               (200, "application/json", b'{"value": 1}')]
        result = cdn.verify_endpoint(URL, b'{"value": 1}')
        self.assertEqual(2, result["attempts"])
        self.assertTrue(all(call.args == (URL,) for call in request.call_args_list))
        sleep.assert_called_once_with(5)

    @patch.object(cdn, "request")
    def test_purge_requires_completion_and_all_providers(self, request):
        request.return_value = (200, "application/json", json.dumps(purge_result()).encode())
        self.assertEqual("PASS", cdn.purge_endpoint(URL)["status"])
        for modification in ({"throttled": True}, {"providers": {"CF": True, "FY": False}}, {"providers": {}}):
            result = purge_result()
            result["paths"][PURGE_PATH].update(modification)
            request.return_value = (200, "application/json", json.dumps(result).encode())
            with self.subTest(modification=modification), self.assertRaises(RuntimeError):
                cdn.purge_endpoint(URL)

    @patch.object(cdn.time, "sleep")
    @patch.object(cdn, "request")
    def test_waits_for_pending_purge(self, request, sleep):
        request.side_effect = [(202, "application/json", b'{"id":"request-1","status":"pending"}'),
                               (200, "application/json", json.dumps(purge_result()).encode())]
        self.assertEqual("PASS", cdn.purge_endpoint(URL)["status"])
        self.assertEqual("https://purge.jsdelivr.net/status/request-1", request.call_args.args[0])

    @patch.object(cdn, "request")
    @patch.object(cdn, "load_pass_production", side_effect=ValueError("not PASS"))
    def test_rejects_failed_production_before_any_network_request(self, load, request):
        report = cdn.distribute(REPOSITORY, COMMIT, Path("unused"))
        self.assertEqual("FAIL", report["overall_status"])
        request.assert_not_called()

    @patch.object(cdn, "verify_endpoint")
    @patch.object(cdn.subprocess, "check_output", return_value="b" * 40)
    @patch.object(cdn, "load_pass_production", return_value={})
    def test_rejects_pre_update_workflow_sha(self, load, git, verify):
        report = cdn.distribute(REPOSITORY, COMMIT, Path("unused"))
        self.assertEqual("FAIL", report["overall_status"])
        self.assertIn("post-push", report["errors"][0])
        verify.assert_not_called()

    @patch.object(cdn, "purge_endpoint")
    @patch.object(cdn, "verify_endpoint")
    @patch.object(cdn.subprocess, "check_output")
    @patch.object(cdn, "load_pass_production")
    def test_verifies_committed_blobs_and_keeps_purge_failures_visible(self, load, git, verify, purge):
        contents = {name: json.dumps({"file": name}).encode() for name in cdn.PUBLIC_FILES}
        load.return_value = contents
        verify.return_value = {"status": "PASS"}
        purge.return_value = {"status": "PASS"}
        for fail_purge in (False, True):
            git.side_effect = [COMMIT, *contents.values()]
            purge.side_effect = RuntimeError("purge throttled") if fail_purge else None
            with self.subTest(fail_purge=fail_purge):
                report = cdn.distribute(REPOSITORY, COMMIT, Path("unused"))
                self.assertEqual("FAIL" if fail_purge else "PASS", report["overall_status"])
                self.assertEqual(6, verify.call_count)
                self.assertEqual(3, purge.call_count)
                for filename, entry in report["files"].items():
                    self.assertIn("expected_sha256", entry)
                    self.assertEqual("PASS", entry["immutable"]["status"])
                    self.assertEqual("PASS", entry["main"]["status"])
                    self.assertEqual("FAIL" if fail_purge else "PASS", entry["purge"]["status"])
                for call in git.call_args_list[-3:]:
                    self.assertIn(COMMIT + ":data/production/", call.args[0][-1])
            verify.reset_mock()
            purge.reset_mock()


if __name__ == "__main__":
    unittest.main()
