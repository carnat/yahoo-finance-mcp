import pathlib
import sys
import unittest

sys.path.append(str(pathlib.Path(__file__).resolve().parent))
from source_text import definition_source, source_between, worker_runtime_source  # noqa: E402


ROOT = pathlib.Path(__file__).resolve().parents[1]


class TestWorkerTransportContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mcp = (ROOT / "worker" / "src" / "mcp.ts").read_text(encoding="utf-8")
        cls.index = (ROOT / "worker" / "src" / "index.ts").read_text(encoding="utf-8")
        cls.tools = worker_runtime_source()  # every Worker runtime module (scripts/source_text.py)
        cls.response = (ROOT / "worker" / "src" / "response.ts").read_text(encoding="utf-8")
        cls.stamp = (ROOT / "scripts" / "stamp_worker_build.py").read_text(encoding="utf-8")

    def test_tools_call_returns_structured_content_and_tool_error_flag(self):
        self.assertIn("structuredContent", self.mcp)
        self.assertIn("isError: true", self.mcp)
        self.assertIn('structuredContent.ok === false', self.mcp)

    def test_initialize_negotiates_current_and_legacy_protocols(self):
        self.assertIn('"2025-06-18"', self.mcp)
        self.assertIn('"2024-11-05"', self.mcp)
        self.assertIn("getBuildVersion()", self.mcp)

    def test_envelope_schema_matches_structured_response(self):
        schema = definition_source("worker", "ENVELOPE_V2_OUTPUT_SCHEMA")
        for field in ('ok:', 'data:', 'meta:', 'error:'):
            self.assertIn(field, schema)
        self.assertIn('required: ["ok", "data", "meta", "error"]', schema)

    def test_dispatch_errors_are_stable_tool_errors(self):
        body = source_between("worker", "export async function callTool(", "export async function callVisibleTool")
        for code in ("TICKER_NOT_FOUND", "RATE_LIMIT", "PROVIDER_TIMEOUT", "PROVIDER_ERROR"):
            self.assertIn(code, body)
        self.assertIn("retryable", body)

    def test_audit_endpoint_fails_closed_without_secret(self):
        audit_start = self.index.index('pathname === "/audit/mcp"')
        audit_end = self.index.index("// MCP endpoint", audit_start)
        body = self.index[audit_start:audit_end]
        self.assertIn("if (!token)", body)
        self.assertIn('status: 404', body)

    def test_runtime_version_is_used_in_envelopes(self):
        self.assertIn('getWorkerVar("SERVER_VERSION")', self.response)
        self.assertIn("getServerVersion()", self.response)

    def test_deploy_stamp_updates_release_and_build_identity(self):
        self.assertIn('_upsert_var(text, "SERVER_VERSION", release_version)', self.stamp)
        self.assertIn('_upsert_var(text, "BUILD_VERSION", build_version)', self.stamp)
        self.assertIn('_upsert_var(text, "BUILD_SHA", build_sha)', self.stamp)
        self.assertIn('build_date = deployed_at[:10]', self.stamp)
        self.assertIn('_upsert_var(text, "BUILD_DATE", build_date)', self.stamp)


if __name__ == "__main__":
    unittest.main()
