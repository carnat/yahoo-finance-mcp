from __future__ import annotations

from pathlib import Path
import unittest

from scripts.public_tool_contract import (
    DESCRIPTION_CONCEPTS,
    OUTPUT_SCHEMA_FIELDS,
    validate_live_tool_contract,
    validate_python_schema_source,
    validate_worker_source_contract,
)


ROOT = Path(__file__).resolve().parents[1]


def _valid_live_tools() -> list[dict]:
    tools: list[dict] = []
    for name, groups in DESCRIPTION_CONCEPTS.items():
        description = " ".join(group[0] for group in groups)
        fields = OUTPUT_SCHEMA_FIELDS.get(name)
        output_schema = {"type": "object", "properties": {}}
        if fields:
            output_schema = {
                "type": "object",
                "properties": {
                    "ok": {"type": "boolean"},
                    "data": {
                        "type": ["object", "null"],
                        "properties": {field: {} for field in fields},
                    },
                    "meta": {"type": "object"},
                    "error": {"type": ["object", "null"]},
                },
            }
        tools.append({"name": name, "description": description, "outputSchema": output_schema})
    return tools


class TestPublicToolContract(unittest.TestCase):
    def test_shared_live_contract_accepts_semantic_alternatives_and_envelope(self) -> None:
        tools = _valid_live_tools()
        calendar = next(tool for tool in tools if tool["name"] == "get_company_events_calendar")
        calendar["description"] = "Earnings dates are UNVERIFIED provider data."
        validate_live_tool_contract(tools)

    def test_shared_live_contract_rejects_missing_output_field(self) -> None:
        tools = _valid_live_tools()
        fund = next(tool for tool in tools if tool["name"] == "get_fund_profile")
        del fund["outputSchema"]["properties"]["data"]["properties"]["sectionStatus"]
        with self.assertRaisesRegex(AssertionError, "sectionStatus"):
            validate_live_tool_contract(tools)

    def test_worker_source_matches_live_contract(self) -> None:
        source = (ROOT / "worker" / "src" / "tools.ts").read_text(encoding="utf-8")
        validate_worker_source_contract(source)

    def test_worker_source_rejects_generic_high_value_schema(self) -> None:
        source = (ROOT / "worker" / "src" / "tools.ts").read_text(encoding="utf-8")
        source = source.replace(
            "get_fund_profile: FUND_PROFILE_OUTPUT_SCHEMA,",
            "get_fund_profile: SIMPLE_OBJECT_SCHEMA,",
            1,
        )
        with self.assertRaisesRegex(AssertionError, "schema mapping"):
            validate_worker_source_contract(source)

    def test_python_schema_mirror_exposes_high_value_fields(self) -> None:
        source = (ROOT / "yfmcp" / "schemas.py").read_text(encoding="utf-8")
        validate_python_schema_source(source)

    def test_python_source_rejects_generic_high_value_schema(self) -> None:
        source = (ROOT / "yfmcp" / "schemas.py").read_text(encoding="utf-8")
        source = source.replace(
            '"get_etf_info": _FUND_PROFILE_OUTPUT_SCHEMA,',
            '"get_etf_info": _SIMPLE_OUTPUT_SCHEMA,',
            1,
        )
        with self.assertRaisesRegex(AssertionError, "schema mapping"):
            validate_python_schema_source(source)


if __name__ == "__main__":
    unittest.main()
