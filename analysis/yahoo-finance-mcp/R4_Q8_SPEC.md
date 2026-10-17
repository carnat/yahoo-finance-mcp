# R4, fourth release (2.13.1): Q8 MCP SDK pin and public APIs only (Python)

**Owner decision Q8:** pin the deployed MCP SDK major, use only public APIs, and fail loudly. Do not do a major-version uplift inside modernisation. Brief R4 asks for `mcp>=2.2,<3` and for replacing `_tool_manager._tools`, the `FastMCP.tool` monkey-patch and `reseat_tool_registration`.

**Version: 2.13.1** (patch). Python only; the Worker has no MCP SDK (hand-rolled JSON-RPC). **Rule zero:** every client-visible output is byte-identical, including Python `tools/list` (names, order, descriptions, `inputSchema` incl. `maxItems`, `outputSchema`, `annotations`) in expanded and grouped mode, `health_check` `manifestHash`/`schemaHash`, and tool results.

## 1. Pin

- `pyproject.toml`: `mcp[cli]>=2.2,<3`. Refresh `uv.lock` only if `uv lock --check` requires it; it already has 2.3.0.
- `scripts/test_workflow_sdk_versions.py` asserts the lower bound `>=2.2` as well as `<3`.

## 2. Replace private internals with public APIs (inventory of 2026-10-17)

| Today | Replacement |
|---|---|
| `app.py` 1.x import fallback (`fastmcp`), 1.x `_mcp_server.version`, `output_schema` signature sniffing | A hard import of `mcp.server.mcpserver.MCPServer`. An ImportError propagates with a clear message naming the supported range. Use the public `version` property. |
| Monkey-patch of `FastMCP.tool` (`_yfmcp_original_tool`), which adds annotations, wraps in `_offload_to_thread` (gate, envelope, budget) and marks `_yfmcp_handler` | A subclass `YfmcpServer(MCPServer)` in `yfmcp/app.py` overrides the **public** `tool()` method. It wraps fn exactly as today, then calls `super().tool(...)` with only the parameters the 2.x signature accepts. It also records each tool in our own ordered registry (name → fn, handler, kwargs). `yfinance_server` becomes a `YfmcpServer`. No class-level patching. |
| `build_handler_registry`, `build_tool_contract_registry`, `yfmcp/tools/system.py` reading `_tool_manager._tools` | Read our registry. Input schemas come from the public `await server.list_tools()` (`inputSchema`) or `Tool.from_function`. They raise if a registered tool is missing, instead of returning an empty result silently. |
| `reseat_tool_registration` (reorders `_tool_manager._tools`; one use for get_company_news) | Our `list_tools()` override (public method, `_handle_list_tools` calls it) orders the result by our registry. Our registry supports an explicit `after=` position, used once for get_company_news. Delete `reseat_tool_registration`. |
| `apply_ticker_caps` and the grouped `parameters` override in `tool_groups.py` (mutating `_tool_manager._tools[*].parameters`) | Apply in the `list_tools()` override: start from `super().list_tools()`, then patch `inputSchema` (ticker `maxItems` from `tickerCaps`; grouped schema from `_grouped_input_schema`) on the returned `mcp.types.Tool` objects. Argument validation is unchanged: our gate and grouped checker already enforce caps and shapes. |
| `output_schema=` passed to `tool()` and silently dropped on 2.x | Stop passing it. The 2.x decorator has no such parameter, so passing it would now raise. The current listing (the SDK's auto-derived output schema) stays byte-identical. The planner records the missing Python `outputSchema` as a Q3i triage item. It is not fixed here, because declaring schemas would change what clients validate. |

**Tests:**
- Remove the 17 scripts' `_FastMCP.tool` monkey-patches. Where a test needs the raw handler or no thread offload, use a public seam in our own code (for example `yfmcp.app.registered_handler(name)`), never SDK internals.
- Replace `_tool_manager._tools[...]`, `_mcp_server` and `_lowlevel_server` uses with our registry, `list_tools()` or `version`.
- Add a guard test that fails if `yfmcp/`, `server.py`, `tool_groups.py` or `scripts/` access `_tool_manager`, `_mcp_server`, `_lowlevel_server` or patch `MCPServer`/`FastMCP` attributes.

## 3. Proof of byte-identity (required in the report)

- **Before/after dumps:** capture Python `tools/list` (expanded and grouped) as JSON at the base commit and after the change, then diff. The diff must be empty.
- **Hashes:** compare the `health_check` `manifestHash`/`schemaHash` computed locally.
- **Tests:** the parity harness, `check_tool_sync`, `test_grouped_contract_parity` and every offline `ci.yml` step pass.

## 4. Docs and live checks

- **Docs section "MCP SDK Pin (2.13.1)":** the range, the public-API rule, the guard test, and the outputSchema note.
- **Live checks:**
  - the Worker redeploys unchanged at 2.13.1, with the canaries passing;
  - the connector `health_check` hashes are unchanged from 2.13.0, apart from the version and build identity.
