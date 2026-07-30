"""
Tool Grouping Layer for LLM Token Efficiency.

Group definitions live in tool_catalog.json so Python, Worker, docs, and smoke
checks can share one machine-readable catalog.
"""

from __future__ import annotations

import copy
import inspect
import json
import types
from pathlib import Path
from typing import Any, Literal, Union, get_args, get_origin, get_type_hints

from yfmcp.envelope import ErrorCode, _mcp_failure


_CATALOG_PATH = Path(__file__).with_name("tool_catalog.json")


def _load_catalog() -> dict[str, Any]:
    with _CATALOG_PATH.open(encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict) or not isinstance(data.get("groups"), dict):
        raise RuntimeError("tool_catalog.json must contain a groups object")
    return data


TOOL_CATALOG = _load_catalog()
TOOL_GROUPS: dict[str, dict[str, Any]] = TOOL_CATALOG["groups"]


def _resolve_handler(handler_name: str, handler_registry: dict):
    handler = handler_registry.get(handler_name)
    if handler is None or not callable(handler):
        raise ValueError(f"Handler '{handler_name}' not found in handler registry")
    return handler


def _is_empty_required_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, set, dict)):
        return not value
    return False


def _matches_annotation(value: Any, annotation: Any) -> bool:
    if annotation in (Any, inspect.Parameter.empty):
        return True
    if annotation is None or annotation is type(None):
        return value is None
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin in (Union, types.UnionType):
        return any(_matches_annotation(value, option) for option in args)
    if origin is Literal:
        # ponytail: grouped boundary checks type; handlers own semantic values.
        return any(_matches_annotation(value, type(option)) for option in args)
    if origin is list:
        return isinstance(value, list) and (
            not args or all(_matches_annotation(item, args[0]) for item in value)
        )
    if origin is dict:
        return isinstance(value, dict) and (
            len(args) < 2
            or all(
                _matches_annotation(key, args[0]) and _matches_annotation(item, args[1])
                for key, item in value.items()
            )
        )
    if origin is tuple:
        if not isinstance(value, tuple):
            return False
        if len(args) == 2 and args[1] is Ellipsis:
            return all(_matches_annotation(item, args[0]) for item in value)
        return len(args) == len(value) and all(
            _matches_annotation(item, expected) for item, expected in zip(value, args)
        )
    if annotation is float:
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if annotation is int:
        return isinstance(value, int) and not isinstance(value, bool)
    if isinstance(annotation, type):
        return isinstance(value, annotation)
    return True


def _input_failure(
    tool: str,
    message: str,
    *,
    missing_params: list[str] | None = None,
    invalid_params: list[str] | None = None,
    unexpected_params: list[str] | None = None,
    expected_params: list[str] | None = None,
) -> str:
    details = {
        "missingParams": missing_params or [],
        "invalidParams": invalid_params or [],
        "unexpectedParams": unexpected_params or [],
        "expectedParams": expected_params or [],
        "recommendedNextAction": "CORRECT_TOOL_PARAMS",
    }
    response = _mcp_failure(
        tool,
        ErrorCode.INPUT_VALIDATION_ERROR,
        message,
        meta_extra={"error_extra": details},
    )
    parsed = json.loads(response)
    if parsed.get("error") is True:
        parsed.update(details)
        return json.dumps(parsed)
    return response


def _normalize_legacy_failure(tool: str, result: Any) -> Any:
    if not isinstance(result, str):
        return result
    text = result.strip()
    if not text:
        return result
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError):
        parsed = None
    if isinstance(parsed, (dict, list)):
        return result
    if isinstance(parsed, str):
        text = parsed.strip()

    lower = text.lower()
    if not (
        lower.startswith("error")
        or (lower.startswith("company ticker") and "not found" in lower)
    ):
        return result
    if lower.startswith("error: invalid") or " is required" in lower:
        code = ErrorCode.INPUT_VALIDATION_ERROR
    elif "no option" in lower:
        code = ErrorCode.NO_OPTIONS_DATA
    elif "not found" in lower:
        code = ErrorCode.TICKER_NOT_FOUND
    elif "rate limit" in lower or "429" in lower:
        code = ErrorCode.RATE_LIMIT
    elif "timeout" in lower or "timed out" in lower:
        code = ErrorCode.PROVIDER_TIMEOUT
    else:
        code = ErrorCode.PROVIDER_ERROR
    return _mcp_failure(tool, code, text)


def _inline_local_schema_refs(schema: dict[str, Any]) -> dict[str, Any]:
    """Inline Pydantic-local refs so each action schema is self-contained."""
    definitions = schema.get("$defs")
    definitions = definitions if isinstance(definitions, dict) else {}

    def _walk(value: Any, seen: frozenset[str] = frozenset()) -> Any:
        if isinstance(value, list):
            return [_walk(item, seen) for item in value]
        if not isinstance(value, dict):
            return value
        reference = value.get("$ref")
        if isinstance(reference, str) and reference.startswith("#/$defs/"):
            name = reference.removeprefix("#/$defs/")
            target = definitions.get(name)
            if isinstance(target, dict) and name not in seen:
                return _walk(copy.deepcopy(target), seen | {name})
        return {
            key: _walk(item, seen)
            for key, item in value.items()
            if key != "$defs"
        }

    resolved = _walk(copy.deepcopy(schema))
    return resolved if isinstance(resolved, dict) else {}


def _grouped_input_schema(
    group_name: str,
    group_def: dict[str, Any],
    contract_registry: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    actions = group_def.get("actions")
    if not isinstance(actions, dict) or not actions:
        raise RuntimeError(f"Grouped tool '{group_name}' has no actions")

    branches: list[dict[str, Any]] = []
    for action, handler_name in actions.items():
        contract = contract_registry.get(handler_name)
        if not isinstance(contract, dict):
            raise RuntimeError(
                f"No canonical input schema for grouped action "
                f"'{group_name}.{action}' ({handler_name})"
            )
        params_schema = _inline_local_schema_refs(contract)
        if params_schema.get("type") != "object" or not isinstance(
            params_schema.get("properties"), dict
        ):
            raise RuntimeError(
                f"Canonical input schema for '{group_name}.{action}' is not an object"
            )
        params_schema.pop("title", None)
        params_schema["additionalProperties"] = False
        required_params = params_schema.get("required")
        branch_required = ["action"]
        if isinstance(required_params, list) and required_params:
            branch_required.append("params")
        branches.append(
            {
                "title": action,
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "const": action,
                        "enum": [action],
                    },
                    "params": params_schema,
                },
                "required": branch_required,
                "additionalProperties": False,
            }
        )

    return {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": list(actions)},
            "params": {
                "type": "object",
                "description": (
                    "Arguments for the selected action. The matching oneOf branch "
                    "defines allowed and required fields."
                ),
            },
        },
        "required": ["action"],
        "oneOf": branches,
        "additionalProperties": False,
    }


async def _route_grouped_call(
    group_name: str,
    action: str,
    params: dict[str, Any] | None,
    handler_registry: dict,
) -> str:
    group = TOOL_GROUPS.get(group_name)
    if group is None:
        return _input_failure(group_name, f"Unknown grouped tool '{group_name}'.")
    if not isinstance(action, str) or not action.strip():
        return _input_failure(group_name, "action is required.")
    action = action.strip()
    handler_name = group["actions"].get(action)
    if handler_name is None:
        valid_actions = sorted(group["actions"])
        return _input_failure(
            group_name,
            f"Unknown action '{action}' for grouped tool '{group_name}'. "
            f"Valid actions: {', '.join(valid_actions)}.",
        )
    if params is None:
        params = {}
    if not isinstance(params, dict):
        return _input_failure(action, "params must be an object when provided.")

    try:
        handler = _resolve_handler(handler_name, handler_registry)
    except ValueError as exc:
        return _mcp_failure(action, ErrorCode.PROVIDER_ERROR, str(exc))

    signature = inspect.signature(handler)
    parameters = {
        name: parameter
        for name, parameter in signature.parameters.items()
        if parameter.kind
        in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    }
    accepts_extra = any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )
    expected = list(parameters)
    required = [
        name
        for name, parameter in parameters.items()
        if parameter.default is inspect.Parameter.empty
    ]
    missing = [name for name in required if name not in params]
    empty = [
        name
        for name in required
        if name in params and _is_empty_required_value(params[name])
    ]
    unexpected = [] if accepts_extra else sorted(set(params) - set(parameters))
    try:
        type_hints = get_type_hints(handler)
    except (NameError, TypeError):
        type_hints = {}
    invalid_types = sorted(
        name
        for name, value in params.items()
        if name in parameters
        and not _matches_annotation(value, type_hints.get(name, parameters[name].annotation))
    )
    invalid = sorted(set(empty + invalid_types))
    if missing or invalid or unexpected:
        reasons = []
        if missing:
            reasons.append(f"missing required parameter(s): {', '.join(missing)}")
        if empty:
            reasons.append(f"empty required parameter(s): {', '.join(empty)}")
        typed_invalid = [name for name in invalid_types if name not in empty]
        if typed_invalid:
            reasons.append(f"invalid parameter value(s): {', '.join(typed_invalid)}")
        if unexpected:
            reasons.append(f"unexpected parameter(s): {', '.join(unexpected)}")
        return _input_failure(
            action,
            f"Invalid params for '{action}': {'; '.join(reasons)}.",
            missing_params=missing,
            invalid_params=invalid,
            unexpected_params=unexpected,
            expected_params=expected,
        )

    try:
        bound = signature.bind(**params)
    except TypeError as exc:
        return _input_failure(
            action,
            f"Invalid params for '{action}': {exc}.",
            invalid_params=sorted(params),
            expected_params=expected,
        )
    result = handler(*bound.args, **bound.kwargs)
    if inspect.isawaitable(result):
        result = await result
    return _normalize_legacy_failure(action, result)


def register_grouped_tools(
    server,
    handler_registry: dict,
    contract_registry: dict[str, dict[str, Any]],
) -> None:
    """Register domain-grouped meta-tools on the FastMCP server.

    Each meta-tool accepts:
      - action: str (required) โ€” which sub-action to invoke
      - params: dict (optional) โ€” parameters for the sub-action (e.g. ticker, period, etc.)

    ``handler_registry`` maps handler function name -> function (see
    ``yfmcp.app.build_handler_registry``). This replaces the 111 individual tool
    registrations with 11 grouped tools, reducing LLM token overhead.
    """

    def _make_handler(gn, registry):
        async def handler(action: str, params: dict | None = None) -> str:
            return await _route_grouped_call(gn, action, params, registry)
        handler.__name__ = gn
        handler.__qualname__ = gn
        return handler

    for group_name, group_def in TOOL_GROUPS.items():
        handler = _make_handler(group_name, handler_registry)
        server.tool(
            name=group_name,
            description=group_def["description"],
        )(handler)
        manager = getattr(server, "_tool_manager", None)
        registered = (
            manager.get_tool(group_name)
            if manager is not None and hasattr(manager, "get_tool")
            else getattr(manager, "_tools", {}).get(group_name)
        )
        if registered is None:
            raise RuntimeError(f"Grouped tool '{group_name}' was not registered")
        registered.parameters = _grouped_input_schema(
            group_name,
            group_def,
            contract_registry,
        )


def get_all_grouped_action_names() -> list[str]:
    """Return a flat list of all action names across all groups."""
    names = []
    for group_def in TOOL_GROUPS.values():
        names.extend(group_def["actions"].keys())
    return names


def get_group_for_action(action: str) -> str | None:
    """Given an action name, return which group it belongs to."""
    for group_name, group_def in TOOL_GROUPS.items():
        if action in group_def["actions"]:
            return group_name
    return None

