import catalog from "../../tool_catalog.json";

export interface GroupedToolDef {
  name: string;
  description: string;
  actions: Record<string, string>;
}

export const GROUPED_TOOL_DEFS: GroupedToolDef[] = Object.entries(catalog.groups).map(
  ([name, group]) => ({ name, description: group.description, actions: group.actions }),
);

/**
 * Per-action batch ticker caps (2.12.0), shared with the Python server. Keys are
 * public action names; "get_market_snapshot.full" is get_market_snapshot with
 * mode "full". Schema maxItems is generated from this table and the tool-call
 * gate (tools.ts) enforces it.
 */
export const TICKER_CAPS: Readonly<Record<string, number>> = Object.freeze(
  Object.assign(Object.create(null) as Record<string, number>, catalog.tickerCaps),
);

export function tickerCap(key: string): number {
  const cap = TICKER_CAPS[key];
  if (typeof cap !== "number") throw new Error(`No tickerCaps entry for '${key}' in tool_catalog.json`);
  return cap;
}
