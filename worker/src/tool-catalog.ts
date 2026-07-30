import catalog from "../../tool_catalog.json";

export interface GroupedToolDef {
  name: string;
  description: string;
  actions: Record<string, string>;
}

export const GROUPED_TOOL_DEFS: GroupedToolDef[] = Object.entries(catalog.groups).map(
  ([name, group]) => ({ name, description: group.description, actions: group.actions }),
);
