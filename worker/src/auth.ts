/**
 * Credential handling for the /mcp endpoint.
 *
 * The credential is a static token (`env.MCP_TOKEN`) presented either as
 * `Authorization: Bearer <token>` or as the path `/mcp/<token>`. Callers use
 * `resolveCredential` only; swapping the static token for another scheme means
 * replacing this file, not the call sites.
 *
 * Neither the token nor a personal key may reach a response, log line, error message or telemetry.
 * If a URL is ever logged, log `redactPath(pathname)` instead.
 */

import { checkPersonalKey } from "./access-keys.js";

export type CredentialTransport = "header" | "path" | "none";
export type CredentialStatus = "valid" | "invalid" | "absent";

export type ResolvedCredential =
  | { route: "mcp"; transport: CredentialTransport; status: CredentialStatus }
  | { route: "other" };

export interface AuthEnv {
  MCP_TOKEN?: unknown;
  [key: string]: unknown;
}

const PATH_FORM = /^\/mcp\/([^/]+)$/;
const BEARER = /^bearer[ \t]+(\S.*)$/i;

/** Constant-time string comparison so a token cannot be probed by timing. */
export function timingSafeEqual(a: string, b: string): boolean {
  const left = new TextEncoder().encode(a);
  const right = new TextEncoder().encode(b);
  let diff = left.length ^ right.length;
  for (let i = 0; i < Math.max(left.length, right.length); i++) {
    diff |= (left[i] ?? 0) ^ (right[i] ?? 0);
  }
  return diff === 0;
}

/** Replace the credential segment of an /mcp/<token> path with `<redacted>`. */
export function redactPath(pathname: string): string {
  return pathname.replace(/^(\/mcp\/)[^/]+/, "$1<redacted>");
}

function configuredToken(env: AuthEnv): string {
  return typeof env.MCP_TOKEN === "string" ? env.MCP_TOKEN : "";
}

function presentedHeaderToken(request: Request): string | null {
  const header = (request.headers.get("Authorization") ?? "").trim();
  const match = BEARER.exec(header);
  return match ? match[1].trim() : null;
}

/**
 * Work out which route a request targets and what credential it presents.
 *
 * - `/mcp` and `/mcp/<one segment>` are the MCP route; anything else is `other`.
 * - When both a header and a path credential are present, the header wins.
 * - An Authorization header that is not `Bearer <token>` is not a credential.
 * - With `MCP_TOKEN` unset or empty the path form does not exist (`other`, so
 *   the caller answers 404), and any presented header credential is `invalid`.
 */
export function resolveCredential(request: Request, pathname: string, env: AuthEnv): ResolvedCredential {
  const read = readCredential(request, pathname, env);
  if (read.route === "other") return read;
  if (read.presented === null) return { route: "mcp", transport: "none", status: "absent" };
  const token = configuredToken(env);
  const status: CredentialStatus = token !== "" && timingSafeEqual(read.presented, token) ? "valid" : "invalid";
  return { route: "mcp", transport: read.transport, status };
}

type ReadCredential =
  | { route: "mcp"; transport: CredentialTransport; presented: string | null }
  | { route: "other" };

function readCredential(request: Request, pathname: string, env: AuthEnv): ReadCredential {
  const pathMatch = PATH_FORM.exec(pathname);
  if (pathname !== "/mcp" && !pathMatch) return { route: "other" };
  // With MCP_TOKEN unset or empty the path form does not exist, for personal keys too.
  if (pathMatch && configuredToken(env) === "") return { route: "other" };
  const headerToken = presentedHeaderToken(request);
  if (headerToken !== null) return { route: "mcp", transport: "header", presented: headerToken };
  if (pathMatch) return { route: "mcp", transport: "path", presented: pathMatch[1] };
  return { route: "mcp", transport: "none", presented: null };
}

/**
 * Who is calling, for an /mcp request already resolved by resolveCredential.
 *
 * - `owner`: the owner token (no limits, provider access, no store or counter call).
 * - `key`: an active personal key within its limits; `paidProviders` from its record.
 * - `limited`: an active personal key over a limit (HTTP 429).
 * - `anonymous`: no credential (grace mode serves it, enforce rejects it).
 * - `rejected`: anything else (the existing 401), unknown and revoked keys included.
 *
 * `keyId` is `owner`, the record's 8-character id, or `-` when no key is identified.
 * Throws (fail closed) when the key store or the usage counter fails.
 */
export type CallerIdentity =
  | { kind: "owner" | "anonymous" | "rejected"; keyId: string; paidProviders: boolean }
  | { kind: "key"; keyId: string; paidProviders: boolean }
  | { kind: "limited"; keyId: string; paidProviders: boolean; retryAfterSeconds: number };

export async function identifyCaller(
  request: Request,
  pathname: string,
  env: AuthEnv,
  status: CredentialStatus,
): Promise<CallerIdentity> {
  if (status === "valid") return { kind: "owner", keyId: "owner", paidProviders: true };
  if (status === "absent") return { kind: "anonymous", keyId: "-", paidProviders: true };
  const read = readCredential(request, pathname, env);
  if (read.route !== "mcp" || read.presented === null) return { kind: "rejected", keyId: "-", paidProviders: false };
  const result = await checkPersonalKey(read.presented, env);
  if (result.kind === "allowed") return { kind: "key", keyId: result.id, paidProviders: result.paidProviders };
  if (result.kind === "limited") {
    return { kind: "limited", keyId: result.id, paidProviders: false, retryAfterSeconds: result.retryAfterSeconds };
  }
  return { kind: "rejected", keyId: "-", paidProviders: false };
}
