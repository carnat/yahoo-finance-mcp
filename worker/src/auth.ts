/**
 * Credential handling for the /mcp endpoint.
 *
 * The credential is a static token (`env.MCP_TOKEN`) presented either as
 * `Authorization: Bearer <token>` or as the path `/mcp/<token>`. Callers use
 * `resolveCredential` only; swapping the static token for another scheme means
 * replacing this file, not the call sites.
 *
 * The token must never reach a response, log line, error message or telemetry.
 * If a URL is ever logged, log `redactPath(pathname)` instead.
 */

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
  const pathMatch = PATH_FORM.exec(pathname);
  if (pathname !== "/mcp" && !pathMatch) return { route: "other" };

  const token = configuredToken(env);
  if (pathMatch && token === "") return { route: "other" };

  const headerToken = presentedHeaderToken(request);
  let transport: CredentialTransport;
  let presented: string;
  if (headerToken !== null) {
    transport = "header";
    presented = headerToken;
  } else if (pathMatch) {
    transport = "path";
    presented = pathMatch[1];
  } else {
    return { route: "mcp", transport: "none", status: "absent" };
  }

  const status: CredentialStatus = token !== "" && timingSafeEqual(presented, token) ? "valid" : "invalid";
  return { route: "mcp", transport, status };
}
