/**
 * Outbound fetch allow-list (2.10.0).
 *
 * Every provider request goes through providerFetch (request-context.ts),
 * which runs checkFetchUrl on the first URL and on every redirect hop. A URL
 * is allowed only when it is https, carries no credentials, uses the default
 * port, does not name an IP literal or a local-only name, and its host is
 * exactly on the allow-list below. Python mirrors these rules and reason
 * strings in yfmcp/url_policy.py.
 *
 * DNS is not checked (Workers cannot resolve names); the exact-host list means
 * a DNS-based bypass needs a host we already trust.
 */

import registryManifest from "./company-ir-page-registry.json";

export type FetchUrlCheck = { ok: true; url: URL } | { ok: false; reason: string };

/** A refused fetch: the first URL or a redirect hop failed the allow-list. */
export class UrlPolicyError extends Error {
  readonly reason: string;
  constructor(reason: string) {
    super(reason);
    this.name = "UrlPolicyError";
    this.reason = reason;
  }
}

export const SEC_HOSTS: readonly string[] = ["www.sec.gov", "data.sec.gov", "efts.sec.gov"];
export const YAHOO_HOSTS: readonly string[] = [
  "finance.yahoo.com",
  "query1.finance.yahoo.com",
  "query2.finance.yahoo.com",
  "fc.yahoo.com",
];
const PROVIDER_HOSTS: readonly string[] = [
  "www.globenewswire.com",
  "finnhub.io",
  "www.alphavantage.co",
  "api.marketaux.com",
  "api.sec.or.th",
  "api.company-information.service.gov.uk",
];

type RegistryEntry = {
  status?: string;
  allowedHosts?: string[];
  allowedPathPrefixes?: string[];
};

const approvedRegistryEntries: RegistryEntry[] = (
  ((registryManifest as unknown as { sources?: RegistryEntry[] }).sources) ?? []
).filter((entry) => entry && entry.status === "approved");

/** Hosts of approved company IR registry entries, computed once at module load. */
export const REGISTRY_HOSTS: readonly string[] = [
  ...new Set(approvedRegistryEntries.flatMap((entry) => (entry.allowedHosts ?? []).map(normalizeHost))),
];

const ALLOWED_HOSTS = new Set<string>([...SEC_HOSTS, ...YAHOO_HOSTS, ...PROVIDER_HOSTS, ...REGISTRY_HOSTS]);

const BLOCKED_SUFFIXES = [".localhost", ".local", ".internal", ".invalid"];

function normalizeHost(host: string): string {
  return host.toLowerCase().replace(/\.$/, "");
}

/** Dotted IPv4, any bracketed IPv6, or an integer/hex/octal IPv4 form. */
function isIpLiteral(host: string): boolean {
  if (host.startsWith("[") || host.includes(":")) return true;
  const labels = host.split(".");
  return labels.length > 0 && labels.every((label) => /^(0x[0-9a-f]*|\d+)$/i.test(label));
}

export function checkFetchUrl(url: string): FetchUrlCheck {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return { ok: false, reason: "invalid URL" };
  }
  if (parsed.protocol !== "https:") return { ok: false, reason: "URL must use https" };
  if (parsed.username || parsed.password) return { ok: false, reason: "URL must not contain credentials" };
  if (parsed.port !== "" && parsed.port !== "443") return { ok: false, reason: "URL port not allowed" };
  const host = normalizeHost(parsed.hostname);
  if (
    host === "" ||
    isIpLiteral(host) ||
    host === "localhost" ||
    BLOCKED_SUFFIXES.some((suffix) => host.endsWith(suffix))
  ) {
    return { ok: false, reason: "URL host not allowed" };
  }
  if (!ALLOWED_HOSTS.has(host)) return { ok: false, reason: "URL host not allowed" };
  return { ok: true, url: parsed };
}

function hostWithoutWww(host: string): string {
  return host.toLowerCase().replace(/^www\./, "");
}

/** A URL inside a registry entry's allowed hosts and path prefixes. */
export function companyIrPageUrlAllowed(
  entry: { allowedHosts: string[]; allowedPathPrefixes: string[] },
  rawUrl: string,
): boolean {
  try {
    const url = new URL(rawUrl);
    if (url.protocol !== "https:") return false;
    const host = hostWithoutWww(url.hostname);
    const allowedHost = entry.allowedHosts.some((candidate) => host === hostWithoutWww(candidate));
    const allowedPath = entry.allowedPathPrefixes.some((prefix) => url.pathname.startsWith(prefix));
    return allowedHost && allowedPath;
  } catch {
    return false;
  }
}

export type AllowedHostKind = "sec" | "yahoo" | "registry" | "provider";

/** Which part of the allow-list a host (already passed by checkFetchUrl) belongs to. */
export function allowedHostKind(url: URL): AllowedHostKind {
  const host = normalizeHost(url.hostname);
  if (SEC_HOSTS.includes(host)) return "sec";
  if (YAHOO_HOSTS.includes(host)) return "yahoo";
  if (REGISTRY_HOSTS.includes(host)) return "registry";
  return "provider";
}

/** A caller URL on a registry host must sit inside an approved entry's path prefixes. */
export function approvedRegistryUrlAllowed(rawUrl: string): boolean {
  return approvedRegistryEntries.some((entry) =>
    companyIrPageUrlAllowed({ allowedHosts: entry.allowedHosts ?? [], allowedPathPrefixes: entry.allowedPathPrefixes ?? [] }, rawUrl),
  );
}
