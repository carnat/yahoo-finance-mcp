function canonicalJsonStringify(value: unknown): string {
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return JSON.stringify(value);
  }
  if (typeof value === "number") {
    return JSON.stringify(Number.isFinite(value) ? value : null);
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalJsonStringify(item)).join(",")}]`;
  }
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record)
      .filter((key) => record[key] !== undefined)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJsonStringify(record[key])}`)
      .join(",")}}`;
  }
  return "null";
}

function yahooTranscriptContentHashDocument(
  ticker: string,
  eventId: string | null,
  fiscalPeriod: string | null,
  fiscalYear: number | null,
  speakers: Record<string, unknown>[],
  paragraphs: Record<string, unknown>[],
): Record<string, unknown> {
  const textOrNull = (value: unknown): string | null => value == null || value === "" ? null : String(value);
  return {
    contract: "CANONICAL_TRANSCRIPT_TEXT_V1",
    ticker: ticker.toUpperCase(),
    eventId: textOrNull(eventId),
    fiscalPeriod: textOrNull(fiscalPeriod),
    fiscalYear: textOrNull(fiscalYear),
    speakers: speakers.map((speaker) => ({
      speakerId: textOrNull(speaker.speakerId),
      name: textOrNull(speaker.name),
      role: textOrNull(speaker.role),
      company: textOrNull(speaker.company),
    })),
    paragraphs: paragraphs.map((paragraph) => ({
      speaker: textOrNull(paragraph.speaker),
      role: textOrNull(paragraph.role),
      company: textOrNull(paragraph.company),
      text: String(paragraph.text ?? ""),
    })),
  };
}

async function sha256Hex(value: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

export async function yahooTranscriptContentSha256(
  ticker: string,
  eventId: string | null,
  fiscalPeriod: string | null,
  fiscalYear: number | null,
  speakers: Record<string, unknown>[],
  paragraphs: Record<string, unknown>[],
): Promise<string> {
  const document = yahooTranscriptContentHashDocument(
    ticker,
    eventId,
    fiscalPeriod,
    fiscalYear,
    speakers,
    paragraphs,
  );
  return sha256Hex(canonicalJsonStringify(document));
}
