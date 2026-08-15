import assert from "node:assert/strict";
import fs from "node:fs";

import { yahooTranscriptContentSha256 } from "../src/transcript-contract.ts";


const payload = JSON.parse(
  fs.readFileSync(new URL("../../fixtures/yahoo_quartr_transcript.json", import.meta.url), "utf8"),
);
const content = payload.transcriptContent;
const speakerMap = new Map();
const speakers = content.speaker_mapping.map((row) => {
  const speakerData = row.speaker_data ?? {};
  const compact = {
    speakerId: row.speaker ?? null,
    name: speakerData.name ?? null,
    role: speakerData.role ?? null,
    company: speakerData.company ?? null,
  };
  speakerMap.set(String(row.speaker), compact);
  return compact;
});
const paragraphs = content.transcript.paragraphs.map((row, index) => {
  const speaker = speakerMap.get(String(row.speaker)) ?? {};
  return {
    index,
    speaker: speaker.name ?? null,
    role: speaker.role ?? null,
    company: speaker.company ?? null,
    start: row.start ?? null,
    end: row.end ?? null,
    text: String(row.text ?? "").trim(),
  };
});

const digest = await yahooTranscriptContentSha256(
  "LITE",
  "660925",
  "Q4",
  2026,
  speakers,
  paragraphs,
);
assert.equal(digest, "b41b012512c35a2ae936fccf99249368a9994c2a0ae086a8fffb0d3f402f0a43");
console.log("PASS canonical transcript hash contract");
