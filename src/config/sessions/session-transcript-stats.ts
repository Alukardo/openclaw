import type { SessionTranscriptReadScope } from "./session-accessor.sqlite-contract.js";
import { readTranscriptStatsSync } from "./session-accessor.sqlite-read.js";
import { withSessionTranscriptReadSource } from "./session-transcript-read-source.js";

/** Read hot and cold transcript statistics from the captured history owner. */
export function readTranscriptStatsAsync(scope: SessionTranscriptReadScope) {
  return withSessionTranscriptReadSource(
    scope,
    readTranscriptStatsSync,
    ({ scope: captured, owner, expectedIdentity }) =>
      owner.readStats({ scope: captured, expectedIdentity }),
  );
}
