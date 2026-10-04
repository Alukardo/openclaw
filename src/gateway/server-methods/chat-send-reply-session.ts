import type { ReplySessionBinding } from "../../auto-reply/reply/get-reply.types.js";
import { getRuntimeConfig } from "../../config/io.js";
import { resolveSessionStorePathCore } from "../../config/sessions/paths.js";
import { readSessionTranscriptWatermark } from "../../config/sessions/session-accessor.sqlite-transcript-watermark.js";
import { captureSessionMutationRouting } from "../session-sharing-preparation.js";
import { loadGatewaySessionEntryReadOnlyInWorker } from "../session-utils-store-worker.js";
import type { PreparedChatSendSession } from "./chat-send-session.js";

export type ChatReplySession = Pick<
  PreparedChatSendSession,
  "agentId" | "backingSessionId" | "cfg" | "clientRunId" | "sessionKey" | "sessionLoadOptions"
> &
  Partial<Pick<PreparedChatSendSession, "entry" | "storePath">>;

/** Initialization owns the start binding; later delivery decisions read fresh stored rows. */
export function createChatReplySessionReader(session: ChatReplySession) {
  let preparedSession: Omit<ReplySessionBinding, "sessionId"> & { sessionId?: string } = {
    sessionKey: session.sessionKey,
    sessionId: session.entry?.sessionId ?? session.backingSessionId,
    lifecycleRevision: session.entry?.lifecycleRevision,
    storePath:
      session.storePath ??
      resolveSessionStorePathCore(session.cfg.session?.store, {
        agentId: session.agentId,
      }),
  };
  return {
    notePreparedSession(this: void, binding: ReplySessionBinding) {
      if (binding.sessionKey === session.sessionKey) {
        preparedSession = { ...binding };
      }
    },
    captureTranscriptStart(this: void) {
      const { sessionId, lifecycleRevision, storePath } = preparedSession;
      const watermark = sessionId
        ? readSessionTranscriptWatermark({
            agentId: session.agentId,
            sessionId,
            sessionKey: session.sessionKey,
            storePath,
          })
        : { generation: null, maxSeq: null };
      return {
        sessionId,
        lifecycleRevision,
        generation: watermark.generation,
        afterSeq: watermark.maxSeq ?? 0,
      };
    },
    async readCurrentSession(this: void, key = session.sessionKey, agentId = session.agentId) {
      const cfg = getRuntimeConfig();
      const assertRoutingCurrent = captureSessionMutationRouting(cfg);
      return await loadGatewaySessionEntryReadOnlyInWorker({
        ...session.sessionLoadOptions,
        cfg,
        key,
        excludeInternalEffects: true,
        agentId,
        assertActive: () => assertRoutingCurrent(getRuntimeConfig()),
      });
    },
  };
}
