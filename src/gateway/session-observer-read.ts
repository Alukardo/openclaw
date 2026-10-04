import { isDeepStrictEqual } from "node:util";
import {
  assertSessionStoreReadCandidate,
  captureSessionStoreCandidateIdentities,
} from "../config/sessions/session-store-read-candidates.js";
import { prepareSessionStoreTargetInventory } from "../config/sessions/session-store-target-inventory.js";
import { readDatabasePathIdentitySync } from "../infra/sqlite-worker-identity.js";
import type { SessionObserverDeps, SessionObserverRead } from "./session-observer-model.js";
import { defaultPersistDigest } from "./session-observer-model.js";
import { captureSessionMutationRouting } from "./session-sharing-preparation.js";
import { loadGatewaySessionEntryReadOnlyInWorker } from "./session-utils-store-worker.js";

/** One observation retains its physical sources; every read still fetches current rows. */
export function captureSessionObserverRead(
  deps: Pick<SessionObserverDeps, "getConfig" | "readSession" | "persistDigest">,
  sessionKey: string,
  agentId: string,
): SessionObserverRead {
  const cfg = deps.getConfig();
  const routing = captureSessionMutationRouting(cfg);
  const inventory = prepareSessionStoreTargetInventory(cfg, [agentId]);
  const identities = captureSessionStoreCandidateIdentities(inventory.candidates);
  let target: Awaited<ReturnType<typeof loadGatewaySessionEntryReadOnlyInWorker>> | undefined;
  const assertCurrent = () => {
    routing(deps.getConfig());
    for (const candidate of inventory.candidates) {
      assertSessionStoreReadCandidate(candidate.path, [candidate]);
    }
    for (const [pathname, expected] of identities) {
      if (!isDeepStrictEqual(readDatabasePathIdentitySync(pathname), expected)) {
        throw new Error("Session observer storage changed during observation");
      }
    }
  };
  const reader: SessionObserverRead = {
    assertCurrent,
    async read() {
      assertCurrent();
      if (deps.readSession) {
        const entry = deps.readSession(sessionKey, agentId);
        assertCurrent();
        return entry;
      }
      const loaded = await loadGatewaySessionEntryReadOnlyInWorker({
        cfg: inventory.config,
        env: inventory.env,
        key: sessionKey,
        agentId,
        assertActive: assertCurrent,
      });
      assertCurrent();
      if (
        target &&
        (!isDeepStrictEqual(target.capturedReadSources, loaded.capturedReadSources) ||
          !isDeepStrictEqual(target.capturedReadSource, loaded.capturedReadSource) ||
          target.canonicalKey !== loaded.canonicalKey)
      ) {
        throw new Error("Session observer source changed during observation");
      }
      // Retain source locators, never a row snapshot for subsequent authority checks.
      target = { ...loaded, entry: undefined, store: {} };
      return loaded.entry;
    },
    async persist(params) {
      assertCurrent();
      if (deps.persistDigest) {
        return deps.persistDigest(params);
      }
      if (!target) {
        // Legacy event admission is synchronous; its asynchronous write still prepares the source.
        await reader.read();
        assertCurrent();
      }
      if (!target) {
        throw new Error("Session observer persistence requires an admitted source");
      }
      return defaultPersistDigest({
        ...params,
        storePath: target.storePath,
        target: {
          agentId: target.agentId,
          storePath: target.storePath,
          readSource: target.capturedReadSource,
          target: { canonicalKey: target.canonicalKey, storeKeys: target.storeKeys },
          env: inventory.env,
        },
        stillCurrent: () => {
          assertCurrent();
          return params.stillCurrent?.() !== false;
        },
      });
    },
  };
  return reader;
}
