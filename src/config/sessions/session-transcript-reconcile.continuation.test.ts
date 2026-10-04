import { isRecord } from "@openclaw/normalization-core/record-coerce";
import { afterEach, expect, it, vi } from "vitest";
import { useAutoCleanupTempDirTracker } from "../../../test/helpers/temp-dir.js";
import type {
  SqliteWorkerOperations,
  SqliteWorkerStore,
} from "../../infra/sqlite-worker-contract.js";
import * as workerStore from "../../infra/sqlite-worker-store.js";
import {
  closeOpenClawAgentDatabasesAsync,
  openOpenClawAgentDatabase,
} from "../../state/openclaw-agent-db.js";
import { closeOpenClawStateDatabaseAsync } from "../../state/openclaw-state-db.js";
import { persistSessionTranscriptTurn } from "./session-accessor.transcript-turn.js";
import { closeSessionTranscriptReconcileWorkerPool } from "./session-transcript-reconcile-pool.js";
import {
  reconcileSessionTranscriptIndexes,
  waitForSessionTranscriptIndexReconcile,
} from "./session-transcript-reconcile.js";
import { useReconcileWorkerObserver } from "./session-transcript-reconcile.test-support.js";

vi.mock("node:worker_threads", async () =>
  (await import("./session-transcript-reconcile.test-support.js")).createObservedWorkerThreads(),
);
const observer = useReconcileWorkerObserver();
const tempDirs = useAutoCleanupTempDirTracker(afterEach);

afterEach(async () => {
  observer.onTask = undefined;
  await closeSessionTranscriptReconcileWorkerPool();
  await closeOpenClawAgentDatabasesAsync();
  await closeOpenClawStateDatabaseAsync();
});

it.each(["continues", "redirtied", "retires", "retires-between-batches"] as const)(
  "preserves committed reconciliation when newly pending work %s",
  async (continuation) => {
    const retires = continuation === "retires" || continuation === "retires-between-batches";
    const continuedSession = continuation === "redirtied" ? "first" : "second";
    const stateDir = tempDirs.make("transcript-reconcile-continuation-");
    const options = { agentId: "main", env: { ...process.env, OPENCLAW_STATE_DIR: stateDir } };
    for (const sessionId of ["first", "second"]) {
      await persistSessionTranscriptTurn(
        { ...options, sessionId, sessionKey: `agent:main:${sessionId}` },
        {
          messages: [
            {
              eventId: `${sessionId}-message`,
              message: { role: "user", content: `${sessionId} continuation transcript` },
            },
          ],
          touchSessionEntry: false,
        },
      );
    }
    await waitForSessionTranscriptIndexReconcile(options);
    const database = openOpenClawAgentDatabase(options);
    const dirty = database.db.prepare(
      "UPDATE session_transcript_index_state SET needs_rebuild = 1 WHERE session_id = ?",
    );
    dirty.run("first");
    const worklists: string[][] = [];
    observer.onTask = ({ input }) => {
      if (input.mode !== "disk") {
        return;
      }
      worklists.push([...input.sessionIds]);
      if (worklists.length === 1) {
        expect(input.sessionIds).toEqual(["first"]);
        if (continuation !== "redirtied") {
          // The real worker has not received its captured plan; this connection is a foreign writer.
          dirty.run("second");
          database.db
            .prepare("DELETE FROM session_transcript_fts_rows WHERE session_id = ?")
            .run("second");
        }
        if (continuation === "retires-between-batches") {
          database.db
            .prepare(`WITH RECURSIVE candidates(n) AS (
              VALUES (0) UNION ALL SELECT n + 1 FROM candidates WHERE n < 199
            ) INSERT INTO session_windows (session_id, session_key, created_at, updated_at)
              SELECT printf('a-%03d', n), ?, 1, 1 FROM candidates`)
            .run("agent:main:first");
        }
      }
    };
    let closing: Promise<void> | undefined;
    let sweepDispatches = 0;
    let sweepResults = 0;
    let dirtiedAgain = false;
    const runOperation = workerStore.runSqliteWorkerStoreOperation;
    const operationSpy =
      continuation !== "continues"
        ? vi
            .spyOn(workerStore, "runSqliteWorkerStoreOperation")
            .mockImplementation(
              <Operations extends SqliteWorkerOperations, T>(
                target: SqliteWorkerStore<Operations>,
                operation: (
                  worker: Pick<SqliteWorkerStore<Operations>, "execute">,
                ) => T | Promise<T>,
                stateContext?: Parameters<typeof runOperation>[2],
                assertCurrent?: Parameters<typeof runOperation>[3],
                createAdmission?: Parameters<typeof runOperation>[4],
              ) =>
                runOperation(
                  target,
                  (worker) =>
                    operation({
                      execute: async (command, commandOptions) => {
                        const publication =
                          command.type === "database.domain.publish" &&
                          isRecord(command.input) &&
                          isRecord(command.input.command)
                            ? command.input.command
                            : undefined;
                        const sweep = publication?.type === "sweep";
                        if (sweep) {
                          sweepDispatches++;
                          if (continuation === "retires-between-batches" && sweepDispatches === 2) {
                            closing = closeOpenClawAgentDatabasesAsync(stateDir);
                          }
                        }
                        const result = await worker.execute(command, commandOptions);
                        if (
                          continuation === "redirtied" &&
                          !dirtiedAgain &&
                          publication?.type === "finalize" &&
                          isRecord(publication.input) &&
                          isRecord(publication.input.plan) &&
                          publication.input.plan.sessionId === "first" &&
                          isRecord(result) &&
                          result.finalized === true
                        ) {
                          dirtiedAgain = true;
                          dirty.run("first");
                          database.db
                            .prepare("DELETE FROM session_transcript_fts_rows WHERE session_id = ?")
                            .run("first");
                        }
                        if (sweep) {
                          sweepResults++;
                          if (continuation === "retires") {
                            expect(result).toMatchObject({
                              sessionIds: ["second"],
                              hasMore: false,
                            });
                            // Revocation is immediate; awaiting close would join this operation.
                            closing = closeOpenClawAgentDatabasesAsync(stateDir);
                          } else if (continuation === "retires-between-batches") {
                            expect(result).toMatchObject({
                              hasMore: true,
                              traversalComplete: false,
                            });
                          }
                        }
                        return result;
                      },
                    }),
                  stateContext,
                  assertCurrent,
                  createAdmission,
                ),
            )
        : undefined;
    try {
      await expect(reconcileSessionTranscriptIndexes(options)).resolves.toEqual({
        reconciledSessions: 1,
      });
      if (retires) {
        expect(closing).toBeDefined();
        await closing;
        expect(sweepDispatches).toBe(continuation === "retires-between-batches" ? 2 : 1);
        expect(sweepResults).toBe(1);
      } else {
        await waitForSessionTranscriptIndexReconcile(options);
      }
      expect(worklists).toEqual(retires ? [["first"]] : [["first"], [continuedSession]]);
      if (continuation === "redirtied") {
        expect(dirtiedAgain).toBe(true);
      }
      const verified = openOpenClawAgentDatabase(options);
      expect(
        verified.db
          .prepare(
            "SELECT session_id, needs_rebuild FROM session_transcript_index_state ORDER BY session_id",
          )
          .all(),
      ).toEqual([
        { session_id: "first", needs_rebuild: 0 },
        { session_id: "second", needs_rebuild: retires ? 1 : 0 },
      ]);
      expect(
        verified.db
          .prepare(
            "SELECT session_id, message_id FROM session_transcript_fts WHERE session_transcript_fts MATCH ?",
          )
          .all(continuedSession),
      ).toEqual(
        retires
          ? []
          : [{ session_id: continuedSession, message_id: `${continuedSession}-message` }],
      );
    } finally {
      await Promise.allSettled([closing]);
      operationSpy?.mockRestore();
    }
  },
);
