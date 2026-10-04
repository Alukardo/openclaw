import { expect, it, vi } from "vitest";
import { createDeferred, withinTest } from "../../test/helpers/promise.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { withOpenClawTestState } from "../test-utils/openclaw-test-state.js";
import * as observerWork from "./session-observer-work.js";
import {
  createHarness,
  event,
  modelMessage,
  preparedModel,
  resetSessionObserverEventSequence,
} from "./session-observer.test-utils.js";

it.for(["model", "synthesized terminal"] as const)(
  "rechecks physical source authority in the consuming frame for a %s digest",
  async (kind, { signal }) => {
    await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
      const config: OpenClawConfig = {
        session: { store: state.path("original.sqlite") },
        gateway: { controlUi: { sessionObserver: true } },
        agents: { defaults: { utilityModel: "openai/gpt-test" } },
      };
      const replacementStore = state.path("replacement.sqlite");
      let changeSourceOnRead = false;
      let sourceChanges = 0;
      let backgroundRegistrations = 0;
      let backgroundFinished = createDeferred();
      const createWork = observerWork.createSessionObserverWork;
      const factory = vi
        .spyOn(observerWork, "createSessionObserverWork")
        .mockImplementation((params) => {
          const work = createWork(params);
          const readCurrent = work.readCurrent.bind(work);
          vi.spyOn(work, "readCurrent").mockImplementation(async (...args) => {
            const session = await readCurrent(...args);
            if (changeSourceOnRead) {
              changeSourceOnRead = false;
              queueMicrotask(() => {
                config.session = { store: replacementStore };
                sourceChanges += 1;
              });
            }
            return session;
          });
          const background = work.background.bind(work);
          vi.spyOn(work, "background").mockImplementation((run) => {
            backgroundRegistrations += 1;
            const finished = backgroundFinished;
            background(async () => {
              try {
                return await run();
              } finally {
                finished.resolve();
              }
            });
          });
          return work;
        });
      let harness: ReturnType<typeof createHarness> | undefined;
      try {
        harness = createHarness({
          config,
          utilityModelRef: kind === "model" ? "openai/gpt-test" : null,
          readSession: vi.fn(() => ({
            sessionId: "session-id",
            lifecycleRevision: "owned",
            updatedAt: 1,
          })),
          prepareModel: vi.fn(async () => preparedModel()),
          completeModel: vi.fn(async () =>
            modelMessage({ headline: "Finished reviewing", health: "done" }),
          ),
        });
        expect(factory).toHaveBeenCalledOnce();
        if (kind === "model") {
          await withinTest(
            harness.observer.handleEventAsync(
              event({
                stream: "lifecycle",
                data: { phase: "start", startedAt: 0 },
              }),
            ),
            signal,
          );
          await withinTest(
            harness.observer.handleEventAsync(
              event({
                stream: "tool",
                data: { phase: "start", name: "read", args: { path: "example.ts" } },
              }),
            ),
            signal,
          );
        } else {
          await withinTest(
            harness.observer.handleEventAsync(
              event({
                stream: "item",
                data: { kind: "preamble", progressText: "Reviewing", startedAt: 0 },
              }),
            ),
            signal,
          );
          expect(backgroundRegistrations).toBe(1);
          await withinTest(backgroundFinished.promise, signal);
          expect(harness.broadcastToConnIds).toHaveBeenCalledOnce();
          expect(harness.persistDigest).toHaveBeenCalledOnce();
          harness.broadcastToConnIds.mockClear();
          harness.persistDigest.mockClear();
          backgroundFinished = createDeferred();
          backgroundRegistrations = 0;
        }
        expect(backgroundRegistrations).toBe(0);
        changeSourceOnRead = true;
        await withinTest(
          harness.observer.handleEventAsync(
            event({
              stream: "lifecycle",
              data: { phase: "end", startedAt: 0, endedAt: 31_000 },
            }),
          ),
          signal,
        );
        expect(backgroundRegistrations).toBeGreaterThan(0);
        await withinTest(backgroundFinished.promise, signal);

        expect(sourceChanges).toBe(1);
        expect(config.session?.store).toBe(replacementStore);
        expect(harness.broadcastToConnIds).not.toHaveBeenCalled();
        if (kind === "model") {
          expect(harness.completeModel).toHaveBeenCalledOnce();
          expect(harness.persistDigest).not.toHaveBeenCalled();
        } else {
          expect(harness.completeModel).not.toHaveBeenCalled();
          expect(harness.persistDigest).toHaveBeenCalledOnce();
          expect(harness.persistDigest.mock.calls[0]?.[0].digest).toMatchObject({ health: "done" });
        }
      } finally {
        try {
          await harness?.observer.disposeAsync();
        } finally {
          factory.mockRestore();
          vi.restoreAllMocks();
          resetSessionObserverEventSequence();
        }
      }
    });
  },
);
