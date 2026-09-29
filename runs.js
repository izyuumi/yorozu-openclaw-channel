// Run boundaries (run-boundary-v1): one run per forwarded inbound message, serialized per
// thread. `emit(frame)` returns false when Yorozu is not connected; such frames are journaled
// and replayed by `replay()` after the next hello, so the host never misses a boundary.

/** @param {(frame: object) => boolean} emit */
export function createRuns(emit) {
  const tails = new Map(); // threadId -> tail of that thread's run queue
  const live = new Map(); // messageId -> AbortController, queued or running
  const journal = new Map(); // messageId -> { status? }: runs the host may not have seen end

  const started = (messageId) => {
    journal.set(messageId, {});
    emit({ type: "run_started", messageId });
  };
  const finished = (messageId, status) => {
    if (emit({ type: "run_finished", messageId, status })) journal.delete(messageId);
    else journal.get(messageId).status = status;
  };

  // The real outcome: an abort only counts if the run did not complete anyway.
  const settle = (aborted, outcome, threw) =>
    aborted && outcome !== "completed" ? "aborted" : threw || outcome === "failed" ? "failed" : "completed";

  return {
    /**
     * Queues `execute(signal, begin)` behind the thread's earlier runs. It calls `begin()` once
     * OpenClaw starts on the message (run_started) and returns "completed" | "failed" | undefined.
     * Throwing before `begin()` sends no boundary and rejects, so the host resends the message.
     */
    run(message, execute) {
      const { id, threadId } = message;
      const controller = new AbortController();
      live.set(id, controller);
      const go = async () => {
        try {
          if (controller.signal.aborted) return started(id), finished(id, "aborted");
          let begun = false;
          let outcome;
          let threw = false;
          try {
            outcome = await execute(controller.signal, () => ((begun = true), started(id)));
          } catch (error) {
            if (!begun) throw error;
            threw = true;
          }
          if (begun) finished(id, settle(controller.signal.aborted, outcome, threw));
        } finally {
          live.delete(id);
        }
      };
      const job = (tails.get(threadId) ?? Promise.resolve()).then(go);
      const tail = job.catch(() => {});
      tails.set(threadId, tail);
      tail.then(() => tails.get(threadId) === tail && tails.delete(threadId));
      return job;
    },
    /** Host abort: cancels exactly this message's run. The outcome is reported by `run`. */
    abort(messageId) {
      live.get(messageId)?.abort();
    },
    /** After hello on a new connection: re-announce unfinished runs, then settle finished ones. */
    replay() {
      for (const [messageId, entry] of journal) {
        emit({ type: "run_started", messageId });
        if (entry.status && emit({ type: "run_finished", messageId, status: entry.status })) journal.delete(messageId);
      }
    },
  };
}
