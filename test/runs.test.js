import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { createInboundDispatcher } from "../dispatch.js";
import { createRuns } from "../runs.js";
import { connectYorozu } from "../socket.js";

/** A fake Yorozu host: records frames across connections, lets the test write back. */
function fakeHost(path) {
  const frames = [];
  let client;
  const server = createServer((socket) => {
    client = socket;
    socket.setEncoding("utf8");
    let buffer = "";
    socket.on("data", (chunk) => {
      buffer += chunk;
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";
      for (const line of lines) if (line) frames.push(JSON.parse(line));
    });
  });
  server.listen(path);
  return {
    frames,
    /** Run-boundary and hello frames only, as `type:messageId:status`. */
    boundaries: () => frames.filter((f) => f.type !== "ack").map((f) => [f.type, f.messageId, f.status].filter(Boolean).join(":")),
    write: (frame) => client.write(`${JSON.stringify(frame)}\n`),
    drop: () => client?.destroy(),
    close: () => new Promise((done) => { client?.destroy(); server.close(() => done()); }),
  };
}

const until = async (check) => {
  for (let i = 0; i < 200 && !check(); i++) await new Promise((r) => setTimeout(r, 10));
  assert.ok(check());
};

/**
 * Wires socket + runs + dispatcher like channel.js, over a fake OpenClaw SDK.
 * `turn(plan, { message, delivered })` plays the OpenClaw run; the default replies once.
 */
async function setup({ turn, route } = {}) {
  const path = join(mkdtempSync(join(tmpdir(), "yorozu-runs-")), "channel.sock");
  const host = fakeHost(path);
  const routed = [];
  const sdk = {
    resolveRoute: ({ peer }) => {
      routed.push(peer.id);
      if (route) return route(peer);
      return { route: { agentId: "ops", sessionKey: `agent:ops:yorozu:direct:${peer.id}` }, buildEnvelope: ({ body }) => body };
    },
    buildContext: (ctx) => ctx,
    createReplyPipeline: () => ({ onModelSelected() {} }),
    dispatchTurn: async (plan) => {
      const message = { id: plan.ctxPayload.messageId, threadId: plan.ctxPayload.conversation.id };
      if (turn) return turn(plan, message);
      await plan.delivery.deliver({ text: "reply" });
      return { dispatched: true };
    },
  };
  const dispatch = createInboundDispatcher(sdk);
  const delivered = [];
  const runs = createRuns((frame) => link.send(frame));
  const link = connectYorozu({
    path,
    retryMs: 20,
    capabilities: ["run-boundary-v1"],
    onOpen: () => runs.replay(),
    onAbort: (id) => runs.abort(id),
    onInbound: (message) =>
      runs.run(message, (signal, begin) =>
        dispatch({ cfg: {}, accountId: "default", message, deliver: async (p) => void delivered.push([message.id, p.text]) }, signal, begin)),
  });
  await until(() => link.connected);
  return { host, link, delivered, routed, close: async () => { link.close(); await host.close(); } };
}

const inbound = (id, threadId = "t1") => ({ type: "inbound", message: { id, threadId, ts: 1, text: "hi" } });

test("hello is the first frame and announces only run-boundary-v1", async () => {
  const { host, close } = await setup();
  await until(() => host.frames.length >= 1);
  assert.deepEqual(host.frames[0], { type: "hello", capabilities: ["run-boundary-v1"] });
  await close();
});

test("one run spans several replies and ends once; a run with no reply still ends", async () => {
  const { host, delivered, routed, close } = await setup({
    turn: async (plan, { id }) => {
      if (id === "u1") { await plan.delivery.deliver({ text: "a" }); await plan.delivery.deliver({ text: "b" }); }
      return { dispatched: true };
    },
  });
  host.write(inbound("u1"));
  await until(() => host.frames.some((f) => f.type === "ack" && f.id === "u1"));
  host.write(inbound("u2"));
  await until(() => host.frames.some((f) => f.type === "ack" && f.id === "u2"));
  assert.deepEqual(host.boundaries(), ["hello", "run_started:u1", "run_finished:u1:completed", "run_started:u2", "run_finished:u2:completed"]);
  assert.deepEqual(delivered, [["u1", "a"], ["u1", "b"]]);
  assert.deepEqual(routed, ["t1", "t1"]);
  await close();
});

test("runs in one thread are serialized; other threads run alongside", async () => {
  const gates = {};
  const { host, close } = await setup({
    turn: (plan, { id }) => new Promise((resolve) => { gates[id] = () => resolve({ dispatched: true }); }),
  });
  host.write(inbound("a1", "ta"));
  host.write(inbound("a2", "ta"));
  host.write(inbound("b1", "tb"));
  await until(() => gates.a1 && gates.b1);
  await new Promise((r) => setTimeout(r, 30));
  assert.equal(gates.a2, undefined);
  assert.deepEqual(host.boundaries(), ["hello", "run_started:a1", "run_started:b1"]);
  gates.a1();
  await until(() => gates.a2);
  assert.deepEqual(host.boundaries().slice(3), ["run_finished:a1:completed", "run_started:a2"]);
  gates.a2();
  gates.b1();
  await until(() => host.boundaries().length === 7);
  await close();
});

test("abort cancels exactly that run and reports aborted", async () => {
  const signals = {};
  const { host, close } = await setup({
    turn: (plan, { id }) => new Promise((resolve, reject) => {
      const signal = plan.replyOptions.abortSignal;
      signals[id] = signal;
      signal.addEventListener("abort", () => reject(new Error("Reply canceled")));
      if (id === "keep") setTimeout(() => resolve({ dispatched: true }), 100);
    }),
  });
  host.write(inbound("stop", "t1"));
  host.write(inbound("keep", "t2"));
  await until(() => host.boundaries().length === 3);
  host.write({ type: "abort", messageId: "stop" });
  await until(() => host.boundaries().includes("run_finished:stop:aborted"));
  assert.equal(signals.keep.aborted, false);
  await until(() => host.boundaries().includes("run_finished:keep:completed"));
  host.write({ type: "abort", messageId: "keep" }); // already finished: ignored
  host.write({ type: "abort", messageId: "unknown" });
  await new Promise((r) => setTimeout(r, 30));
  assert.equal(host.boundaries().length, 5);
  await close();
});

test("abort of a queued run skips OpenClaw but still closes the run", async () => {
  let release;
  const { host, routed, close } = await setup({ turn: () => new Promise((resolve) => { release = () => resolve({ dispatched: true }); }) });
  host.write(inbound("first"));
  host.write(inbound("second"));
  await until(() => release);
  host.write({ type: "abort", messageId: "second" });
  await new Promise((r) => setTimeout(r, 30));
  release();
  await until(() => host.boundaries().length === 5);
  assert.deepEqual(host.boundaries(), ["hello", "run_started:first", "run_finished:first:completed", "run_started:second", "run_finished:second:aborted"]);
  assert.deepEqual(routed, ["t1"]);
  await close();
});

test("an abort that lost the race to completion reports completed", async () => {
  const { host, close } = await setup({
    turn: async (plan) => { plan.replyOptions.onAgentRunTerminalOutcome("completed"); host.write({ type: "abort", messageId: "u1" }); await new Promise((r) => setTimeout(r, 30)); },
  });
  host.write(inbound("u1"));
  await until(() => host.boundaries().length === 3);
  assert.equal(host.boundaries()[2], "run_finished:u1:completed");
  await close();
});

test("failures report failed; a refusal before the run starts sends no boundary and no ack", async () => {
  const { host, close } = await setup({
    turn: async (plan, { id }) => {
      if (id === "bad-outcome") plan.replyOptions.onAgentRunTerminalOutcome("failed");
      if (id === "bad-delivery") plan.delivery.onError(new Error("send failed"));
      if (id === "bad-throw") throw new Error("boom");
      return { dispatched: true };
    },
  });
  for (const id of ["bad-outcome", "bad-delivery", "bad-throw"]) host.write(inbound(id));
  await until(() => host.frames.filter((f) => f.type === "ack").length === 3);
  assert.deepEqual(host.boundaries().filter((b) => b.startsWith("run_finished")), [
    "run_finished:bad-outcome:failed", "run_finished:bad-delivery:failed", "run_finished:bad-throw:failed",
  ]);
  await close();

  const refused = await setup({ route: () => { throw new Error("no binding"); } });
  refused.host.write(inbound("nobind"));
  await new Promise((r) => setTimeout(r, 50));
  assert.deepEqual(refused.host.frames, [{ type: "hello", capabilities: ["run-boundary-v1"] }]);
  await refused.close();
});

test("reconnect re-announces an unfinished run after hello; a run that ended offline is replayed", async () => {
  let release;
  const { host, link, close } = await setup({
    turn: (plan, { id }) => id === "long" ? new Promise((resolve) => { release = () => resolve({ dispatched: true }); }) : { dispatched: true },
  });
  host.write(inbound("long"));
  await until(() => host.boundaries().length === 2);

  host.drop();
  await until(() => !link.connected);
  await until(() => link.connected);
  await until(() => host.boundaries().length === 4);
  assert.deepEqual(host.boundaries().slice(2), ["hello", "run_started:long"]);

  host.drop();
  await until(() => !link.connected);
  release(); // ends while offline
  await until(() => link.connected);
  await until(() => host.boundaries().length === 7);
  assert.deepEqual(host.boundaries().slice(4), ["hello", "run_started:long", "run_finished:long:completed"]);
  await close();
});
