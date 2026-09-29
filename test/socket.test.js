import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { connectYorozu } from "../socket.js";

/** A fake Yorozu host: records frames (hellos apart), lets the test write back. */
function fakeHost(path) {
  const frames = [];
  const hellos = [];
  let client;
  const server = createServer((socket) => {
    client = socket;
    socket.setEncoding("utf8");
    let buffer = "";
    socket.on("data", (chunk) => {
      buffer += chunk;
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";
      for (const line of lines) {
        if (!line) continue;
        const frame = JSON.parse(line);
        (frame.type === "hello" ? hellos : frames).push(frame);
      }
    });
  });
  server.listen(path);
  return {
    frames,
    hellos,
    write: (frame) => client.write(`${JSON.stringify(frame)}\n`),
    drop: () => client?.destroy(),
    close: () => new Promise((done) => { client?.destroy(); server.close(() => done()); }),
  };
}

const until = async (check) => {
  for (let i = 0; i < 200 && !check(); i++) await new Promise((r) => setTimeout(r, 10));
  assert.ok(check());
};

test("delivers with ack, refuses on error, acks inbound once handled and dedupes resends", async () => {
  const path = join(mkdtempSync(join(tmpdir(), "yorozu-link-")), "channel.sock");
  const host = fakeHost(path);
  const handled = [];
  const link = connectYorozu({ path, retryMs: 20, onInbound: async (m) => void handled.push(m.id) });
  await until(() => link.connected);

  const sent = link.deliver("t1", "hello");
  await until(() => host.frames.length === 1);
  assert.deepEqual({ ...host.frames[0], id: undefined }, { type: "deliver", id: undefined, threadId: "t1", text: "hello" });
  host.write({ type: "ack", id: host.frames[0].id });
  assert.equal(await sent, host.frames[0].id);

  const refused = link.deliver("code", "no");
  await until(() => host.frames.length === 2);
  host.write({ type: "error", id: host.frames[1].id, reason: "not-a-channel-thread" });
  await assert.rejects(refused, /not-a-channel-thread/);

  const inbound = { type: "inbound", message: { id: "u1", threadId: "t1", ts: 1, text: "hi" } };
  host.write(inbound);
  host.write(inbound);
  await until(() => host.frames.filter((f) => f.type === "ack").length === 2);
  assert.deepEqual(handled, ["u1"]);

  host.drop();
  await until(() => !link.connected);
  await assert.rejects(link.deliver("t1", "offline"), /not running/);
  await until(() => link.connected);

  link.close();
  await host.close();
});

test("a failed inbound is not acked, so the host resends it", async () => {
  const path = join(mkdtempSync(join(tmpdir(), "yorozu-link-")), "channel.sock");
  const host = fakeHost(path);
  let fail = true;
  const link = connectYorozu({ path, onInbound: async () => { if (fail) throw new Error("gateway busy"); } });
  await until(() => link.connected);
  host.write({ type: "inbound", message: { id: "u2", threadId: "t1", ts: 1, text: "hi" } });
  await new Promise((r) => setTimeout(r, 50));
  assert.equal(host.frames.length, 0);
  fail = false;
  host.write({ type: "inbound", message: { id: "u2", threadId: "t1", ts: 1, text: "hi" } });
  await until(() => host.frames.length === 1);
  assert.deepEqual(host.frames[0], { type: "ack", id: "u2" });
  link.close();
  await host.close();
});

test("says hello with its capabilities on every connection, before anything else", async () => {
  const path = join(mkdtempSync(join(tmpdir(), "yorozu-link-")), "channel.sock");
  const host = fakeHost(path);
  const link = connectYorozu({ path, retryMs: 20, capabilities: ["run-boundary-v1"], onInbound: async () => {} });
  await until(() => host.hellos.length === 1);
  host.drop();
  await until(() => host.hellos.length === 2);
  assert.deepEqual(host.hellos, Array(2).fill({ type: "hello", capabilities: ["run-boundary-v1"] }));
  assert.deepEqual(host.frames, []);
  link.close();
  await host.close();
});
