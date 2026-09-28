// Client for Yorozu's channel.sock (packages/runtime/src/channel.ts in the Yorozu repo).
// Newline-delimited JSON. Both directions are acked by id; the host resends unacked
// inbound messages on every connect, so inbound ids are deduped here.
//
// Host frames:   { type: "inbound", message: { id, threadId, ts, text } }
//                { type: "ack", id } | { type: "error", id, reason }
// Plugin frames: { type: "deliver", id, threadId, text } | { type: "ack", id }
import { randomUUID } from "node:crypto";
import { createConnection } from "node:net";

/**
 * @param {{
 *   path: string,
 *   onInbound: (message: { id: string, threadId: string, ts: number, text: string }) => Promise<void>,
 *   onStatus?: (connected: boolean) => void,
 *   onError?: (message: string) => void,
 *   retryMs?: number,
 *   ackTimeoutMs?: number,
 * }} options `onInbound` resolves once OpenClaw has taken the message; only then is it acked.
 */
export function connectYorozu(options) {
  const retryMs = options.retryMs ?? 2000;
  const ackTimeoutMs = options.ackTimeoutMs ?? 10_000;
  const waiting = new Map();
  const seen = new Set();
  let socket;
  let connected = false;
  let closed = false;
  let timer;

  const write = (frame) => socket?.write(`${JSON.stringify(frame)}\n`);

  const handle = (frame) => {
    if (frame.type === "inbound") {
      const { message } = frame;
      if (seen.has(message.id)) return void write({ type: "ack", id: message.id });
      seen.add(message.id);
      options.onInbound(message).then(
        () => write({ type: "ack", id: message.id }),
        (error) => {
          // Not acked: the host resends it on the next connect.
          seen.delete(message.id);
          options.onError?.(`inbound ${message.id} failed: ${String(error)}`);
        },
      );
      return;
    }
    const pending = waiting.get(frame.id);
    if (!pending) return;
    waiting.delete(frame.id);
    if (frame.type === "ack") pending.resolve();
    else pending.reject(new Error(`yorozu refused: ${frame.reason}`));
  };

  const open = () => {
    if (closed) return;
    const next = createConnection(options.path);
    socket = next;
    let buffer = "";
    next.setEncoding("utf8");
    next.on("connect", () => {
      connected = true;
      options.onStatus?.(true);
    });
    next.on("data", (chunk) => {
      buffer += chunk;
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";
      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          handle(JSON.parse(line));
        } catch (error) {
          options.onError?.(`bad frame: ${String(error)}`);
        }
      }
    });
    next.on("error", (error) => options.onError?.(error.message));
    next.on("close", () => {
      if (connected) options.onStatus?.(false);
      connected = false;
      for (const [id, pending] of waiting) {
        waiting.delete(id);
        pending.reject(new Error("yorozu disconnected"));
      }
      if (!closed) timer = setTimeout(open, retryMs);
    });
  };
  open();

  return {
    get connected() {
      return connected;
    },
    /** Resolves with the message id once Yorozu has logged it. */
    deliver(threadId, text) {
      if (!connected) return Promise.reject(new Error("yorozu is not running"));
      const id = randomUUID();
      return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
          waiting.delete(id);
          reject(new Error("yorozu did not ack"));
        }, ackTimeoutMs);
        waiting.set(id, {
          resolve: () => (clearTimeout(timeout), resolve(id)),
          reject: (error) => (clearTimeout(timeout), reject(error)),
        });
        write({ type: "deliver", id, threadId, text });
      });
    },
    close() {
      closed = true;
      clearTimeout(timer);
      socket?.destroy();
    },
  };
}
