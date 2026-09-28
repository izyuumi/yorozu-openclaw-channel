import { homedir } from "node:os";
import { join } from "node:path";
import {
  buildChannelOutboundSessionRoute,
  createChannelPluginBase,
  createChatChannelPlugin,
} from "openclaw/plugin-sdk/channel-core";
import { dispatchInboundDirectDm } from "openclaw/plugin-sdk/channel-inbound";
import { PlatformMessageNotDispatchedError } from "openclaw/plugin-sdk/error-runtime";
import { connectYorozu } from "./socket.js";

// One Yorozu thread = one OpenClaw direct peer, so each thread gets its own session
// (session.dmScope per-channel-peer). Target: `yorozu:<threadId>`.
// Access control is the socket's 0600 mode: only this Mac's user reaches it, so every
// inbound message is the owner's.

const DEFAULT_SOCKET = join(homedir(), "Library/Application Support/Yorozu/channel.sock");

const section = (cfg) => cfg.channels?.yorozu ?? {};

const resolveAccount = (cfg)=> {
  const s = section(cfg);
  return { accountId: "default", enabled: s.enabled !== false, configured: true, socketPath: s.socketPath ?? DEFAULT_SOCKET };
};

export const normalizeYorozuTarget = (value)=> {
  const id = value.trim().replace(/^yorozu:/i, "").trim();
  return id && id.length <= 128 && !/\s/.test(id) ? id : undefined;
};

let link;

async function send(to, text) {
  const threadId = normalizeYorozuTarget(to);
  if (!threadId) throw new Error("Yorozu target must be yorozu:<threadId>");
  if (!link?.connected) {
    throw new PlatformMessageNotDispatchedError("Yorozu is not running", { cause: undefined, retryable: true });
  }
  try {
    return { messageId: await link.deliver(threadId, text) };
  } catch (cause) {
    throw new PlatformMessageNotDispatchedError(`Yorozu delivery failed: ${String(cause)}`, { cause, retryable: true });
  }
}

export const yorozuPlugin = createChatChannelPlugin({
  base: {
    ...createChannelPluginBase({
      id: "yorozu",
      meta: {
        id: "yorozu",
        label: "Yorozu",
        selectionLabel: "Yorozu",
        blurb: "Chat with OpenClaw from Yorozu on this Mac, iPhone and iPad.",
        docsPath: "/channels/yorozu",
      },
      capabilities: { chatTypes: ["direct"], media: false, reactions: false, threads: false, nativeCommands: false },
      reload: { configPrefixes: ["channels.yorozu"] },
      config: {
        listAccountIds: () => ["default"],
        defaultAccountId: () => "default",
        resolveAccount,
        inspectAccount: (cfg) => {
          const account = resolveAccount(cfg);
          return { accountId: "default", enabled: account.enabled, configured: true };
        },
        isEnabled: (account) => account.enabled,
        isConfigured: () => true,
      },
    }),
    messaging: {
      targetPrefixes: ["yorozu"],
      normalizeTarget: normalizeYorozuTarget,
      inferTargetChatType: () => "direct",
      targetResolver: { looksLikeId: (value) => normalizeYorozuTarget(value) !== undefined, hint: "<yorozu:threadId>" },
      resolveOutboundSessionRoute: (params) => {
        const peer = normalizeYorozuTarget(params.target);
        return peer ? buildChannelOutboundSessionRoute({
          cfg: params.cfg, agentId: params.agentId, channel: "yorozu",
          ...(params.accountId !== undefined ? { accountId: params.accountId } : {}),
          peer: { kind: "direct", id: peer }, chatType: "direct", from: `yorozu:${peer}`, to: `yorozu:${peer}`,
        }) : null;
      },
    },
    gateway: {
      startAccount: async (ctx) => {
        ctx.setStatus({ accountId: ctx.accountId, running: true, connected: false });
        const current = connectYorozu({
          path: ctx.account.socketPath,
          onStatus: (connected) => ctx.setStatus({ accountId: ctx.accountId, running: true, connected }),
          onError: (message) => ctx.log?.warn?.(`yorozu: ${message}`),
          onInbound: async (message) => {
            await dispatchInboundDirectDm({
              channelIngress: "unsupported",
              cfg: ctx.cfg,
              channel: "yorozu",
              channelLabel: "Yorozu",
              accountId: ctx.accountId,
              peer: { kind: "direct", id: message.threadId },
              senderId: "owner",
              senderAddress: `yorozu:${message.threadId}`,
              recipientAddress: "yorozu:openclaw",
              conversationLabel: `Yorozu ${message.threadId}`,
              rawBody: message.text,
              messageId: message.id,
              timestamp: message.ts,
              commandAuthorized: true,
              inboundAccessAuthorized: true,
              deliver: async (payload) => {
                const text = typeof payload?.text === "string" ? payload.text : "";
                if (text.trim()) await send(message.threadId, text);
              },
              onRecordError: (error) => ctx.log?.error?.(`yorozu inbound record failed: ${String(error)}`),
              onDispatchError: (error) => ctx.log?.error?.(`yorozu inbound dispatch failed: ${String(error)}`),
            });
          },
        });
        link = current;
        await new Promise((resolve) => {
          if (ctx.abortSignal.aborted) return resolve();
          ctx.abortSignal.addEventListener("abort", () => resolve(), { once: true });
        });
        current.close();
        if (link === current) link = undefined;
        ctx.setStatus({ accountId: ctx.accountId, running: false, connected: false });
      },
    },
  },
  outbound: {
    base: {
      deliveryMode: "gateway",
      resolveTarget: ({ to }) => {
        const threadId = normalizeYorozuTarget(to ?? "");
        return threadId ? { ok: true, to: threadId } : { ok: false, error: new Error("Yorozu target must be yorozu:<threadId>") };
      },
    },
    attachedResults: { channel: "yorozu", sendText: ({ to, text }) => send(to, text) },
  },
});
