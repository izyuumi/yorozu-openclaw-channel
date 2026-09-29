// Hands one Yorozu message to OpenClaw with an abort signal, mirroring the SDK's
// dispatchInboundDirectDm (which cannot pass reply options). `sdk` is injected so tests can fake it.

/**
 * @param {{ resolveRoute: Function, buildContext: Function, createReplyPipeline: Function, dispatchTurn: Function }} sdk
 * @returns {(params: { cfg: object, accountId: string, message: object, deliver: Function, log?: object }, signal: AbortSignal, begin: () => void) => Promise<"completed" | "failed" | undefined>}
 */
export const createInboundDispatcher = (sdk) => async ({ cfg, accountId, message, deliver, log }, signal, begin) => {
  const peer = { kind: "direct", id: message.threadId };
  // Refused here (e.g. no binding with several agents): throws before `begin`, so it is resent.
  const { route, buildEnvelope } = sdk.resolveRoute({ cfg, channel: "yorozu", accountId, peer });
  const label = `Yorozu ${message.threadId}`;
  const ctxPayload = await sdk.buildContext({
    channel: "yorozu",
    accountId: route.accountId ?? accountId,
    messageId: message.id,
    messageIdFull: message.id,
    timestamp: message.ts,
    from: `yorozu:${message.threadId}`,
    sender: { id: "owner", name: label },
    conversation: { kind: "direct", id: peer.id, routePeer: peer, label },
    route: { agentId: route.agentId, accountId: route.accountId, routeSessionKey: route.sessionKey, dispatchSessionKey: route.sessionKey },
    reply: { to: "yorozu:openclaw", originatingTo: `yorozu:${message.threadId}` },
    message: {
      body: buildEnvelope({ channel: "Yorozu", from: label, body: message.text, timestamp: message.ts }),
      bodyForAgent: message.text,
      rawBody: message.text,
      commandBody: message.text,
    },
    access: { commands: { authorized: true } },
    channelIngress: "unsupported",
    extra: { NativeDirectUserId: peer.id, OriginatingChannel: "yorozu" },
  });
  const { onModelSelected, ...replyPipeline } = sdk.createReplyPipeline({
    cfg, agentId: route.agentId, channel: "yorozu", accountId: route.accountId ?? accountId,
  });
  let outcome; // undefined until OpenClaw reports one
  begin();
  const result = await sdk.dispatchTurn({
    cfg,
    channel: "yorozu",
    accountId: route.accountId ?? accountId,
    route: { agentId: route.agentId, sessionKey: route.sessionKey },
    ctxPayload,
    record: { onRecordError: (error) => log?.error?.(`yorozu inbound record failed: ${String(error)}`) },
    delivery: {
      deliver,
      onError: (error) => {
        outcome = "failed";
        log?.error?.(`yorozu inbound dispatch failed: ${String(error)}`);
      },
    },
    replyPipeline,
    replyOptions: {
      onModelSelected,
      abortSignal: signal,
      onAgentRunTerminalOutcome: (terminal) => {
        if (outcome !== "failed") outcome = terminal;
      },
    },
  });
  return result?.dispatched === false ? "failed" : outcome;
};
