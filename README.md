# Yorozu channel for OpenClaw

Adds [Yorozu](https://yorozu.yumi.to) as an [OpenClaw](https://openclaw.ai) chat channel, like
Signal or Telegram. Each Yorozu thread is its own OpenClaw conversation, and OpenClaw's cron,
heartbeat and `message` tool can post into Yorozu (target `yorozu:<threadId>`).

The plugin runs inside the OpenClaw Gateway and connects to the Yorozu host on the same Mac
through its owner-only socket, `~/Library/Application Support/Yorozu/channel.sock`. It needs no
credentials; the socket's `0600` mode is the access control.

## Install

On the Mac that hosts Yorozu:

```sh
openclaw plugins install git:github.com/izyuumi/yorozu-openclaw-channel --accept-capabilities
openclaw config set channels.yorozu.enabled true
openclaw agents bind --bind yorozu
openclaw gateway restart
```

OpenClaw asks you to confirm a source outside ClawHub; in a non-interactive shell, add
`--force` to the install line. `agents bind` routes Yorozu to OpenClaw's default agent; add `--agent <id>` to pick another.
With several agents and no binding, OpenClaw rejects every Yorozu message.

`openclaw channels status` then lists **Yorozu** as connected while Yorozu is running.

Update with `openclaw plugins update yorozu`, then restart the Gateway.

## Config

| Key | Default |
| --- | --- |
| `channels.yorozu.enabled` | `true` |
| `channels.yorozu.socketPath` | `~/Library/Application Support/Yorozu/channel.sock` |

## Protocol

Newline-delimited JSON over the socket. Both directions are at least once and acked by id.

| Frame | Direction | Meaning |
| --- | --- | --- |
| `inbound` | Yorozu → plugin | A user message. Acked once OpenClaw has dispatched it; resent on every connect until then. |
| `deliver` | plugin → Yorozu | An OpenClaw reply. An unknown thread id opens a new thread. |
| `ack` / `error` | both | Receipt by id. |
| `hello` | plugin → Yorozu | First frame on every connection: `{ capabilities: ["run-boundary-v1"] }`. |
| `run_started` / `run_finished` | plugin → Yorozu | `run-boundary-v1`: one run per `inbound`, from OpenClaw starting on it to its end (`completed`, `failed` or `aborted`), however many replies or none. Runs are serialized per thread, and an unfinished run is re-announced after a reconnect. |
| `abort` | Yorozu → plugin | Cancels exactly that run's OpenClaw turn; the real outcome comes back in `run_finished`. |

### Model selection

`model-select-v1` (the Yorozu model picker) is **not** announced yet. The per-thread route and the
session model override are reachable from a plugin, but the model catalog with OpenClaw's policy and
availability data (`models.list`) is only reachable through `api.runtime.gateway.request`, which
OpenClaw 2026.9.1 limits to bundled or trusted official plugins. A git-installed plugin cannot use it.

Text only for now. The Yorozu side lives in
[`packages/runtime/src/channel.ts`](https://github.com/izyuumi/yorozu/blob/main/packages/runtime/src/channel.ts).

## Develop

```sh
npm test
```
