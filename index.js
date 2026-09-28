import { defineChannelPluginEntry } from "openclaw/plugin-sdk/channel-core";
import { yorozuPlugin } from "./channel.js";

export default defineChannelPluginEntry({
  id: "yorozu",
  name: "Yorozu",
  description: "Yorozu app as an OpenClaw chat channel, over Yorozu's local channel socket.",
  plugin: yorozuPlugin,
});
