# Geppetto

<p align="center">
   <img src="Geppetto.png" alt="Illustration of Geppetto" width="220">
</p>

A Rust Discord bot for live voice conversations with GPT. Join a server voice
channel, run `/join`, and speak. The bot streams your voice to OpenAI Live or Realtime
and streams its spoken response back into Discord. You can interrupt it while
it speaks, and the conversation retains context until reset or disconnected.

This uses the **paid OpenAI or Azure OpenAI API**, not your ChatGPT app subscription. It provides
a similar speech-to-speech experience, not a connection to the ChatGPT app or
its saved chats, memory, browsing, or tools.

## Setup

1. Install stable Rust with [rustup](https://rustup.rs/), CMake, and a C/C++ build
   toolchain. Songbird builds native voice dependencies. On macOS, install the
   Xcode command-line tools (`xcode-select --install`) and CMake
   (`brew install cmake`). On Debian/Ubuntu, install `build-essential cmake pkg-config`.
2. Create an application in the [Discord Developer Portal](https://discord.com/developers/applications).
   Open **Bot** and create/reset its bot token. Keep it private.
3. Under **OAuth2 > URL Generator**, select `bot` and `applications.commands`.
   Grant **View Channels**, **Send Messages**, **Connect**, and **Speak**, then
   use the generated URL to invite the bot to your server. Channel permission
   overrides must also permit these actions. No privileged intents or
   Message Content intent are needed.
4. For Azure OpenAI, deploy a conversational Realtime model in a supported region
   and retrieve the resource endpoint, deployment name, and resource API key from
   Azure/Foundry. See **Azure OpenAI** below. Alternatively, create an
   [OpenAI API key](https://platform.openai.com/api-keys), enable API billing, and
   ensure your project has access to the configured Realtime model.
5. Create your local configuration with `cp .env.example .env` and fill in
   `DISCORD_TOKEN` and your chosen provider's configuration directly in your editor.
   Do not share credentials. Optionally set `DISCORD_GUILD_ID` to your server ID for
   immediate slash-command registration. Enable Discord Developer Mode to copy
   the ID. Without it, global command propagation can take time.
6. Run:

   ```sh
   cargo run --release
   ```

The bot logs when it is ready. Join a normal server voice channel and run `/join`
in a text channel the bot can access. Keep the Rust process running. Use
headphones to prevent your microphone feeding the bot's voice back to it.

## Commands

| Command | Action |
| --- | --- |
| `/join` | Start listening to the invoking user's microphone in their voice channel. |
| `/reset` | Start a fresh OpenAI conversation; only the conversation owner can do this. |
| `/leave` | Stop listening and disconnect; available to the owner or a user with Manage Server. |

Only one conversation is active per server. Separate servers use separate
sessions. The bot leaves when the owner leaves or moves channels, after five
minutes without detectable speech, or after 55 minutes to stay below OpenAI's
session lifetime. API or connection failures stop the session and post a notice;
use `/join` to reconnect. Reconnection and `/reset` start with fresh context.

## Privacy And Costs

- Only the `/join` user's audio is forwarded. Other participants can hear the
  response but their microphones are not sent to OpenAI. There is no group mode.
- The bot posts a visible AI/listening notice before joining and receiving audio.
  Inform everyone in the call and obtain any consent required in your jurisdiction.
- Audio is buffered in memory, not recorded to disk. The bot does not log audio,
  transcripts, or credentials. Your chosen provider's data handling policies apply;
  Azure mode sends audio to your Azure resource, not the public OpenAI endpoint.
- Active conversations incur API usage charges. Set project spending controls in
   your provider's dashboard and use `/leave` when finished. Buffers are bounded; an
  overloaded input queue or excessive playback backlog ends the session.
- Keep `.env` private. `RUST_LOG` defaults to restrained logging; verbose dependency
  logging may expose additional connection metadata.

## Configuration

### Azure OpenAI Live (gpt-live-1)

Deploy `gpt-live-1` and a Responses API backend model in your Azure resource,
then set their actual deployment names:

```dotenv
VOICE_API=live
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com
AZURE_OPENAI_API_KEY=your-resource-key
AZURE_OPENAI_DEPLOYMENT_NAME=your-gpt-live-1-deployment
AZURE_OPENAI_LIVE_BACKEND_DEPLOYMENT_NAME=your-responses-deployment
OPENAI_VOICE=ash
```

Live connects to your Azure resource's `/openai/v1/live/sessions` endpoint
using the `api-key` header. Both deployment names are sent in `session.start`,
not URL query parameters. The backend deployment must support the Responses API
(for example, a deployment of `gpt-6-luna`). Direct OpenAI model settings and its
key are ignored when the Azure endpoint is set. The bot finalizes Live sessions
before disconnecting. Azure Live has local protocol tests but has not been
verified against the provider with real credentials. Run the session check below
before joining Discord.

See [Microsoft's GPT-Live WebSocket guide](https://learn.microsoft.com/en-us/azure/foundry/openai/how-to/gpt-live)
and [Responses delegation guide](https://learn.microsoft.com/en-us/azure/foundry/openai/how-to/gpt-live-delegation).

Set `VOICE_API=realtime` (the default when unset) to retain the existing direct
OpenAI or Azure Realtime integration. Live and Realtime use separate session
and audio event protocols; do not put `gpt-live-1` in `OPENAI_REALTIME_MODEL`.

### Azure OpenAI

Set these values in your local configuration:

```dotenv
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com
AZURE_OPENAI_API_KEY=your-resource-key
AZURE_OPENAI_DEPLOYMENT_NAME=your-voice-deployment
```

Use your actual **deployment name**, not necessarily the underlying model ID.
Deploy a conversational speech-to-speech Realtime model such as `gpt-realtime-2.1`
if available in your Azure region. Chat-only, transcription, translation, and
`gpt-live-1` deployments are not compatible with the Realtime conversation
protocol; use `VOICE_API=live` and the configuration above for `gpt-live-1`.
Public OpenAI model availability and pricing do not establish Azure
availability or pricing; check your Foundry deployment and Azure pricing.

A nonempty `AZURE_OPENAI_ENDPOINT` enables Azure mode for either voice protocol.
Direct OpenAI keys and model settings are then ignored, and the Azure key and deployment are
required. The endpoint must be the HTTPS resource root or its `/openai/v1` base
URL, not a chat-completions or deployment-specific URL. The bot connects to:

```text
wss://your-resource.openai.azure.com/openai/v1/realtime?model=your-voice-deployment
```

Authentication uses an `api-key` header; the key is never placed in the URL.
This integration uses the **GA v1 API**, without an `api-version` query parameter.
Legacy preview protocols and Microsoft Entra ID/managed identity authentication
are not implemented. The runtime must have network access to your Azure resource,
including any private endpoint or firewall restrictions.

See [Microsoft's Azure Realtime WebSocket guide](https://learn.microsoft.com/en-us/azure/foundry/openai/how-to/realtime-audio-websockets).

### Direct OpenAI And Shared Settings

Set `VOICE_API=realtime` and leave `AZURE_OPENAI_ENDPOINT` empty to use direct
OpenAI Realtime.
`OPENAI_REALTIME_MODEL` defaults to `gpt-realtime-2.1`; select another compatible
Realtime model your API project can access. `OPENAI_VOICE` defaults to `ash`.
`SYSTEM_PROMPT` controls the assistant's speaking style. Environment variables
override values loaded from `.env`. Credentials are validated on startup.

For direct OpenAI Live, leave the Azure endpoint empty and set `VOICE_API=live`,
`OPENAI_API_KEY`, `OPENAI_LIVE_MODEL` (default `gpt-live-1`), and
`OPENAI_LIVE_BACKEND_MODEL` (default `gpt-6-luna`). These model settings are
only used without an Azure endpoint.

The bot supports server voice channels, not direct-message or group-DM calls.
Speaker overlap, loud rooms, and microphone echo affect turn detection.
Interruption truncation uses Songbird's playback position, so network and device
buffering can make the estimate differ slightly from what listeners actually hear.

## Backend

Serenity handles Discord slash commands and gateway state. Songbird 0.6 handles
Discord voice, including DAVE encryption and Opus decoding/encoding. A bounded
receive queue forwards only the owner's audio to a per-server worker. Rubato
converts Discord's 48 kHz stereo PCM to OpenAI's 24 kHz mono PCM16. Silence frames
continue between Discord speaking packets so server-side voice activity detection
can detect the end of a turn.

OpenAI Realtime runs over an authenticated TLS WebSocket. Audio deltas feed a
bounded live PCM source played by Songbird. On interruption the bot stops the
track, discards queued audio, and truncates the assistant item to its played
duration. Ctrl+C closes sessions and disconnects the Discord gateway.

## Checks

To check provider authentication and session settings without joining Discord or
sending microphone audio, run:

```sh
cargo run --example check_voice
```

The check opens and immediately closes the selected provider session, waiting
for finalization in Live mode; provider billing rules
still apply. Rejected settings report the provider's error code, parameter, and
message with configured credentials redacted. Available voices depend on your
deployment; `spruce` was rejected by the tested Azure `gpt-realtime-2.1-mini`
deployment, while `ash` is supported. Restart the bot after changing configuration.

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

Tests cover PCM conversion, resampling, stream shutdown, live Songbird decoding,
speaker-ID filtering, Azure endpoint validation and authentication headers,
deployment query encoding, and the Live and Realtime session payloads. They do not require
credentials. Live Discord/OpenAI operation requires an end-to-end call with real
credentials; compilation and local tests alone cannot validate network access,
server permissions, model availability, or audible call quality.