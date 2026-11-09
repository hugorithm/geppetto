use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use songbird::{Call, input::RawAdapter, tracks::TrackHandle};
use tokio::{
    net::TcpStream,
    sync::{Mutex, mpsc},
    time::{Instant, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tokio_util::sync::CancellationToken;

use crate::{
    audio::{DiscordResampler, PlaybackBuffer},
    config::{Config, VoiceApi},
    live,
};

pub(crate) type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub fn session_update(config: &Config) -> Value {
    json!({
        "type": "session.update",
        "session": {
            "type": "realtime",
            "instructions": config.instructions,
            "output_modalities": ["audio"],
            "max_output_tokens": 1024,
            "audio": {
                "input": {
                    "format": {"type": "audio/pcm", "rate": 24000},
                    "turn_detection": {
                        "type": "server_vad", "threshold": 0.5,
                        "prefix_padding_ms": 300, "silence_duration_ms": 650,
                        "create_response": true, "interrupt_response": true
                    }
                },
                "output": {"format": {"type": "audio/pcm", "rate": 24000}, "voice": config.voice}
            }
        }
    })
}

pub async fn connect(config: &Config) -> Result<Socket> {
    connect_inner(config)
        .await
        .map_err(|error| anyhow::anyhow!(safe_connection_error(&error, config)))
}

fn safe_connection_error(error: &anyhow::Error, config: &Config) -> String {
    let mut message = format!("{error:#}");
    for secret in [&config.openai_key, &config.discord_token] {
        if !secret.is_empty() {
            message = message.replace(secret, "[redacted]");
        }
    }
    message
        .chars()
        .filter(|character| !character.is_control())
        .take(1000)
        .collect()
}

async fn connect_inner(config: &Config) -> Result<Socket> {
    let request = connection_request(config)?;
    let (mut socket, _) = timeout(Duration::from_secs(15), connect_async(request))
        .await
        .context("OpenAI connection timed out")?
        .map_err(|error| {
            if let tokio_tungstenite::tungstenite::Error::Http(response) = &error {
                let details = response
                    .body()
                    .as_deref()
                    .and_then(|body| serde_json::from_slice::<Value>(body).ok())
                    .map(|body| format!("{}: {}", body["error"]["code"], body["error"]["message"]))
                    .unwrap_or_else(|| {
                        "Check resource access, authentication, and deployment availability".into()
                    });
                anyhow::anyhow!(
                    "Voice handshake rejected (HTTP {}): {details}",
                    response.status()
                )
            } else {
                anyhow::anyhow!(error)
            }
        })?;
    let (initial, ready) = match config.voice_api {
        VoiceApi::Realtime => (session_update(config), "session.updated"),
        VoiceApi::Live => (
            live::session_start(
                &config.model,
                &config.voice,
                &config.instructions,
                &config.live_backend_model,
            ),
            "session.started",
        ),
    };
    send(&mut socket, initial).await?;
    timeout(Duration::from_secs(15), async {
        while let Some(message) = socket.next().await {
            match message? {
                Message::Text(text) => {
                    let event: Value = serde_json::from_str(&text)?;
                    match event["type"].as_str() {
                        Some(event_type) if event_type == ready => return Ok(()),
                        Some("error") => bail!(
                            "OpenAI rejected session configuration (code {}, parameter {}): {}",
                            event["error"]["code"],
                            event["error"]["param"],
                            event["error"]["message"]
                        ),
                        _ => {}
                    }
                }
                Message::Ping(bytes) => socket.send(Message::Pong(bytes)).await?,
                Message::Close(_) => bail!("OpenAI closed during session setup"),
                _ => {}
            }
        }
        bail!("OpenAI closed before session was ready")
    })
    .await
    .context("OpenAI session setup timed out")??;
    Ok(socket)
}

fn connection_request(
    config: &Config,
) -> Result<tokio_tungstenite::tungstenite::http::Request<()>> {
    anyhow::ensure!(
        !config.model.trim().is_empty(),
        "Voice model or Azure deployment name cannot be empty"
    );
    let mut url = match (config.voice_api, &config.azure_endpoint) {
        (VoiceApi::Live, Some(endpoint)) => {
            let mut url = endpoint.clone();
            url.set_path("/openai/v1/live/sessions");
            url
        }
        (VoiceApi::Live, None) => url::Url::parse("wss://api.openai.com/v1/live/sessions")?,
        (VoiceApi::Realtime, Some(endpoint)) => endpoint.clone(),
        (VoiceApi::Realtime, None) => url::Url::parse("wss://api.openai.com/v1/realtime")?,
    };
    if config.voice_api == VoiceApi::Realtime {
        url.query_pairs_mut().append_pair("model", &config.model);
    }
    let mut request = url.as_str().into_client_request()?;
    if config.azure_endpoint.is_some() {
        request
            .headers_mut()
            .insert("api-key", config.openai_key.parse()?);
    } else {
        request.headers_mut().insert(
            "Authorization",
            format!("Bearer {}", config.openai_key).parse()?,
        );
    }
    Ok(request)
}

pub(crate) async fn send(socket: &mut Socket, event: Value) -> Result<()> {
    timeout(
        Duration::from_secs(5),
        socket.send(Message::Text(event.to_string().into())),
    )
    .await
    .context("OpenAI write timed out")??;
    Ok(())
}

struct Playback {
    buffer: PlaybackBuffer,
    track: TrackHandle,
    item_id: String,
    content_index: u64,
    samples: u64,
}

impl Drop for Playback {
    fn drop(&mut self) {
        self.buffer.finish(true);
        let _ = self.track.stop();
    }
}

pub async fn run(
    mut socket: Socket,
    call: Arc<Mutex<Call>>,
    mut audio: mpsc::Receiver<Vec<i16>>,
    cancel: CancellationToken,
    voice_api: VoiceApi,
) -> Result<()> {
    if voice_api == VoiceApi::Live {
        return live::run(socket, call, audio, cancel).await;
    }
    let mut resampler = DiscordResampler::new()?;
    let mut playback: Option<Playback> = None;
    let mut interrupted_item: Option<String> = None;
    let mut last_speech = Instant::now();
    let deadline = tokio::time::sleep(Duration::from_secs(55 * 60));
    tokio::pin!(deadline);
    let mut housekeeping = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = &mut deadline => break,
            _ = housekeeping.tick() => {
                if last_speech.elapsed() > Duration::from_secs(5 * 60) { break; }
            }
            frame = audio.recv() => {
                let Some(frame) = frame else { break; };
                if frame.iter().any(|sample| i32::from(*sample).abs() > 300) {
                    last_speech = Instant::now();
                }
                let pcm = resampler.convert(&frame)?;
                if !pcm.is_empty() {
                    send(&mut socket, json!({"type": "input_audio_buffer.append", "audio": STANDARD.encode(pcm)})).await?;
                }
            }
            message = socket.next() => {
                let Some(message) = message else { bail!("OpenAI voice connection closed"); };
                match message? {
                    Message::Text(text) => {
                        let event: Value = serde_json::from_str(&text)?;
                        match event["type"].as_str() {
                            Some("input_audio_buffer.speech_started") => {
                                tracing::info!("Voice provider detected caller speech");
                                if let Some(current) = playback.take() {
                                    let played = timeout(Duration::from_millis(250), current.track.get_info()).await
                                        .ok().and_then(Result::ok)
                                        .map(|info| info.position.as_millis() as u64).unwrap_or(current.samples / 24)
                                        .min(current.samples / 24);
                                    current.buffer.finish(true);
                                    let _ = current.track.stop();
                                    interrupted_item = Some(current.item_id.clone());
                                    if played < current.samples / 24 {
                                        send(&mut socket, json!({
                                            "type": "conversation.item.truncate", "item_id": current.item_id,
                                            "content_index": current.content_index, "audio_end_ms": played
                                        })).await?;
                                    }
                                }
                            }
                            Some("response.output_audio.delta") => {
                                let item_id = event["item_id"].as_str().context("Missing audio item ID")?;
                                if interrupted_item.as_deref() == Some(item_id) { continue; }
                                let pcm = STANDARD.decode(event["delta"].as_str().context("Missing audio delta")?)?;
                                if playback.as_ref().is_none_or(|current| current.item_id != item_id) {
                                    let buffer = PlaybackBuffer::default();
                                    buffer.append_pcm16(&pcm)?;
                                    let track = call.lock().await.play_input(RawAdapter::new(buffer.clone(), 24_000, 1).into());
                                    tracing::info!("Voice response received; Discord playback requested");
                                    playback = Some(Playback {
                                        buffer, track, item_id: item_id.into(), samples: (pcm.len() / 2) as u64,
                                        content_index: event["content_index"].as_u64().unwrap_or(0),
                                    });
                                } else if let Some(current) = &mut playback {
                                    current.buffer.append_pcm16(&pcm)?;
                                    current.samples += (pcm.len() / 2) as u64;
                                }
                            }
                            Some("response.output_audio.done") => {
                                if let Some(current) = &playback
                                    && event["item_id"].as_str() == Some(&current.item_id) {
                                    current.buffer.finish(false);
                                }
                            }
                            Some("response.done") if event["response"]["status"] == "failed" => {
                                bail!("OpenAI response failed");
                            }
                            Some("error") => bail!("OpenAI realtime error: {}", event["error"]["code"]),
                            _ => {}
                        }
                    }
                    Message::Ping(bytes) => socket.send(Message::Pong(bytes)).await?,
                    Message::Close(_) => bail!("OpenAI closed the session"),
                    _ => {}
                }
            }
        }
    }
    drop(playback);
    let _ = timeout(Duration::from_secs(2), socket.close(None)).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_uses_matching_pcm_formats_and_automatic_interruptions() {
        let config = Config {
            discord_token: String::new(),
            openai_key: String::new(),
            model: "gpt-realtime".into(),
            voice_api: VoiceApi::Realtime,
            live_backend_model: "gpt-6-luna".into(),
            azure_endpoint: None,
            voice: "marin".into(),
            instructions: "Be concise".into(),
            guild_id: None,
        };
        let event = session_update(&config);
        assert_eq!(event["session"]["audio"]["input"]["format"]["rate"], 24000);
        assert_eq!(event["session"]["audio"]["output"]["format"]["rate"], 24000);
        assert_eq!(
            event["session"]["audio"]["input"]["turn_detection"]["interrupt_response"],
            true
        );
    }

    fn connection_config(azure: bool) -> Config {
        Config {
            discord_token: String::new(),
            openai_key: "test-key".into(),
            model: "my-voice-deployment".into(),
            voice_api: VoiceApi::Realtime,
            live_backend_model: "gpt-6-luna".into(),
            azure_endpoint: azure.then(|| {
                url::Url::parse("wss://example.openai.azure.com/openai/v1/realtime").unwrap()
            }),
            voice: "marin".into(),
            instructions: String::new(),
            guild_id: None,
        }
    }

    #[test]
    fn azure_request_uses_deployment_and_api_key_without_public_auth() {
        let request = connection_request(&connection_config(true)).unwrap();
        assert_eq!(
            request.uri().to_string(),
            "wss://example.openai.azure.com/openai/v1/realtime?model=my-voice-deployment"
        );
        assert_eq!(request.headers()["host"], "example.openai.azure.com");
        assert_eq!(request.headers()["api-key"], "test-key");
        assert!(!request.headers().contains_key("Authorization"));
        assert!(!request.uri().to_string().contains("test-key"));
        assert!(!request.uri().to_string().contains("api-version"));
    }

    #[test]
    fn public_request_preserves_bearer_authentication() {
        let request = connection_request(&connection_config(false)).unwrap();
        assert_eq!(request.uri().host(), Some("api.openai.com"));
        assert_eq!(request.headers()["Authorization"], "Bearer test-key");
        assert!(!request.headers().contains_key("api-key"));
    }

    #[test]
    fn live_request_uses_public_live_endpoint_without_query_or_azure_key_header() {
        let mut config = connection_config(false);
        config.voice_api = VoiceApi::Live;
        let request = connection_request(&config).unwrap();
        assert_eq!(
            request.uri().to_string(),
            "wss://api.openai.com/v1/live/sessions"
        );
        assert_eq!(request.headers()["Authorization"], "Bearer test-key");
        assert!(!request.headers().contains_key("api-key"));
    }

    #[test]
    fn azure_live_request_uses_resource_endpoint_and_api_key_without_query() {
        let mut config = connection_config(true);
        config.voice_api = VoiceApi::Live;
        config.live_backend_model = "my-responses-deployment".into();
        let request = connection_request(&config).unwrap();
        assert_eq!(
            request.uri().to_string(),
            "wss://example.openai.azure.com/openai/v1/live/sessions"
        );
        assert_eq!(request.headers()["api-key"], "test-key");
        assert!(!request.headers().contains_key("Authorization"));
        let event = live::session_start(
            &config.model,
            &config.voice,
            &config.instructions,
            &config.live_backend_model,
        );
        assert_eq!(event["session"]["model"], "my-voice-deployment");
        assert_eq!(
            event["session"]["delegation"]["responses"]["model"],
            "my-responses-deployment"
        );
    }

    #[test]
    fn deployment_name_cannot_inject_query_parameters() {
        let mut config = connection_config(true);
        config.model = "voice&api-key=injected".into();
        let request = connection_request(&config).unwrap();
        let url = url::Url::parse(&request.uri().to_string()).unwrap();
        let parameters: Vec<_> = url.query_pairs().collect();
        assert_eq!(parameters.len(), 1);
        assert_eq!(parameters[0].0, "model");
        assert_eq!(parameters[0].1, config.model);
    }

    #[test]
    fn connection_diagnostics_redact_credentials_and_keep_the_reason() {
        let mut config = connection_config(true);
        config.discord_token = "private-discord-token".into();
        let error = anyhow::anyhow!(
            "Rejected voice; API key={}, Discord token={}",
            config.openai_key,
            config.discord_token
        )
        .context("Session setup failed");
        let message = safe_connection_error(&error, &config);
        assert!(message.contains("Session setup failed: Rejected voice"));
        assert!(!message.contains(&config.openai_key));
        assert!(!message.contains(&config.discord_token));
        assert_eq!(message.matches("[redacted]").count(), 2);
    }
}
