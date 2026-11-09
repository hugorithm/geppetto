use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::StreamExt;
use serde_json::{Value, json};
use songbird::{Call, input::RawAdapter, tracks::TrackHandle};
use tokio::{
    sync::{Mutex, mpsc},
    time::{Instant, timeout},
};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::{
    audio::{DiscordResampler, PlaybackBuffer},
    realtime::{Socket, send},
};

pub fn session_start(model: &str, voice: &str, instructions: &str, backend_model: &str) -> Value {
    json!({
        "type": "session.start",
        "session": {
            "model": model,
            "instructions": instructions,
            "audio": {
                "output": {"voice": voice}
            },
            "delegation": {
                "type": "responses",
                "responses": {"model": backend_model, "tool_choice": "none"}
            }
        }
    })
}

pub fn audio_append(audio: &str) -> Value {
    json!({"type": "session.input_audio.append", "audio": audio})
}

struct Playback {
    buffer: PlaybackBuffer,
    track: TrackHandle,
}

impl Drop for Playback {
    fn drop(&mut self) {
        self.buffer.finish(true);
        let _ = self.track.stop();
    }
}

pub(crate) async fn run(
    mut socket: Socket,
    call: Arc<Mutex<Call>>,
    audio: mpsc::Receiver<Vec<i16>>,
    cancel: CancellationToken,
) -> Result<()> {
    let result = run_session(&mut socket, call, audio, cancel).await;
    if !matches!(result, Ok(true))
        && let Err(error) = close(&mut socket).await
    {
        tracing::warn!(%error, "Live session finalization was not confirmed");
    }
    let _ = timeout(Duration::from_secs(2), socket.close(None)).await;
    result.map(|_| ())
}

async fn run_session(
    socket: &mut Socket,
    call: Arc<Mutex<Call>>,
    mut audio: mpsc::Receiver<Vec<i16>>,
    cancel: CancellationToken,
) -> Result<bool> {
    let mut resampler = DiscordResampler::new()?;
    let mut playback: Option<Playback> = None;
    let mut last_speech = Instant::now();
    let deadline = tokio::time::sleep(Duration::from_secs(55 * 60));
    tokio::pin!(deadline);
    let mut housekeeping = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(false),
            _ = &mut deadline => return Ok(false),
            _ = housekeeping.tick() => {
                if last_speech.elapsed() > Duration::from_secs(5 * 60) { return Ok(false); }
            }
            frame = audio.recv() => {
                let Some(frame) = frame else { return Ok(false); };
                if frame.iter().any(|sample| i32::from(*sample).abs() > 300) {
                    last_speech = Instant::now();
                }
                let pcm = resampler.convert(&frame)?;
                if !pcm.is_empty() { send(socket, audio_append(&STANDARD.encode(pcm))).await?; }
            }
            message = socket.next() => {
                let Some(message) = message else { bail!("Live voice connection closed without finalization"); };
                match message? {
                    Message::Text(text) => {
                        let event: Value = serde_json::from_str(&text)?;
                        match event["type"].as_str() {
                            Some("session.output_audio.delta") => {
                                let pcm = STANDARD.decode(event["delta"].as_str().context("Missing Live audio delta")?)?;
                                if pcm.is_empty() { continue; }
                                if let Some(current) = &playback {
                                    current.buffer.append_pcm16(&pcm)?;
                                } else {
                                    let buffer = PlaybackBuffer::default();
                                    buffer.append_pcm16(&pcm)?;
                                    let track = call.lock().await.play_input(RawAdapter::new(buffer.clone(), 24_000, 1).into());
                                    tracing::info!("Live voice response received; Discord playback requested");
                                    playback = Some(Playback { buffer, track });
                                }
                            }
                            Some("session.input_transcript.delta") => tracing::info!("Live provider recognized caller speech"),
                            Some("response.event") if event["event"]["type"] == "response.failed" => {
                                tracing::warn!("Live delegated response failed; check backend model access and billing");
                            }
                            Some("session.closed") => {
                                tracing::info!(reason = %event["reason"], seconds = %event["usage"]["seconds"], "Live session finalized");
                                return Ok(true);
                            }
                            Some("error") => bail!("Live session error: {}", event["error"]["code"]),
                            _ => {}
                        }
                    }
                    Message::Close(_) => bail!("Live voice connection closed without finalization"),
                    _ => {}
                }
            }
        }
    }
}

pub async fn close(socket: &mut Socket) -> Result<()> {
    send(socket, json!({"type": "session.close"})).await?;
    timeout(Duration::from_secs(15), async {
        while let Some(message) = socket.next().await {
            match message? {
                Message::Text(text) => {
                    let event: Value = serde_json::from_str(&text)?;
                    match event["type"].as_str() {
                        Some("session.closed") => {
                            tracing::info!(reason = %event["reason"], seconds = %event["usage"]["seconds"], "Live session finalized");
                            return Ok(());
                        }
                        Some("error") => bail!("Live close rejected: {}", event["error"]["code"]),
                        _ => {}
                    }
                }
                Message::Close(_) => bail!("Live connection closed before final usage"),
                _ => {}
            }
        }
        bail!("Live connection closed before final usage")
    }).await.context("Timed out waiting for Live session finalization")?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_start_configures_full_duplex_audio_and_responses_delegation() {
        let event = session_start("gpt-live-1", "ash", "Be concise", "gpt-6-luna");
        assert_eq!(event["type"], "session.start");
        assert_eq!(event["session"]["model"], "gpt-live-1");
        assert!(event["session"]["audio"].get("format").is_none());
        assert_eq!(event["session"]["audio"]["output"]["voice"], "ash");
        assert_eq!(event["session"]["delegation"]["type"], "responses");
        assert_eq!(
            event["session"]["delegation"]["responses"]["model"],
            "gpt-6-luna"
        );
        assert!(event["session"].get("store").is_none());
        assert_eq!(audio_append("AA==")["type"], "session.input_audio.append");
    }
}
