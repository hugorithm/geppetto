use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Context as _, Result, bail};
use geppetto::{audio::INPUT_FRAMES, config::Config, realtime};
use serenity::{all::*, async_trait};
use songbird::{CoreEvent, Event, EventContext, SerenityInit, driver::DecodeMode};
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

struct Session {
    owner: UserId,
    channel: ChannelId,
    ssrc: Arc<AtomicU64>,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl Session {
    async fn stop(self) {
        self.cancel.cancel();
        let _ = self.task.await;
    }
}

struct Bot {
    config: Arc<Config>,
    sessions: Mutex<HashMap<GuildId, Session>>,
}

#[derive(Default)]
struct ReceiveHealth {
    decoded_frames: AtomicU64,
    audible_frames: AtomicU64,
}

impl ReceiveHealth {
    fn record_frame(&self, frame: &[i16]) -> bool {
        self.decoded_frames.fetch_add(1, Ordering::Relaxed);
        if frame.iter().any(|sample| i32::from(*sample).abs() > 300) {
            self.audible_frames.fetch_add(1, Ordering::Relaxed) == 0
        } else {
            false
        }
    }
}

#[derive(Clone)]
struct Receive {
    owner: UserId,
    ssrc: Arc<AtomicU64>,
    health: Arc<ReceiveHealth>,
    audio: mpsc::Sender<Vec<i16>>,
    cancel: CancellationToken,
}

#[async_trait]
impl songbird::EventHandler for Receive {
    async fn act(&self, event: &EventContext<'_>) -> Option<Event> {
        if self.cancel.is_cancelled() {
            return Some(Event::Cancel);
        }
        match event {
            EventContext::SpeakingStateUpdate(speaking) => {
                if speaking.user_id.map(|user| user.0) == Some(self.owner.get()) {
                    self.ssrc
                        .store(u64::from(speaking.ssrc) + 1, Ordering::Relaxed);
                    info!(
                        ssrc = speaking.ssrc,
                        "Discord caller audio stream identified"
                    );
                }
            }
            EventContext::VoiceTick(tick) => {
                let ssrc = self.ssrc.load(Ordering::Relaxed);
                let frame = if ssrc > 0 {
                    tick.speaking
                        .get(&((ssrc - 1) as u32))
                        .and_then(|voice| voice.decoded_voice.clone())
                        .filter(|frame| !frame.is_empty())
                } else {
                    None
                };
                if let Some(frame) = &frame
                    && self.health.record_frame(frame)
                {
                    info!("Discord microphone audio received and decoded");
                }
                let frame = frame.unwrap_or_else(|| vec![0; INPUT_FRAMES * 2]);
                if self.audio.try_send(frame).is_err() {
                    warn!("Voice input queue unavailable; stopping session");
                    self.cancel.cancel();
                }
            }
            _ => {}
        }
        None
    }
}

impl Bot {
    async fn execute(&self, ctx: &Context, command: &CommandInteraction) -> Result<String> {
        let guild_id = command.guild_id.context("Use this command in a server")?;
        let manager = songbird::get(ctx)
            .await
            .context("Voice manager unavailable")?;
        let mut sessions = self.sessions.lock().await;
        if sessions
            .get(&guild_id)
            .is_some_and(|session| session.cancel.is_cancelled())
            && let Some(session) = sessions.remove(&guild_id)
        {
            session.stop().await;
        }
        match command.data.name.as_str() {
            "join" | "reset" => {
                let channel = ctx
                    .cache
                    .guild(guild_id)
                    .and_then(|guild| {
                        guild
                            .voice_states
                            .get(&command.user.id)
                            .and_then(|state| state.channel_id)
                    })
                    .context("Join a voice channel first")?;
                let resetting = command.data.name == "reset";
                let ssrc = if let Some(session) = sessions.get(&guild_id) {
                    if !resetting {
                        bail!("I already have an active conversation in this server");
                    }
                    if session.owner != command.user.id {
                        bail!("Only the person who started the conversation can reset it");
                    }
                    if session.channel != channel {
                        bail!("Return to the conversation's voice channel first");
                    }
                    session.ssrc.clone()
                } else {
                    if resetting {
                        bail!("There is no active conversation to reset");
                    }
                    Arc::new(AtomicU64::new(0))
                };
                let socket = realtime::connect(&self.config).await.map_err(|error| {
                    error!(%error, "Could not start OpenAI voice");
                    anyhow::anyhow!("Could not start OpenAI voice: {error}")
                })?;
                if let Some(session) = sessions.remove(&guild_id) {
                    session.stop().await;
                }
                if command.channel_id.say(&ctx.http, format!(
                    "Geppetto is joining <#{}>. AI-generated voice; only <@{}>'s microphone is sent to OpenAI. \
                     Audio is not saved locally. /leave stops listening.", channel.get(), command.user.id.get()
                )).await.is_err() {
                    bail!("I need Send Messages here to announce that voice listening is starting");
                }
                let call = manager.get_or_insert(guild_id);
                let cancel = CancellationToken::new();
                let (audio, receiver) = mpsc::channel(50);
                let health = Arc::new(ReceiveHealth::default());
                let handler = Receive {
                    owner: command.user.id,
                    ssrc: ssrc.clone(),
                    health: health.clone(),
                    audio,
                    cancel: cancel.clone(),
                };
                let deafen_result = {
                    let mut call = call.lock().await;
                    call.remove_all_global_events();
                    call.add_global_event(
                        Event::Core(CoreEvent::SpeakingStateUpdate),
                        handler.clone(),
                    );
                    call.add_global_event(Event::Core(CoreEvent::VoiceTick), handler);
                    call.deafen(false).await
                };
                if deafen_result.is_err() || manager.join(guild_id, channel).await.is_err() {
                    cancel.cancel();
                    let _ = manager.remove(guild_id).await;
                    bail!(
                        "Could not join voice. Check my View Channel, Connect, and Speak permissions"
                    );
                }
                let worker_cancel = cancel.clone();
                let voice_api = self.config.voice_api;
                let http = ctx.http.clone();
                let text_channel = command.channel_id;
                let monitor_ssrc = ssrc.clone();
                let task = tokio::spawn(async move {
                    let monitor_cancel = worker_cancel.clone();
                    let monitor = tokio::spawn(async move {
                        tokio::select! {
                            _ = monitor_cancel.cancelled() => {},
                            _ = tokio::time::sleep(std::time::Duration::from_secs(15)) => {
                                if health.audible_frames.load(Ordering::Relaxed) == 0 {
                                    let identified = monitor_ssrc.load(Ordering::Relaxed) != 0;
                                    let decoded_frames = health.decoded_frames.load(Ordering::Relaxed);
                                    warn!(%guild_id, identified, decoded_frames,
                                        "No audible caller audio received after 15 seconds. If you were speaking: identified=false suggests missing speaker mapping; decoded_frames=0 suggests stalled Discord receive/decryption; decoded_frames>0 suggests silence or a muted microphone. Try /leave then /join.");
                                }
                            }
                        }
                    });
                    let result =
                        realtime::run(socket, call, receiver, worker_cancel.clone(), voice_api)
                            .await;
                    worker_cancel.cancel();
                    let _ = monitor.await;
                    if result.is_err() {
                        error!(%guild_id, "Voice session failed; check OpenAI billing, limits, and network");
                    }
                    let _ = manager.remove(guild_id).await;
                    let notice = if result.is_err() {
                        "Voice conversation ended due to an API or connection error. Use /join to reconnect."
                    } else {
                        "Voice conversation ended. Geppetto is no longer listening."
                    };
                    let _ = text_channel.say(&http, notice).await;
                });
                sessions.insert(
                    guild_id,
                    Session {
                        owner: command.user.id,
                        channel,
                        ssrc,
                        cancel,
                        task,
                    },
                );
                Ok(if resetting {
                    "Conversation reset. You can speak now."
                } else {
                    "Connected. You can speak now."
                }
                .into())
            }
            "leave" => {
                let session = sessions
                    .get(&guild_id)
                    .context("There is no active conversation")?;
                let moderator = command
                    .member
                    .as_ref()
                    .and_then(|member| member.permissions)
                    .is_some_and(|permissions| permissions.contains(Permissions::MANAGE_GUILD));
                if session.owner != command.user.id && !moderator {
                    bail!("Only the conversation owner or a server manager can disconnect me");
                }
                if let Some(session) = sessions.remove(&guild_id) {
                    session.stop().await;
                }
                Ok("Disconnected and stopped listening.".into())
            }
            _ => bail!("Unknown command"),
        }
    }
}

#[async_trait]
impl EventHandler for Bot {
    async fn ready(&self, ctx: Context, ready: Ready) {
        let commands = vec![
            CreateCommand::new("join")
                .description("Start an AI voice conversation using your microphone")
                .dm_permission(false),
            CreateCommand::new("leave")
                .description("Disconnect Geppetto and stop listening")
                .dm_permission(false),
            CreateCommand::new("reset")
                .description("Start a fresh voice conversation")
                .dm_permission(false),
        ];
        let result = if let Some(guild_id) = self.config.guild_id {
            GuildId::new(guild_id)
                .set_commands(&ctx.http, commands)
                .await
        } else {
            Command::set_global_commands(&ctx.http, commands).await
        };
        match result {
            Ok(_) => info!(user = %ready.user.name, "Bot ready; slash commands registered"),
            Err(_) => error!("Could not register slash commands; check application permissions"),
        }
    }

    async fn interaction_create(&self, ctx: Context, interaction: Interaction) {
        let Interaction::Command(command) = interaction else {
            return;
        };
        if command
            .create_response(
                &ctx.http,
                CreateInteractionResponse::Defer(
                    CreateInteractionResponseMessage::new().ephemeral(true),
                ),
            )
            .await
            .is_err()
        {
            return;
        }
        let message = match self.execute(&ctx, &command).await {
            Ok(message) => message,
            Err(error) => error.to_string(),
        };
        let _ = command
            .edit_response(&ctx.http, EditInteractionResponse::new().content(message))
            .await;
    }

    async fn voice_state_update(&self, ctx: Context, _old: Option<VoiceState>, new: VoiceState) {
        let Some(guild_id) = new.guild_id else {
            return;
        };
        let bot_id = ctx.cache.current_user().id;
        let sessions = self.sessions.lock().await;
        if let Some(session) = sessions.get(&guild_id)
            && (new.user_id == session.owner || new.user_id == bot_id)
            && new.channel_id != Some(session.channel)
        {
            session.cancel.cancel();
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "geppetto=info,warn".into()),
        )
        .init();
    let config = Arc::new(Config::from_env()?);
    let manager = songbird::Songbird::serenity_from_config(
        songbird::Config::default().decode_mode(DecodeMode::Decode(Default::default())),
    );
    let bot = Arc::new(Bot {
        config: config.clone(),
        sessions: Mutex::new(HashMap::new()),
    });
    let intents = GatewayIntents::GUILDS | GatewayIntents::GUILD_VOICE_STATES;
    let mut client = Client::builder(&config.discord_token, intents)
        .event_handler_arc(bot.clone())
        .register_songbird_with(manager)
        .await?;
    tokio::select! {
        result = client.start() => result.context("Discord gateway failed")?,
        _ = tokio::signal::ctrl_c() => info!("Stopping bot"),
    }
    let sessions: Vec<Session> = bot
        .sessions
        .lock()
        .await
        .drain()
        .map(|(_, session)| session)
        .collect();
    for session in &sessions {
        session.cancel.cancel();
    }
    for session in sessions {
        session.stop().await;
    }
    client.shard_manager.shutdown_all().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn receive_only_maps_the_conversation_owner() {
        let (audio, _receiver) = mpsc::channel(1);
        let ssrc = Arc::new(AtomicU64::new(0));
        let handler = Receive {
            owner: UserId::new(42),
            ssrc: ssrc.clone(),
            health: Arc::new(ReceiveHealth::default()),
            audio,
            cancel: CancellationToken::new(),
        };
        let other = serde_json::from_value(serde_json::json!({
            "speaking": 1, "delay": 0, "ssrc": 123, "user_id": "99"
        }))
        .unwrap();
        songbird::EventHandler::act(&handler, &EventContext::SpeakingStateUpdate(other)).await;
        assert_eq!(ssrc.load(Ordering::Relaxed), 0);
        let owner = serde_json::from_value(serde_json::json!({
            "speaking": 1, "delay": 0, "ssrc": 456, "user_id": "42"
        }))
        .unwrap();
        songbird::EventHandler::act(&handler, &EventContext::SpeakingStateUpdate(owner)).await;
        assert_eq!(ssrc.load(Ordering::Relaxed), 457);
        handler.cancel.cancel();
        let other = serde_json::from_value(serde_json::json!({
            "speaking": 1, "delay": 0, "ssrc": 789, "user_id": "42"
        }))
        .unwrap();
        songbird::EventHandler::act(&handler, &EventContext::SpeakingStateUpdate(other)).await;
        assert_eq!(ssrc.load(Ordering::Relaxed), 457);
    }

    #[test]
    fn receive_health_distinguishes_decoded_silence_from_audible_audio() {
        let health = ReceiveHealth::default();
        assert!(!health.record_frame(&[0, 0, 0, 0]));
        assert_eq!(health.decoded_frames.load(Ordering::Relaxed), 1);
        assert_eq!(health.audible_frames.load(Ordering::Relaxed), 0);
        assert!(health.record_frame(&[i16::MIN, i16::MAX]));
        assert!(!health.record_frame(&[1000, -1000]));
        assert_eq!(health.decoded_frames.load(Ordering::Relaxed), 3);
        assert_eq!(health.audible_frames.load(Ordering::Relaxed), 2);
    }
}
