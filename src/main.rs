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
    text_channel: ChannelId,
    ssrc: Arc<AtomicU64>,
    access: Arc<Mutex<SpeakerAccess>>,
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

struct SpeakerAccess {
    owner: UserId,
    bot: UserId,
    everyone: bool,
    speakers: HashMap<u32, UserId>,
}

impl SpeakerAccess {
    fn allows(&self, ssrc: u32) -> bool {
        self.speakers
            .get(&ssrc)
            .is_some_and(|user| *user != self.bot && (self.everyone || *user == self.owner))
    }
}

fn mix_audio<'audio>(frames: impl Iterator<Item = &'audio [i16]>) -> Vec<i16> {
    let mut mixed = vec![0_i32; INPUT_FRAMES * 2];
    for frame in frames {
        for (sample, incoming) in mixed.iter_mut().zip(frame) {
            *sample += i32::from(*incoming);
        }
    }
    mixed
        .into_iter()
        .map(|sample| sample.clamp(i32::from(i16::MIN), i32::from(i16::MAX)) as i16)
        .collect()
}

fn can_control(owner: UserId, actor: UserId, moderator: bool) -> bool {
    owner == actor || moderator
}

#[derive(Clone)]
struct Receive {
    owner: UserId,
    ssrc: Arc<AtomicU64>,
    access: Arc<Mutex<SpeakerAccess>>,
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
                if let Some(user) = speaking.user_id {
                    self.access
                        .lock()
                        .await
                        .speakers
                        .insert(speaking.ssrc, UserId::new(user.0));
                }
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
                let access = self.access.lock().await;
                let frames: Vec<_> = tick
                    .speaking
                    .iter()
                    .filter(|(ssrc, _)| access.allows(**ssrc))
                    .filter_map(|(_, voice)| voice.decoded_voice.as_deref())
                    .filter(|frame| !frame.is_empty())
                    .collect();
                let frame = mix_audio(frames.iter().copied());
                if !frames.is_empty() && self.health.record_frame(&frame) {
                    info!("Discord microphone audio received and decoded");
                }
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
                let (ssrc, access) = if let Some(session) = sessions.get(&guild_id) {
                    if !resetting {
                        bail!("I already have an active conversation in this server");
                    }
                    if session.owner != command.user.id {
                        bail!("Only the person who started the conversation can reset it");
                    }
                    if session.channel != channel {
                        bail!("Return to the conversation's voice channel first");
                    }
                    (session.ssrc.clone(), session.access.clone())
                } else {
                    if resetting {
                        bail!("There is no active conversation to reset");
                    }
                    (
                        Arc::new(AtomicU64::new(0)),
                        Arc::new(Mutex::new(SpeakerAccess {
                            owner: command.user.id,
                            bot: ctx.cache.current_user().id,
                            everyone: false,
                            speakers: HashMap::new(),
                        })),
                    )
                };
                let socket = realtime::connect(&self.config).await.map_err(|error| {
                    error!(%error, "Could not start OpenAI voice");
                    anyhow::anyhow!("Could not start OpenAI voice: {error}")
                })?;
                if let Some(session) = sessions.remove(&guild_id) {
                    session.stop().await;
                }
                let audience = if access.lock().await.everyone {
                    "everyone in the channel's microphones are".to_string()
                } else {
                    format!("only <@{}>'s microphone is", command.user.id.get())
                };
                if command.channel_id.say(&ctx.http, format!(
                    "Geppetto is joining <#{}>. AI-generated voice; {audience} sent to the AI provider. \
                     Audio is not saved locally. /leave stops listening.", channel.get()
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
                    access: access.clone(),
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
                let worker_config = self.config.clone();
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
                    if let Err(error) = &result {
                        error!(%guild_id, error = %realtime::safe_error_message(error, &worker_config), "Voice session failed");
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
                        text_channel: command.channel_id,
                        ssrc,
                        access,
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
            "listen" | "leave" => {
                let session = sessions
                    .get(&guild_id)
                    .context("There is no active conversation")?;
                let moderator = command
                    .member
                    .as_ref()
                    .and_then(|member| member.permissions)
                    .is_some_and(|permissions| permissions.contains(Permissions::MANAGE_GUILD));
                if !can_control(session.owner, command.user.id, moderator) {
                    bail!(
                        "Only the conversation owner or a server manager can change listening or disconnect me"
                    );
                }
                if command.data.name == "listen" {
                    let mode = command
                        .data
                        .options
                        .iter()
                        .find(|option| option.name == "mode")
                        .and_then(|option| option.value.as_str())
                        .context("Choose owner or everyone")?;
                    let everyone = match mode {
                        "owner" => false,
                        "everyone" => true,
                        _ => bail!("Choose owner or everyone"),
                    };
                    if session.access.lock().await.everyone == everyone {
                        return Ok(format!("Listening mode is already {mode}."));
                    }
                    let notice = if everyone {
                        format!(
                            "Geppetto will now send everyone's microphone audio in <#{}> to the AI provider. Inform participants and obtain their consent. /listen mode:owner restores owner-only listening; /leave stops listening.",
                            session.channel.get()
                        )
                    } else {
                        format!(
                            "Geppetto will now send only <@{}>'s microphone audio in <#{}> to the AI provider. Other participants are no longer forwarded.",
                            session.owner.get(),
                            session.channel.get()
                        )
                    };
                    session
                        .text_channel
                        .say(&ctx.http, notice)
                        .await
                        .context("I need Send Messages to announce the listening change")?;
                    session.access.lock().await.everyone = everyone;
                    return Ok(format!(
                        "Listening mode changed to {mode}. Conversation context is unchanged."
                    ));
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
            CreateCommand::new("listen")
                .description(
                    "Choose whose microphones are sent to the AI in the active conversation",
                )
                .dm_permission(false)
                .add_option(
                    CreateCommandOption::new(
                        CommandOptionType::String,
                        "mode",
                        "Who can speak with Geppetto",
                    )
                    .required(true)
                    .add_string_choice("Only the person who used /join", "owner")
                    .add_string_choice("Everyone in the voice channel", "everyone"),
                ),
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
    async fn receive_tracks_owner_and_retains_other_speakers_for_mode_changes() {
        let (audio, _receiver) = mpsc::channel(1);
        let ssrc = Arc::new(AtomicU64::new(0));
        let handler = Receive {
            owner: UserId::new(42),
            ssrc: ssrc.clone(),
            access: Arc::new(Mutex::new(SpeakerAccess {
                owner: UserId::new(42),
                bot: UserId::new(1),
                everyone: false,
                speakers: HashMap::new(),
            })),
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
        {
            let mut access = handler.access.lock().await;
            assert!(access.allows(456));
            assert!(!access.allows(123));
            access.everyone = true;
            assert!(access.allows(123));
            access.everyone = false;
            assert!(!access.allows(123));
        }
        handler.cancel.cancel();
        let other = serde_json::from_value(serde_json::json!({
            "speaking": 1, "delay": 0, "ssrc": 789, "user_id": "42"
        }))
        .unwrap();
        songbird::EventHandler::act(&handler, &EventContext::SpeakingStateUpdate(other)).await;
        assert_eq!(ssrc.load(Ordering::Relaxed), 457);
    }

    #[test]
    fn listening_mode_switches_known_speakers_and_excludes_bot_and_unknown_streams() {
        let mut access = SpeakerAccess {
            owner: UserId::new(42),
            bot: UserId::new(1),
            everyone: false,
            speakers: HashMap::from([
                (123, UserId::new(99)),
                (456, UserId::new(42)),
                (789, UserId::new(1)),
            ]),
        };
        assert!(access.allows(456));
        assert!(!access.allows(123));
        access.everyone = true;
        assert!(access.allows(123));
        assert!(access.allows(456));
        assert!(!access.allows(789));
        assert!(!access.allows(999));
        access.everyone = false;
        assert!(!access.allows(123));
        assert!(access.allows(456));
    }

    #[test]
    fn mixed_audio_preserves_tick_duration_and_clamps_overlap() {
        let first = [i16::MAX, i16::MIN, 1000, -1000];
        let second = [1, -1, -500, 500];
        let frame = mix_audio([first.as_slice(), second.as_slice()].into_iter());
        assert_eq!(frame.len(), INPUT_FRAMES * 2);
        assert_eq!(&frame[..4], &[i16::MAX, i16::MIN, 500, -500]);
        assert!(frame[4..].iter().all(|sample| *sample == 0));
        assert!(
            mix_audio(std::iter::empty())
                .iter()
                .all(|sample| *sample == 0)
        );
    }

    #[test]
    fn only_owner_or_server_manager_can_control_listening() {
        assert!(can_control(UserId::new(42), UserId::new(42), false));
        assert!(can_control(UserId::new(42), UserId::new(99), true));
        assert!(!can_control(UserId::new(42), UserId::new(99), false));
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
