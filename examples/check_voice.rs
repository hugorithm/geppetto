use anyhow::Result;
use geppetto::{config::Config, realtime};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let config = Config::from_env()?;
    println!(
        "Checking voice session: deployment/model={}, voice={}",
        config.model, config.voice
    );
    let mut socket = realtime::connect(&config).await?;
    socket.close(None).await?;
    println!("Voice session accepted. Connection closed without sending microphone audio.");
    Ok(())
}
