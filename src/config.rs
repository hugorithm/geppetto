use std::env;

use anyhow::{Context, Result, ensure};
use url::Url;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VoiceApi {
    Realtime,
    Live,
}

impl VoiceApi {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "realtime" => Ok(Self::Realtime),
            "live" => Ok(Self::Live),
            _ => anyhow::bail!("VOICE_API must be realtime or live"),
        }
    }
}

pub struct Config {
    pub discord_token: String,
    pub openai_key: String,
    pub model: String,
    pub voice_api: VoiceApi,
    pub live_backend_model: String,
    pub azure_endpoint: Option<Url>,
    pub voice: String,
    pub instructions: String,
    pub guild_id: Option<u64>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let voice_api = VoiceApi::parse(&env::var("VOICE_API").unwrap_or_default())?;
        let azure_endpoint = env::var("AZURE_OPENAI_ENDPOINT")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|value| parse_azure_endpoint(&value))
            .transpose()?;
        let (openai_key, model) = if azure_endpoint.is_some() {
            (
                required("AZURE_OPENAI_API_KEY")?,
                required("AZURE_OPENAI_DEPLOYMENT_NAME")?,
            )
        } else if voice_api == VoiceApi::Live {
            (
                required("OPENAI_API_KEY")?,
                nonempty_or_default("OPENAI_LIVE_MODEL", "gpt-live-1"),
            )
        } else {
            (
                required("OPENAI_API_KEY")?,
                env::var("OPENAI_REALTIME_MODEL").unwrap_or_else(|_| "gpt-realtime-2.1".into()),
            )
        };
        let live_backend_model = if voice_api == VoiceApi::Live && azure_endpoint.is_some() {
            required("AZURE_OPENAI_LIVE_BACKEND_DEPLOYMENT_NAME")?
        } else {
            nonempty_or_default("OPENAI_LIVE_BACKEND_MODEL", "gpt-6-luna")
        };
        Ok(Self {
            discord_token: required("DISCORD_TOKEN")?,
            openai_key,
            model,
            voice_api,
            live_backend_model,
            azure_endpoint,
            voice: env::var("OPENAI_VOICE").unwrap_or_else(|_| "ash".into()),
            instructions: env::var("SYSTEM_PROMPT").unwrap_or_else(|_| {
                "You are Geppetto, an AI voice assistant in a Discord call. Speak naturally, \
                 keep responses concise, and respond in the user's language. \
                 Never claim to be human."
                    .into()
            }),
            guild_id: env::var("DISCORD_GUILD_ID")
                .ok()
                .filter(|value| !value.is_empty())
                .map(|value| {
                    value
                        .parse()
                        .context("DISCORD_GUILD_ID must be a numeric server ID")
                })
                .transpose()?,
        })
    }
}

fn nonempty_or_default(name: &str, default: &str) -> String {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default.into())
}

fn parse_azure_endpoint(endpoint: &str) -> Result<Url> {
    let mut url =
        Url::parse(endpoint.trim()).context("AZURE_OPENAI_ENDPOINT must be a resource URL")?;
    ensure!(
        matches!(url.scheme(), "https" | "wss") && url.host_str().is_some(),
        "AZURE_OPENAI_ENDPOINT must use HTTPS or WSS"
    );
    ensure!(
        url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "AZURE_OPENAI_ENDPOINT must not contain credentials, query parameters, or a fragment"
    );
    ensure!(
        matches!(url.path().trim_end_matches('/'), "" | "/openai/v1"),
        "AZURE_OPENAI_ENDPOINT must be the resource root or /openai/v1 base URL, not a deployment URL"
    );
    url.set_scheme("wss")
        .map_err(|_| anyhow::anyhow!("Invalid Azure endpoint scheme"))?;
    url.set_path("/openai/v1/realtime");
    Ok(url)
}

fn required(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("Set {name} in the environment or .env"))?;
    ensure!(!value.trim().is_empty(), "{name} cannot be empty");
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn voice_api_selection_defaults_to_realtime() {
        assert_eq!(VoiceApi::parse("").unwrap(), VoiceApi::Realtime);
        assert_eq!(VoiceApi::parse("live").unwrap(), VoiceApi::Live);
        assert!(VoiceApi::parse("unknown").is_err());
    }

    #[test]
    fn azure_endpoint_normalizes_resource_and_v1_urls() {
        for endpoint in [
            "https://example.openai.azure.com",
            "https://example.openai.azure.com/",
            "https://example.openai.azure.com/openai/v1/",
        ] {
            assert_eq!(
                parse_azure_endpoint(endpoint).unwrap().as_str(),
                "wss://example.openai.azure.com/openai/v1/realtime"
            );
        }
    }

    #[test]
    fn azure_endpoint_rejects_insecure_or_ambiguous_urls() {
        for endpoint in [
            "http://example.openai.azure.com",
            "https://user:secret@example.openai.azure.com",
            "https://example.openai.azure.com/?api-key=secret",
            "https://example.openai.azure.com/#fragment",
            "https://example.openai.azure.com/openai/deployments/chat",
        ] {
            assert!(parse_azure_endpoint(endpoint).is_err());
        }
    }
}
