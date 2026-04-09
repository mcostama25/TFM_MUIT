use anyhow::Result;
use config::{Config, Environment, File};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub datahub: DatahubConfig,
    pub server: ServerConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatahubConfig {
    pub base_url: String,
    pub token: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub port: u16,
}

impl AppConfig {
    /// Load config from `config.toml`, then override with env vars.
    /// Env var format: DATAHUB__BASE_URL, DATAHUB__TOKEN, SERVER__PORT
    pub fn load() -> Result<Self> {
        let cfg = Config::builder()
            .add_source(File::with_name("config").required(true))
            .add_source(Environment::default().separator("__"))
            .build()?;

        Ok(cfg.try_deserialize()?)
    }
}
