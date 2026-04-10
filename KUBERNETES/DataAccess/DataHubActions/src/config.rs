use anyhow::Result;
use config::{Config, Environment, File};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub kafka: KafkaConfig,
    pub schema_registry: SchemaRegistryConfig,
    pub server: ServerConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub group_id: String,
    /// Kafka topics to subscribe to. Set in config.toml as a TOML array.
    /// Env override: KAFKA__BOOTSTRAP_SERVERS, KAFKA__GROUP_ID, etc. (scalars only).
    pub topics: Vec<String>,
    pub auto_offset_reset: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SchemaRegistryConfig {
    /// Base URL of the Schema Registry.
    /// For DataHub GMS: http://datahub-datahub-gms:8080
    /// Schema fetch path: {url}/schema-registry/api/schemas/ids/{id}
    pub url: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub port: u16,
}

impl AppConfig {
    /// Load config from `config.toml`, then override with env vars.
    /// Env var format: KAFKA__BOOTSTRAP_SERVERS, SCHEMA_REGISTRY__URL, SERVER__PORT
    pub fn load() -> Result<Self> {
        let cfg = Config::builder()
            .add_source(File::with_name("config").required(true))
            .add_source(Environment::default().separator("__"))
            .build()?;

        Ok(cfg.try_deserialize()?)
    }
}
