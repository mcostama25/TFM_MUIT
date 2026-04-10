use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Config error: {0}")]
    Config(#[from] config::ConfigError),

    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error("Avro decode error: {0}")]
    AvroDecode(String),

    #[error("Schema registry error: {0}")]
    SchemaRegistry(String),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

impl From<apache_avro::Error> for AppError {
    fn from(e: apache_avro::Error) -> Self {
        AppError::AvroDecode(e.to_string())
    }
}
