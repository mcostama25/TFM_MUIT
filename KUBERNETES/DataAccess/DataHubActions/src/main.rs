mod avro;
mod config;
mod datahub;
mod error;
mod health;
mod kafka;
mod models;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use avro::AvroDecoder;
use health::health_router;
use kafka::{build_consumer, run_consumer_loop};

#[tokio::main]
async fn main() -> Result<()> {
    // Tracing — mirrors HTTPClient pattern
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "datahub_kafka_consumer=info,rdkafka=warn".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Config
    let cfg = config::AppConfig::load()?;
    info!(
        bootstrap_servers = %cfg.kafka.bootstrap_servers,
        group_id = %cfg.kafka.group_id,
        topics = ?cfg.kafka.topics,
        "Kafka config loaded"
    );
    info!(
        schema_registry_url = %cfg.schema_registry.url,
        "Schema registry config loaded"
    );

    // Shared Avro decoder with schema cache
    let decoder = Arc::new(AvroDecoder::new(&cfg.schema_registry.url));

    // Kafka consumer
    let consumer = build_consumer(&cfg.kafka)?;
    let topics = cfg.kafka.topics.clone();

    // Health server
    let addr = SocketAddr::from(([0, 0, 0, 0], cfg.server.port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let health_app = health_router().layer(TraceLayer::new_for_http());
    info!("Health server listening on :{}", cfg.server.port);

    // Run both tasks concurrently.
    // tokio::join! keeps both alive for the process lifetime.
    // If either exits, the process exits — K8s Deployment will restart it.
    tokio::join!(
        async {
            if let Err(e) = run_consumer_loop(consumer, &topics, decoder).await {
                tracing::error!(error = %e, "Consumer loop exited with error");
            }
        },
        async {
            if let Err(e) = axum::serve(listener, health_app).await {
                tracing::error!(error = %e, "Health server exited with error");
            }
        }
    );

    Ok(())
}
