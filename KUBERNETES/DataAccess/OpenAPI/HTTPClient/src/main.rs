mod api;
mod config;
mod datahub;
mod error;
mod models;
mod repository;

use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use api::{build_router, AppState};
use datahub::client::DatahubClient;

#[tokio::main]
async fn main() -> Result<()> {
    // Tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "datahub_dcat_client=info,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Config
    let cfg = config::AppConfig::load()?;
    info!("Connecting to DataHub at {}", cfg.datahub.base_url);

    // Build the DataHub client (single instance, shared via Arc)
    let client = Arc::new(DatahubClient::new(&cfg.datahub)?);

    // Build shared application state — each repository trait is implemented by
    // the same DatahubClient, so we clone the Arc for each slot.
    let state = Arc::new(AppState {
        catalogs: client.clone(),
        catalog_records: client.clone(),
        datasets: client.clone(),
        dataset_series: client.clone(),
        distributions: client.clone(),
        dataservices: client.clone(),
        keywords: client.clone(),
        themes: client.clone(),
        references: client.clone(),
        relations: client.clone(),
        qualified_relations: client.clone(),
        resources: client.clone(),
    });

    // Build Axum router
    let app = build_router(state).layer(TraceLayer::new_for_http());

    let addr = SocketAddr::from(([0, 0, 0, 0], cfg.server.port));
    info!("DCAT API server listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
