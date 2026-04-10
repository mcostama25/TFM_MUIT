mod api;
mod config;
mod datahub;
mod error;
mod models;
mod repository;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use axum::Router;
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use api::{
    catalogs::CatalogRouter,
    common::CommonRouter,
    dataservices::DataServiceRouter,
    datasets::DatasetRouter,
    distributions::DistributionRouter,
    owners::OwnerRouter,
};
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

    // Single DatahubClient instance, cloned into each router
    let client = Arc::new(DatahubClient::new(&cfg.datahub)?);

    let app = Router::new()
        .merge(
            CatalogRouter::new(client.clone(), client.clone(), client.clone()).router(),
        )
        .merge(
            DatasetRouter::new(client.clone(), client.clone(), client.clone(), client.clone())
                .router(),
        )
        .merge(DistributionRouter::new(client.clone()).router())
        .merge(DataServiceRouter::new(client.clone()).router())
        .merge(
            CommonRouter::new(
                client.clone(),
                client.clone(),
                client.clone(),
                client.clone(),
                client.clone(),
                client.clone(),
            )
            .router(),
        )
        .merge(OwnerRouter::new(client.clone()).router())
        .layer(TraceLayer::new_for_http());

    let addr = SocketAddr::from(([0, 0, 0, 0], cfg.server.port));
    info!("DCAT API server listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
