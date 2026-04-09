use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};

use crate::{
    error::AppError,
    models::distribution::{Distribution, DistributionFilters},
};

use super::AppState;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/distributions",
            get(list_distributions).post(create_distribution),
        )
        .route(
            "/distributions/:distribution_id",
            get(get_distribution)
                .put(update_distribution)
                .delete(delete_distribution),
        )
}

async fn list_distributions(
    State(state): State<Arc<AppState>>,
    Query(filters): Query<DistributionFilters>,
) -> Result<Json<Vec<Distribution>>, AppError> {
    Ok(Json(state.distributions.list(filters).await?))
}

async fn create_distribution(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Distribution>,
) -> Result<(StatusCode, Json<Distribution>), AppError> {
    let created = state.distributions.create(body).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn get_distribution(
    State(state): State<Arc<AppState>>,
    Path(distribution_id): Path<String>,
) -> Result<Json<Distribution>, AppError> {
    Ok(Json(state.distributions.get(&distribution_id).await?))
}

async fn update_distribution(
    State(state): State<Arc<AppState>>,
    Path(distribution_id): Path<String>,
    Json(body): Json<Distribution>,
) -> Result<StatusCode, AppError> {
    state.distributions.update(&distribution_id, body).await?;
    Ok(StatusCode::OK)
}

async fn delete_distribution(
    State(state): State<Arc<AppState>>,
    Path(distribution_id): Path<String>,
) -> Result<StatusCode, AppError> {
    state.distributions.delete(&distribution_id).await?;
    Ok(StatusCode::NO_CONTENT)
}
