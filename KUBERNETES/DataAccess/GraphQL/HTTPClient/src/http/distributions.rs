use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use tracing::info;

use crate::{
    models::distribution::{Distribution, DistributionFilters},
    repository::distribution::DistributionRepository,
};

pub struct DistributionRouter<DI> {
    dist_svc: Arc<DI>,
}

impl<DI> DistributionRouter<DI>
where
    DI: DistributionRepository + Send + Sync + 'static,
{
    pub fn new(dist_svc: Arc<DI>) -> Self {
        Self { dist_svc }
    }

    pub fn router(self) -> Router {
        Router::new()
            .route(
                "/distributions",
                get(Self::handle_list_distributions).post(Self::handle_create_distribution),
            )
            .route(
                "/distributions/:distribution_id",
                get(Self::handle_get_distribution)
                    .put(Self::handle_update_distribution)
                    .delete(Self::handle_delete_distribution),
            )
            .with_state(self.dist_svc)
    }

    async fn handle_list_distributions(
        State(dist_svc): State<Arc<DI>>,
        Query(filters): Query<DistributionFilters>,
    ) -> impl IntoResponse {
        info!("GET /distributions");
        match dist_svc.list(filters).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_create_distribution(
        State(dist_svc): State<Arc<DI>>,
        Json(body): Json<Distribution>,
    ) -> impl IntoResponse {
        info!("POST /distributions");
        match dist_svc.create(body).await {
            Ok(v) => (StatusCode::CREATED, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_get_distribution(
        State(dist_svc): State<Arc<DI>>,
        Path(distribution_id): Path<String>,
    ) -> impl IntoResponse {
        info!("GET /distributions/{distribution_id}");
        match dist_svc.get(&distribution_id).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_update_distribution(
        State(dist_svc): State<Arc<DI>>,
        Path(distribution_id): Path<String>,
        Json(body): Json<Distribution>,
    ) -> impl IntoResponse {
        info!("PUT /distributions/{distribution_id}");
        match dist_svc.update(&distribution_id, body).await {
            Ok(()) => StatusCode::OK.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_delete_distribution(
        State(dist_svc): State<Arc<DI>>,
        Path(distribution_id): Path<String>,
    ) -> impl IntoResponse {
        info!("DELETE /distributions/{distribution_id}");
        match dist_svc.delete(&distribution_id).await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => e.into_response(),
        }
    }
}
