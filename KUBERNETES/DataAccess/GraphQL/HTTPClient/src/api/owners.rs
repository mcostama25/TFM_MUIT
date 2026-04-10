use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use tracing::info;

use crate::repository::common::OwnerRepository;

pub struct OwnerRouter<O> {
    owner_svc: Arc<O>,
}

impl<O> OwnerRouter<O>
where
    O: OwnerRepository + Send + Sync + 'static,
{
    pub fn new(owner_svc: Arc<O>) -> Self {
        Self { owner_svc }
    }

    pub fn router(self) -> Router {
        Router::new()
            .route(
                "/owners/:username/resources",
                get(Self::handle_list_owner_resources),
            )
            .with_state(self.owner_svc)
    }

    async fn handle_list_owner_resources(
        State(owner_svc): State<Arc<O>>,
        Path(username): Path<String>,
    ) -> impl IntoResponse {
        info!("GET /owners/{username}/resources");
        match owner_svc.list_by_owner(&username).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }
}
