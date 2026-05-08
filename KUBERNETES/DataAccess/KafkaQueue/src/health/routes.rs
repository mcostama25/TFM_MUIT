use axum::{http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use serde_json::json;

pub fn health_router() -> Router {
    Router::new().route("/health", get(health_handler))
}

async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({ "status": "ok" })))
}
