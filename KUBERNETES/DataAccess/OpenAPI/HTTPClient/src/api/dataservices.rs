use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};

use crate::{
    error::AppError,
    models::{
        dataservice::{DataService, DataServiceFilters},
        dataset::{Dataset, DatasetFilters},
    },
};

use super::AppState;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/dataservices",
            get(list_dataservices).post(create_dataservice),
        )
        .route(
            "/dataservices/:service_id",
            get(get_dataservice)
                .put(update_dataservice)
                .delete(delete_dataservice),
        )
        .route(
            "/dataservices/:service_id/datasets",
            get(list_service_datasets).post(create_service_dataset),
        )
        .route(
            "/dataservices/:service_id/datasets/:dataset_id",
            get(get_service_dataset)
                .put(update_service_dataset)
                .delete(delete_service_dataset),
        )
}

async fn list_dataservices(
    State(state): State<Arc<AppState>>,
    Query(filters): Query<DataServiceFilters>,
) -> Result<Json<Vec<DataService>>, AppError> {
    Ok(Json(state.dataservices.list(filters).await?))
}

async fn create_dataservice(
    State(state): State<Arc<AppState>>,
    Json(body): Json<DataService>,
) -> Result<(StatusCode, Json<DataService>), AppError> {
    let created = state.dataservices.create(body).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn get_dataservice(
    State(state): State<Arc<AppState>>,
    Path(service_id): Path<String>,
) -> Result<Json<DataService>, AppError> {
    Ok(Json(state.dataservices.get(&service_id).await?))
}

async fn update_dataservice(
    State(state): State<Arc<AppState>>,
    Path(service_id): Path<String>,
    Json(body): Json<DataService>,
) -> Result<StatusCode, AppError> {
    state.dataservices.update(&service_id, body).await?;
    Ok(StatusCode::OK)
}

async fn delete_dataservice(
    State(state): State<Arc<AppState>>,
    Path(service_id): Path<String>,
) -> Result<StatusCode, AppError> {
    state.dataservices.delete(&service_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

// --- DataService → Datasets ---

async fn list_service_datasets(
    State(state): State<Arc<AppState>>,
    Path(service_id): Path<String>,
    Query(filters): Query<DatasetFilters>,
) -> Result<Json<Vec<Dataset>>, AppError> {
    Ok(Json(
        state.dataservices.list_datasets(&service_id, filters).await?,
    ))
}

async fn create_service_dataset(
    State(state): State<Arc<AppState>>,
    Path(service_id): Path<String>,
    Json(body): Json<Dataset>,
) -> Result<StatusCode, AppError> {
    state.dataservices.create_dataset(&service_id, body).await?;
    Ok(StatusCode::CREATED)
}

async fn get_service_dataset(
    State(state): State<Arc<AppState>>,
    Path((service_id, dataset_id)): Path<(String, String)>,
) -> Result<Json<Dataset>, AppError> {
    Ok(Json(
        state.dataservices.get_dataset(&service_id, &dataset_id).await?,
    ))
}

async fn update_service_dataset(
    State(state): State<Arc<AppState>>,
    Path((service_id, dataset_id)): Path<(String, String)>,
    Json(body): Json<Dataset>,
) -> Result<StatusCode, AppError> {
    state
        .dataservices
        .update_dataset(&service_id, &dataset_id, body)
        .await?;
    Ok(StatusCode::OK)
}

async fn delete_service_dataset(
    State(state): State<Arc<AppState>>,
    Path((service_id, dataset_id)): Path<(String, String)>,
) -> Result<StatusCode, AppError> {
    state
        .dataservices
        .delete_dataset(&service_id, &dataset_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
