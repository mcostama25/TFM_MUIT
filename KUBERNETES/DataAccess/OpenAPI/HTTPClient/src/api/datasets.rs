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
        dataservice::DataService,
        dataset::{Dataset, DatasetFilters, DatasetSeries},
        distribution::Distribution,
    },
};

use super::AppState;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        // Dataset collection
        .route("/datasets", get(list_datasets).post(create_dataset))
        .route(
            "/datasets/:dataset_id",
            get(get_dataset).put(update_dataset).delete(delete_dataset),
        )
        // Dataset → Distributions
        .route(
            "/datasets/:dataset_id/distributions",
            get(list_dataset_distributions).post(create_dataset_distribution),
        )
        .route(
            "/datasets/:dataset_id/distributions/:distribution_id",
            get(get_dataset_distribution)
                .put(update_dataset_distribution)
                .delete(delete_dataset_distribution),
        )
        // Dataset → DataServices
        .route(
            "/datasets/:dataset_id/dataservices",
            get(list_dataset_dataservices).post(create_dataset_dataservice),
        )
        // DatasetSeries
        .route(
            "/datasetseries",
            get(list_dataset_series).post(create_dataset_series),
        )
        .route(
            "/datasetseries/:series_id",
            get(get_dataset_series)
                .put(update_dataset_series)
                .delete(delete_dataset_series),
        )
        .route(
            "/datasetseries/:series_id/datasets",
            get(list_series_datasets),
        )
}

// --- Dataset handlers ---

async fn list_datasets(
    State(state): State<Arc<AppState>>,
    Query(filters): Query<DatasetFilters>,
) -> Result<Json<Vec<Dataset>>, AppError> {
    Ok(Json(state.datasets.list(filters).await?))
}

async fn create_dataset(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Dataset>,
) -> Result<(StatusCode, Json<Dataset>), AppError> {
    let created = state.datasets.create(body).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn get_dataset(
    State(state): State<Arc<AppState>>,
    Path(dataset_id): Path<String>,
) -> Result<Json<Dataset>, AppError> {
    Ok(Json(state.datasets.get(&dataset_id).await?))
}

async fn update_dataset(
    State(state): State<Arc<AppState>>,
    Path(dataset_id): Path<String>,
    Json(body): Json<Dataset>,
) -> Result<StatusCode, AppError> {
    state.datasets.update(&dataset_id, body).await?;
    Ok(StatusCode::OK)
}

async fn delete_dataset(
    State(state): State<Arc<AppState>>,
    Path(dataset_id): Path<String>,
) -> Result<StatusCode, AppError> {
    state.datasets.delete(&dataset_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

// --- Dataset → Distributions ---

async fn list_dataset_distributions(
    State(state): State<Arc<AppState>>,
    Path(dataset_id): Path<String>,
) -> Result<Json<Vec<Distribution>>, AppError> {
    Ok(Json(
        state.datasets.list_distributions(&dataset_id).await?,
    ))
}

async fn create_dataset_distribution(
    State(state): State<Arc<AppState>>,
    Path(_dataset_id): Path<String>,
    Json(body): Json<Distribution>,
) -> Result<StatusCode, AppError> {
    state.distributions.create(body).await?;
    Ok(StatusCode::CREATED)
}

async fn get_dataset_distribution(
    State(state): State<Arc<AppState>>,
    Path((_dataset_id, distribution_id)): Path<(String, String)>,
) -> Result<Json<Distribution>, AppError> {
    Ok(Json(state.distributions.get(&distribution_id).await?))
}

async fn update_dataset_distribution(
    State(state): State<Arc<AppState>>,
    Path((_dataset_id, distribution_id)): Path<(String, String)>,
    Json(body): Json<Distribution>,
) -> Result<StatusCode, AppError> {
    state.distributions.update(&distribution_id, body).await?;
    Ok(StatusCode::OK)
}

async fn delete_dataset_distribution(
    State(state): State<Arc<AppState>>,
    Path((_dataset_id, distribution_id)): Path<(String, String)>,
) -> Result<StatusCode, AppError> {
    state.distributions.delete(&distribution_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

// --- Dataset → DataServices ---

async fn list_dataset_dataservices(
    State(state): State<Arc<AppState>>,
    Path(dataset_id): Path<String>,
) -> Result<Json<Vec<DataService>>, AppError> {
    Ok(Json(
        state.datasets.list_dataservices(&dataset_id).await?,
    ))
}

async fn create_dataset_dataservice(
    State(state): State<Arc<AppState>>,
    Path(_dataset_id): Path<String>,
    Json(body): Json<DataService>,
) -> Result<StatusCode, AppError> {
    state.dataservices.create(body).await?;
    Ok(StatusCode::CREATED)
}

// --- DatasetSeries handlers ---

async fn list_dataset_series(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<DatasetSeries>>, AppError> {
    Ok(Json(state.dataset_series.list().await?))
}

async fn create_dataset_series(
    State(state): State<Arc<AppState>>,
    Json(body): Json<DatasetSeries>,
) -> Result<(StatusCode, Json<DatasetSeries>), AppError> {
    let created = state.dataset_series.create(body).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn get_dataset_series(
    State(state): State<Arc<AppState>>,
    Path(series_id): Path<String>,
) -> Result<Json<DatasetSeries>, AppError> {
    Ok(Json(state.dataset_series.get(&series_id).await?))
}

async fn update_dataset_series(
    State(state): State<Arc<AppState>>,
    Path(series_id): Path<String>,
    Json(body): Json<DatasetSeries>,
) -> Result<StatusCode, AppError> {
    state.dataset_series.update(&series_id, body).await?;
    Ok(StatusCode::OK)
}

async fn delete_dataset_series(
    State(state): State<Arc<AppState>>,
    Path(series_id): Path<String>,
) -> Result<StatusCode, AppError> {
    state.dataset_series.delete(&series_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_series_datasets(
    State(state): State<Arc<AppState>>,
    Path(series_id): Path<String>,
    Query(filters): Query<DatasetFilters>,
) -> Result<Json<Vec<Dataset>>, AppError> {
    Ok(Json(
        state.dataset_series.list_datasets(&series_id, filters).await?,
    ))
}
