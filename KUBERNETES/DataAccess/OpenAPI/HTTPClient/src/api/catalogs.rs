use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};

use crate::{
    error::AppError,
    models::catalog::{Catalog, CatalogFilters, CatalogRecord},
    models::dataset::Dataset,
};

use super::AppState;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        // Catalog collection
        .route("/catalogs", get(list_catalogs).post(create_catalog))
        // Catalog item
        .route(
            "/catalogs/:catalog_id",
            get(get_catalog).put(update_catalog).delete(delete_catalog),
        )
        // Catalog → Datasets
        .route("/catalogs/:catalog_id/datasets", get(list_catalog_datasets).post(create_catalog_dataset))
        .route(
            "/catalogs/:catalog_id/datasets/:dataset_id",
            get(get_catalog_dataset)
                .put(update_catalog_dataset)
                .delete(delete_catalog_dataset),
        )
        // Catalog → CatalogRecords
        .route("/catalogs/:catalog_id/catalog_records", get(list_catalog_records))
        // CatalogRecords collection
        .route("/catalog_records", get(list_all_catalog_records).post(create_catalog_record))
        .route(
            "/catalog_record/:record_id",
            get(get_catalog_record)
                .put(update_catalog_record)
                .delete(delete_catalog_record),
        )
}

// --- Catalog handlers ---

async fn list_catalogs(
    State(state): State<Arc<AppState>>,
    Query(filters): Query<CatalogFilters>,
) -> Result<Json<Vec<Catalog>>, AppError> {
    Ok(Json(state.catalogs.list(filters).await?))
}

async fn create_catalog(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Catalog>,
) -> Result<(StatusCode, Json<Catalog>), AppError> {
    let created = state.catalogs.create(body).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn get_catalog(
    State(state): State<Arc<AppState>>,
    Path(catalog_id): Path<String>,
) -> Result<Json<Catalog>, AppError> {
    Ok(Json(state.catalogs.get(&catalog_id).await?))
}

async fn update_catalog(
    State(state): State<Arc<AppState>>,
    Path(catalog_id): Path<String>,
    Json(body): Json<Catalog>,
) -> Result<StatusCode, AppError> {
    state.catalogs.update(&catalog_id, body).await?;
    Ok(StatusCode::OK)
}

async fn delete_catalog(
    State(state): State<Arc<AppState>>,
    Path(catalog_id): Path<String>,
) -> Result<StatusCode, AppError> {
    state.catalogs.delete(&catalog_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

// --- Catalog → Datasets ---

async fn list_catalog_datasets(
    State(state): State<Arc<AppState>>,
    Path(catalog_id): Path<String>,
) -> Result<Json<Vec<Dataset>>, AppError> {
    Ok(Json(state.catalogs.list_datasets(&catalog_id).await?))
}

async fn create_catalog_dataset(
    State(state): State<Arc<AppState>>,
    Path(_catalog_id): Path<String>,
    Json(body): Json<Dataset>,
) -> Result<StatusCode, AppError> {
    state.datasets.create(body).await?;
    Ok(StatusCode::CREATED)
}

async fn get_catalog_dataset(
    State(state): State<Arc<AppState>>,
    Path((_catalog_id, dataset_id)): Path<(String, String)>,
) -> Result<Json<Dataset>, AppError> {
    Ok(Json(state.datasets.get(&dataset_id).await?))
}

async fn update_catalog_dataset(
    State(state): State<Arc<AppState>>,
    Path((_catalog_id, dataset_id)): Path<(String, String)>,
    Json(body): Json<Dataset>,
) -> Result<StatusCode, AppError> {
    state.datasets.update(&dataset_id, body).await?;
    Ok(StatusCode::OK)
}

async fn delete_catalog_dataset(
    State(state): State<Arc<AppState>>,
    Path((_catalog_id, dataset_id)): Path<(String, String)>,
) -> Result<StatusCode, AppError> {
    state.datasets.delete(&dataset_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

// --- CatalogRecord handlers ---

async fn list_catalog_records(
    State(state): State<Arc<AppState>>,
    Path(catalog_id): Path<String>,
) -> Result<Json<Vec<CatalogRecord>>, AppError> {
    Ok(Json(
        state.catalog_records.list_for_catalog(&catalog_id).await?,
    ))
}

async fn list_all_catalog_records(
    State(state): State<Arc<AppState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Vec<CatalogRecord>>, AppError> {
    let catalog_id = params.get("catalog_id").map(String::as_str);
    Ok(Json(state.catalog_records.list(catalog_id).await?))
}

async fn create_catalog_record(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CatalogRecord>,
) -> Result<(StatusCode, Json<CatalogRecord>), AppError> {
    let created = state.catalog_records.create(body).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn get_catalog_record(
    State(state): State<Arc<AppState>>,
    Path(record_id): Path<String>,
) -> Result<Json<CatalogRecord>, AppError> {
    Ok(Json(state.catalog_records.get(&record_id).await?))
}

async fn update_catalog_record(
    State(state): State<Arc<AppState>>,
    Path(record_id): Path<String>,
    Json(body): Json<CatalogRecord>,
) -> Result<StatusCode, AppError> {
    state.catalog_records.update(&record_id, body).await?;
    Ok(StatusCode::OK)
}

async fn delete_catalog_record(
    State(state): State<Arc<AppState>>,
    Path(record_id): Path<String>,
) -> Result<StatusCode, AppError> {
    state.catalog_records.delete(&record_id).await?;
    Ok(StatusCode::NO_CONTENT)
}
