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
    models::catalog::{Catalog, CatalogFilters, CatalogRecord},
    models::dataset::Dataset,
    repository::{
        catalog::{CatalogRecordRepository, CatalogRepository},
        dataset::DatasetRepository,
    },
};

pub struct CatalogRouter<C, CR, DS> {
    catalog_svc: Arc<C>,
    record_svc: Arc<CR>,
    dataset_svc: Arc<DS>,
}

impl<C, CR, DS> CatalogRouter<C, CR, DS>
where
    C: CatalogRepository + Send + Sync + 'static,
    CR: CatalogRecordRepository + Send + Sync + 'static,
    DS: DatasetRepository + Send + Sync + 'static,
{
    pub fn new(catalog_svc: Arc<C>, record_svc: Arc<CR>, dataset_svc: Arc<DS>) -> Self {
        Self { catalog_svc, record_svc, dataset_svc }
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/catalogs", get(Self::handle_list_catalogs).post(Self::handle_create_catalog))
            .route(
                "/catalogs/:catalog_id",
                get(Self::handle_get_catalog)
                    .put(Self::handle_update_catalog)
                    .delete(Self::handle_delete_catalog),
            )
            .route(
                "/catalogs/:catalog_id/datasets",
                get(Self::handle_list_catalog_datasets).post(Self::handle_create_catalog_dataset),
            )
            .route(
                "/catalogs/:catalog_id/datasets/:dataset_id",
                get(Self::handle_get_catalog_dataset)
                    .put(Self::handle_update_catalog_dataset)
                    .delete(Self::handle_delete_catalog_dataset),
            )
            .route(
                "/catalogs/:catalog_id/catalog_records",
                get(Self::handle_list_catalog_records),
            )
            .route(
                "/catalog_records",
                get(Self::handle_list_all_catalog_records).post(Self::handle_create_catalog_record),
            )
            .route(
                "/catalog_record/:record_id",
                get(Self::handle_get_catalog_record)
                    .put(Self::handle_update_catalog_record)
                    .delete(Self::handle_delete_catalog_record),
            )
            .with_state((self.catalog_svc, self.record_svc, self.dataset_svc))
    }

    // --- Catalog handlers ---

    async fn handle_list_catalogs(
        State((catalog_svc, _, _)): State<(Arc<C>, Arc<CR>, Arc<DS>)>,
        Query(filters): Query<CatalogFilters>,
    ) -> impl IntoResponse {
        info!("GET /catalogs");
        match catalog_svc.list(filters).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_create_catalog(
        State((catalog_svc, _, _)): State<(Arc<C>, Arc<CR>, Arc<DS>)>,
        Json(body): Json<Catalog>,
    ) -> impl IntoResponse {
        info!("POST /catalogs");
        match catalog_svc.create(body).await {
            Ok(v) => (StatusCode::CREATED, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_get_catalog(
        State((catalog_svc, _, _)): State<(Arc<C>, Arc<CR>, Arc<DS>)>,
        Path(catalog_id): Path<String>,
    ) -> impl IntoResponse {
        info!("GET /catalogs/{catalog_id}");
        match catalog_svc.get(&catalog_id).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_update_catalog(
        State((catalog_svc, _, _)): State<(Arc<C>, Arc<CR>, Arc<DS>)>,
        Path(catalog_id): Path<String>,
        Json(body): Json<Catalog>,
    ) -> impl IntoResponse {
        info!("PUT /catalogs/{catalog_id}");
        match catalog_svc.update(&catalog_id, body).await {
            Ok(()) => StatusCode::OK.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_delete_catalog(
        State((catalog_svc, _, _)): State<(Arc<C>, Arc<CR>, Arc<DS>)>,
        Path(catalog_id): Path<String>,
    ) -> impl IntoResponse {
        info!("DELETE /catalogs/{catalog_id}");
        match catalog_svc.delete(&catalog_id).await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => e.into_response(),
        }
    }

    // --- Catalog → Datasets ---

    async fn handle_list_catalog_datasets(
        State((catalog_svc, _, _)): State<(Arc<C>, Arc<CR>, Arc<DS>)>,
        Path(catalog_id): Path<String>,
    ) -> impl IntoResponse {
        info!("GET /catalogs/{catalog_id}/datasets");
        match catalog_svc.list_datasets(&catalog_id).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_create_catalog_dataset(
        State((_, _, dataset_svc)): State<(Arc<C>, Arc<CR>, Arc<DS>)>,
        Path(_catalog_id): Path<String>,
        Json(body): Json<Dataset>,
    ) -> impl IntoResponse {
        info!("POST /catalogs/{_catalog_id}/datasets");
        match dataset_svc.create(body).await {
            Ok(_) => StatusCode::CREATED.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_get_catalog_dataset(
        State((_, _, dataset_svc)): State<(Arc<C>, Arc<CR>, Arc<DS>)>,
        Path((_catalog_id, dataset_id)): Path<(String, String)>,
    ) -> impl IntoResponse {
        info!("GET /catalogs/{_catalog_id}/datasets/{dataset_id}");
        match dataset_svc.get(&dataset_id).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_update_catalog_dataset(
        State((_, _, dataset_svc)): State<(Arc<C>, Arc<CR>, Arc<DS>)>,
        Path((_catalog_id, dataset_id)): Path<(String, String)>,
        Json(body): Json<Dataset>,
    ) -> impl IntoResponse {
        info!("PUT /catalogs/{_catalog_id}/datasets/{dataset_id}");
        match dataset_svc.update(&dataset_id, body).await {
            Ok(()) => StatusCode::OK.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_delete_catalog_dataset(
        State((_, _, dataset_svc)): State<(Arc<C>, Arc<CR>, Arc<DS>)>,
        Path((_catalog_id, dataset_id)): Path<(String, String)>,
    ) -> impl IntoResponse {
        info!("DELETE /catalogs/{_catalog_id}/datasets/{dataset_id}");
        match dataset_svc.delete(&dataset_id).await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => e.into_response(),
        }
    }

    // --- CatalogRecord handlers ---

    async fn handle_list_catalog_records(
        State((_, record_svc, _)): State<(Arc<C>, Arc<CR>, Arc<DS>)>,
        Path(catalog_id): Path<String>,
    ) -> impl IntoResponse {
        info!("GET /catalogs/{catalog_id}/catalog_records");
        match record_svc.list_for_catalog(&catalog_id).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_list_all_catalog_records(
        State((_, record_svc, _)): State<(Arc<C>, Arc<CR>, Arc<DS>)>,
        Query(params): Query<std::collections::HashMap<String, String>>,
    ) -> impl IntoResponse {
        info!("GET /catalog_records");
        let catalog_id = params.get("catalog_id").map(String::as_str);
        match record_svc.list(catalog_id).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_create_catalog_record(
        State((_, record_svc, _)): State<(Arc<C>, Arc<CR>, Arc<DS>)>,
        Json(body): Json<CatalogRecord>,
    ) -> impl IntoResponse {
        info!("POST /catalog_records");
        match record_svc.create(body).await {
            Ok(v) => (StatusCode::CREATED, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_get_catalog_record(
        State((_, record_svc, _)): State<(Arc<C>, Arc<CR>, Arc<DS>)>,
        Path(record_id): Path<String>,
    ) -> impl IntoResponse {
        info!("GET /catalog_record/{record_id}");
        match record_svc.get(&record_id).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_update_catalog_record(
        State((_, record_svc, _)): State<(Arc<C>, Arc<CR>, Arc<DS>)>,
        Path(record_id): Path<String>,
        Json(body): Json<CatalogRecord>,
    ) -> impl IntoResponse {
        info!("PUT /catalog_record/{record_id}");
        match record_svc.update(&record_id, body).await {
            Ok(()) => StatusCode::OK.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_delete_catalog_record(
        State((_, record_svc, _)): State<(Arc<C>, Arc<CR>, Arc<DS>)>,
        Path(record_id): Path<String>,
    ) -> impl IntoResponse {
        info!("DELETE /catalog_record/{record_id}");
        match record_svc.delete(&record_id).await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => e.into_response(),
        }
    }
}
