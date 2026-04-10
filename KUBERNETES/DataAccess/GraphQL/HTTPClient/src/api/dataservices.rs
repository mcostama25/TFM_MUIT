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
    models::{
        dataservice::{DataService, DataServiceFilters},
        dataset::{Dataset, DatasetFilters},
    },
    repository::dataservice::DataServiceRepository,
};

pub struct DataServiceRouter<SVC> {
    svc: Arc<SVC>,
}

impl<SVC> DataServiceRouter<SVC>
where
    SVC: DataServiceRepository + Send + Sync + 'static,
{
    pub fn new(svc: Arc<SVC>) -> Self {
        Self { svc }
    }

    pub fn router(self) -> Router {
        Router::new()
            .route(
                "/dataservices",
                get(Self::handle_list_dataservices).post(Self::handle_create_dataservice),
            )
            .route(
                "/dataservices/:service_id",
                get(Self::handle_get_dataservice)
                    .put(Self::handle_update_dataservice)
                    .delete(Self::handle_delete_dataservice),
            )
            .route(
                "/dataservices/:service_id/datasets",
                get(Self::handle_list_service_datasets).post(Self::handle_create_service_dataset),
            )
            .route(
                "/dataservices/:service_id/datasets/:dataset_id",
                get(Self::handle_get_service_dataset)
                    .put(Self::handle_update_service_dataset)
                    .delete(Self::handle_delete_service_dataset),
            )
            .with_state(self.svc)
    }

    async fn handle_list_dataservices(
        State(svc): State<Arc<SVC>>,
        Query(filters): Query<DataServiceFilters>,
    ) -> impl IntoResponse {
        info!("GET /dataservices");
        match svc.list(filters).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_create_dataservice(
        State(svc): State<Arc<SVC>>,
        Json(body): Json<DataService>,
    ) -> impl IntoResponse {
        info!("POST /dataservices");
        match svc.create(body).await {
            Ok(v) => (StatusCode::CREATED, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_get_dataservice(
        State(svc): State<Arc<SVC>>,
        Path(service_id): Path<String>,
    ) -> impl IntoResponse {
        info!("GET /dataservices/{service_id}");
        match svc.get(&service_id).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_update_dataservice(
        State(svc): State<Arc<SVC>>,
        Path(service_id): Path<String>,
        Json(body): Json<DataService>,
    ) -> impl IntoResponse {
        info!("PUT /dataservices/{service_id}");
        match svc.update(&service_id, body).await {
            Ok(()) => StatusCode::OK.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_delete_dataservice(
        State(svc): State<Arc<SVC>>,
        Path(service_id): Path<String>,
    ) -> impl IntoResponse {
        info!("DELETE /dataservices/{service_id}");
        match svc.delete(&service_id).await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_list_service_datasets(
        State(svc): State<Arc<SVC>>,
        Path(service_id): Path<String>,
        Query(filters): Query<DatasetFilters>,
    ) -> impl IntoResponse {
        info!("GET /dataservices/{service_id}/datasets");
        match svc.list_datasets(&service_id, filters).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_create_service_dataset(
        State(svc): State<Arc<SVC>>,
        Path(service_id): Path<String>,
        Json(body): Json<Dataset>,
    ) -> impl IntoResponse {
        info!("POST /dataservices/{service_id}/datasets");
        match svc.create_dataset(&service_id, body).await {
            Ok(_) => StatusCode::CREATED.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_get_service_dataset(
        State(svc): State<Arc<SVC>>,
        Path((service_id, dataset_id)): Path<(String, String)>,
    ) -> impl IntoResponse {
        info!("GET /dataservices/{service_id}/datasets/{dataset_id}");
        match svc.get_dataset(&service_id, &dataset_id).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_update_service_dataset(
        State(svc): State<Arc<SVC>>,
        Path((service_id, dataset_id)): Path<(String, String)>,
        Json(body): Json<Dataset>,
    ) -> impl IntoResponse {
        info!("PUT /dataservices/{service_id}/datasets/{dataset_id}");
        match svc.update_dataset(&service_id, &dataset_id, body).await {
            Ok(()) => StatusCode::OK.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_delete_service_dataset(
        State(svc): State<Arc<SVC>>,
        Path((service_id, dataset_id)): Path<(String, String)>,
    ) -> impl IntoResponse {
        info!("DELETE /dataservices/{service_id}/datasets/{dataset_id}");
        match svc.delete_dataset(&service_id, &dataset_id).await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => e.into_response(),
        }
    }
}
