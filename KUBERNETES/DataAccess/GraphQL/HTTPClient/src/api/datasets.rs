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
        dataservice::DataService,
        dataset::{Dataset, DatasetFilters, DatasetSeries},
        distribution::Distribution,
    },
    repository::{
        dataservice::DataServiceRepository,
        dataset::{DatasetRepository, DatasetSeriesRepository},
        distribution::DistributionRepository,
    },
};

pub struct DatasetRouter<DS, DI, SVC, SS> {
    dataset_svc: Arc<DS>,
    dist_svc: Arc<DI>,
    svc_svc: Arc<SVC>,
    series_svc: Arc<SS>,
}

impl<DS, DI, SVC, SS> DatasetRouter<DS, DI, SVC, SS>
where
    DS: DatasetRepository + Send + Sync + 'static,
    DI: DistributionRepository + Send + Sync + 'static,
    SVC: DataServiceRepository + Send + Sync + 'static,
    SS: DatasetSeriesRepository + Send + Sync + 'static,
{
    pub fn new(dataset_svc: Arc<DS>, dist_svc: Arc<DI>, svc_svc: Arc<SVC>, series_svc: Arc<SS>) -> Self {
        Self { dataset_svc, dist_svc, svc_svc, series_svc }
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/datasets", get(Self::handle_list_datasets).post(Self::handle_create_dataset))
            .route(
                "/datasets/:dataset_id",
                get(Self::handle_get_dataset)
                    .put(Self::handle_update_dataset)
                    .delete(Self::handle_delete_dataset),
            )
            .route(
                "/datasets/:dataset_id/distributions",
                get(Self::handle_list_dataset_distributions).post(Self::handle_create_dataset_distribution),
            )
            .route(
                "/datasets/:dataset_id/distributions/:distribution_id",
                get(Self::handle_get_dataset_distribution)
                    .put(Self::handle_update_dataset_distribution)
                    .delete(Self::handle_delete_dataset_distribution),
            )
            .route(
                "/datasets/:dataset_id/dataservices",
                get(Self::handle_list_dataset_dataservices).post(Self::handle_create_dataset_dataservice),
            )
            .route(
                "/datasetseries",
                get(Self::handle_list_dataset_series).post(Self::handle_create_dataset_series),
            )
            .route(
                "/datasetseries/:series_id",
                get(Self::handle_get_dataset_series)
                    .put(Self::handle_update_dataset_series)
                    .delete(Self::handle_delete_dataset_series),
            )
            .route("/datasetseries/:series_id/datasets", get(Self::handle_list_series_datasets))
            .with_state((self.dataset_svc, self.dist_svc, self.svc_svc, self.series_svc))
    }

    async fn handle_list_datasets(
        State((dataset_svc, _, _, _)): State<(Arc<DS>, Arc<DI>, Arc<SVC>, Arc<SS>)>,
        Query(filters): Query<DatasetFilters>,
    ) -> impl IntoResponse {
        info!("GET /datasets");
        match dataset_svc.list(filters).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_create_dataset(
        State((dataset_svc, _, _, _)): State<(Arc<DS>, Arc<DI>, Arc<SVC>, Arc<SS>)>,
        Json(body): Json<Dataset>,
    ) -> impl IntoResponse {
        info!("POST /datasets");
        match dataset_svc.create(body).await {
            Ok(v) => (StatusCode::CREATED, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_get_dataset(
        State((dataset_svc, _, _, _)): State<(Arc<DS>, Arc<DI>, Arc<SVC>, Arc<SS>)>,
        Path(dataset_id): Path<String>,
    ) -> impl IntoResponse {
        info!("GET /datasets/{dataset_id}");
        match dataset_svc.get(&dataset_id).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_update_dataset(
        State((dataset_svc, _, _, _)): State<(Arc<DS>, Arc<DI>, Arc<SVC>, Arc<SS>)>,
        Path(dataset_id): Path<String>,
        Json(body): Json<Dataset>,
    ) -> impl IntoResponse {
        info!("PUT /datasets/{dataset_id}");
        match dataset_svc.update(&dataset_id, body).await {
            Ok(()) => StatusCode::OK.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_delete_dataset(
        State((dataset_svc, _, _, _)): State<(Arc<DS>, Arc<DI>, Arc<SVC>, Arc<SS>)>,
        Path(dataset_id): Path<String>,
    ) -> impl IntoResponse {
        info!("DELETE /datasets/{dataset_id}");
        match dataset_svc.delete(&dataset_id).await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_list_dataset_distributions(
        State((dataset_svc, _, _, _)): State<(Arc<DS>, Arc<DI>, Arc<SVC>, Arc<SS>)>,
        Path(dataset_id): Path<String>,
    ) -> impl IntoResponse {
        info!("GET /datasets/{dataset_id}/distributions");
        match dataset_svc.list_distributions(&dataset_id).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_create_dataset_distribution(
        State((_, dist_svc, _, _)): State<(Arc<DS>, Arc<DI>, Arc<SVC>, Arc<SS>)>,
        Path(_dataset_id): Path<String>,
        Json(body): Json<Distribution>,
    ) -> impl IntoResponse {
        info!("POST /datasets/{_dataset_id}/distributions");
        match dist_svc.create(body).await {
            Ok(_) => StatusCode::CREATED.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_get_dataset_distribution(
        State((_, dist_svc, _, _)): State<(Arc<DS>, Arc<DI>, Arc<SVC>, Arc<SS>)>,
        Path((_dataset_id, distribution_id)): Path<(String, String)>,
    ) -> impl IntoResponse {
        info!("GET /datasets/{_dataset_id}/distributions/{distribution_id}");
        match dist_svc.get(&distribution_id).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_update_dataset_distribution(
        State((_, dist_svc, _, _)): State<(Arc<DS>, Arc<DI>, Arc<SVC>, Arc<SS>)>,
        Path((_dataset_id, distribution_id)): Path<(String, String)>,
        Json(body): Json<Distribution>,
    ) -> impl IntoResponse {
        info!("PUT /datasets/{_dataset_id}/distributions/{distribution_id}");
        match dist_svc.update(&distribution_id, body).await {
            Ok(()) => StatusCode::OK.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_delete_dataset_distribution(
        State((_, dist_svc, _, _)): State<(Arc<DS>, Arc<DI>, Arc<SVC>, Arc<SS>)>,
        Path((_dataset_id, distribution_id)): Path<(String, String)>,
    ) -> impl IntoResponse {
        info!("DELETE /datasets/{_dataset_id}/distributions/{distribution_id}");
        match dist_svc.delete(&distribution_id).await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_list_dataset_dataservices(
        State((dataset_svc, _, _, _)): State<(Arc<DS>, Arc<DI>, Arc<SVC>, Arc<SS>)>,
        Path(dataset_id): Path<String>,
    ) -> impl IntoResponse {
        info!("GET /datasets/{dataset_id}/dataservices");
        match dataset_svc.list_dataservices(&dataset_id).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_create_dataset_dataservice(
        State((_, _, svc_svc, _)): State<(Arc<DS>, Arc<DI>, Arc<SVC>, Arc<SS>)>,
        Path(_dataset_id): Path<String>,
        Json(body): Json<DataService>,
    ) -> impl IntoResponse {
        info!("POST /datasets/{_dataset_id}/dataservices");
        match svc_svc.create(body).await {
            Ok(_) => StatusCode::CREATED.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_list_dataset_series(
        State((_, _, _, series_svc)): State<(Arc<DS>, Arc<DI>, Arc<SVC>, Arc<SS>)>,
    ) -> impl IntoResponse {
        info!("GET /datasetseries");
        match series_svc.list().await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_create_dataset_series(
        State((_, _, _, series_svc)): State<(Arc<DS>, Arc<DI>, Arc<SVC>, Arc<SS>)>,
        Json(body): Json<DatasetSeries>,
    ) -> impl IntoResponse {
        info!("POST /datasetseries");
        match series_svc.create(body).await {
            Ok(v) => (StatusCode::CREATED, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_get_dataset_series(
        State((_, _, _, series_svc)): State<(Arc<DS>, Arc<DI>, Arc<SVC>, Arc<SS>)>,
        Path(series_id): Path<String>,
    ) -> impl IntoResponse {
        info!("GET /datasetseries/{series_id}");
        match series_svc.get(&series_id).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_update_dataset_series(
        State((_, _, _, series_svc)): State<(Arc<DS>, Arc<DI>, Arc<SVC>, Arc<SS>)>,
        Path(series_id): Path<String>,
        Json(body): Json<DatasetSeries>,
    ) -> impl IntoResponse {
        info!("PUT /datasetseries/{series_id}");
        match series_svc.update(&series_id, body).await {
            Ok(()) => StatusCode::OK.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_delete_dataset_series(
        State((_, _, _, series_svc)): State<(Arc<DS>, Arc<DI>, Arc<SVC>, Arc<SS>)>,
        Path(series_id): Path<String>,
    ) -> impl IntoResponse {
        info!("DELETE /datasetseries/{series_id}");
        match series_svc.delete(&series_id).await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_list_series_datasets(
        State((_, _, _, series_svc)): State<(Arc<DS>, Arc<DI>, Arc<SVC>, Arc<SS>)>,
        Path(series_id): Path<String>,
        Query(filters): Query<DatasetFilters>,
    ) -> impl IntoResponse {
        info!("GET /datasetseries/{series_id}/datasets");
        match series_svc.list_datasets(&series_id, filters).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }
}
