pub mod catalogs;
pub mod common;
pub mod dataservices;
pub mod datasets;
pub mod distributions;

use std::sync::Arc;

use axum::Router;

use crate::repository::{
    catalog::{CatalogRecordRepository, CatalogRepository},
    common::{
        KeywordRepository, QualifiedRelationRepository, ReferenceRepository, RelationRepository,
        ResourceRepository, ThemeRepository,
    },
    dataservice::DataServiceRepository,
    dataset::{DatasetRepository, DatasetSeriesRepository},
    distribution::DistributionRepository,
};

/// Shared application state injected into every Axum handler via `State<Arc<AppState>>`.
pub struct AppState {
    pub catalogs: Arc<dyn CatalogRepository>,
    pub catalog_records: Arc<dyn CatalogRecordRepository>,
    pub datasets: Arc<dyn DatasetRepository>,
    pub dataset_series: Arc<dyn DatasetSeriesRepository>,
    pub distributions: Arc<dyn DistributionRepository>,
    pub dataservices: Arc<dyn DataServiceRepository>,
    pub keywords: Arc<dyn KeywordRepository>,
    pub themes: Arc<dyn ThemeRepository>,
    pub references: Arc<dyn ReferenceRepository>,
    pub relations: Arc<dyn RelationRepository>,
    pub qualified_relations: Arc<dyn QualifiedRelationRepository>,
    pub resources: Arc<dyn ResourceRepository>,
}

/// Build the full Axum router by merging all domain sub-routers.
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .merge(catalogs::router())
        .merge(datasets::router())
        .merge(distributions::router())
        .merge(dataservices::router())
        .merge(common::router())
        .with_state(state)
}
