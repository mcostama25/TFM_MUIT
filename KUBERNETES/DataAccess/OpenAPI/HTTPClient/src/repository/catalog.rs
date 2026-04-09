use async_trait::async_trait;

use crate::{
    error::AppError,
    models::catalog::{Catalog, CatalogFilters, CatalogRecord},
    models::dataset::Dataset,
};

#[async_trait]
pub trait CatalogRepository: Send + Sync {
    async fn list(&self, filters: CatalogFilters) -> Result<Vec<Catalog>, AppError>;
    async fn get(&self, catalog_id: &str) -> Result<Catalog, AppError>;
    async fn create(&self, catalog: Catalog) -> Result<Catalog, AppError>;
    async fn update(&self, catalog_id: &str, catalog: Catalog) -> Result<(), AppError>;
    async fn delete(&self, catalog_id: &str) -> Result<(), AppError>;
    async fn list_datasets(&self, catalog_id: &str) -> Result<Vec<Dataset>, AppError>;
}

#[async_trait]
pub trait CatalogRecordRepository: Send + Sync {
    async fn list(&self, catalog_id: Option<&str>) -> Result<Vec<CatalogRecord>, AppError>;
    async fn list_for_catalog(&self, catalog_id: &str) -> Result<Vec<CatalogRecord>, AppError>;
    async fn get(&self, record_id: &str) -> Result<CatalogRecord, AppError>;
    async fn create(&self, record: CatalogRecord) -> Result<CatalogRecord, AppError>;
    async fn update(&self, record_id: &str, record: CatalogRecord) -> Result<(), AppError>;
    async fn delete(&self, record_id: &str) -> Result<(), AppError>;
}
