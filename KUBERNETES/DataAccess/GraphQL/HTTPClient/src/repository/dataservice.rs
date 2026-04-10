use async_trait::async_trait;

use crate::{
    error::AppError,
    models::{
        dataservice::{DataService, DataServiceFilters},
        dataset::{Dataset, DatasetFilters},
    },
};

#[async_trait]
pub trait DataServiceRepository: Send + Sync {
    async fn list(&self, filters: DataServiceFilters) -> Result<Vec<DataService>, AppError>;
    async fn get(&self, service_id: &str) -> Result<DataService, AppError>;
    async fn create(&self, service: DataService) -> Result<DataService, AppError>;
    async fn update(&self, service_id: &str, service: DataService) -> Result<(), AppError>;
    async fn delete(&self, service_id: &str) -> Result<(), AppError>;
    async fn list_datasets(
        &self,
        service_id: &str,
        filters: DatasetFilters,
    ) -> Result<Vec<Dataset>, AppError>;
    async fn get_dataset(
        &self,
        service_id: &str,
        dataset_id: &str,
    ) -> Result<Dataset, AppError>;
    async fn create_dataset(
        &self,
        service_id: &str,
        dataset: Dataset,
    ) -> Result<Dataset, AppError>;
    async fn update_dataset(
        &self,
        service_id: &str,
        dataset_id: &str,
        dataset: Dataset,
    ) -> Result<(), AppError>;
    async fn delete_dataset(
        &self,
        service_id: &str,
        dataset_id: &str,
    ) -> Result<(), AppError>;
}
