use async_trait::async_trait;

use crate::{
    error::AppError,
    models::{
        dataset::{Dataset, DatasetFilters, DatasetSeries},
        distribution::Distribution,
        dataservice::DataService,
    },
};

#[async_trait]
pub trait DatasetRepository: Send + Sync {
    async fn list(&self, filters: DatasetFilters) -> Result<Vec<Dataset>, AppError>;
    async fn get(&self, dataset_id: &str) -> Result<Dataset, AppError>;
    async fn create(&self, dataset: Dataset) -> Result<Dataset, AppError>;
    async fn update(&self, dataset_id: &str, dataset: Dataset) -> Result<(), AppError>;
    async fn delete(&self, dataset_id: &str) -> Result<(), AppError>;
    async fn list_distributions(
        &self,
        dataset_id: &str,
    ) -> Result<Vec<Distribution>, AppError>;
    async fn list_dataservices(
        &self,
        dataset_id: &str,
    ) -> Result<Vec<DataService>, AppError>;
}

#[async_trait]
pub trait DatasetSeriesRepository: Send + Sync {
    async fn list(&self) -> Result<Vec<DatasetSeries>, AppError>;
    async fn get(&self, series_id: &str) -> Result<DatasetSeries, AppError>;
    async fn create(&self, series: DatasetSeries) -> Result<DatasetSeries, AppError>;
    async fn update(&self, series_id: &str, series: DatasetSeries) -> Result<(), AppError>;
    async fn delete(&self, series_id: &str) -> Result<(), AppError>;
    async fn list_datasets(
        &self,
        series_id: &str,
        filters: DatasetFilters,
    ) -> Result<Vec<Dataset>, AppError>;
}
