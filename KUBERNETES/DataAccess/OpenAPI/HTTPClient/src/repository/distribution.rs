use async_trait::async_trait;

use crate::{
    error::AppError,
    models::distribution::{Distribution, DistributionFilters},
};

#[async_trait]
pub trait DistributionRepository: Send + Sync {
    async fn list(&self, filters: DistributionFilters) -> Result<Vec<Distribution>, AppError>;
    async fn get(&self, distribution_id: &str) -> Result<Distribution, AppError>;
    async fn create(&self, dist: Distribution) -> Result<Distribution, AppError>;
    async fn update(
        &self,
        distribution_id: &str,
        dist: Distribution,
    ) -> Result<(), AppError>;
    async fn delete(&self, distribution_id: &str) -> Result<(), AppError>;
}
