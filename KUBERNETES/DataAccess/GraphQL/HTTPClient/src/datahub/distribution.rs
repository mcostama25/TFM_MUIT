use anyhow::anyhow;
use async_trait::async_trait;

use crate::{
    error::AppError,
    models::distribution::{Distribution, DistributionFilters},
    repository::distribution::DistributionRepository,
};

use super::{
    client::DatahubClient,
    mapping::{distribution_to_dh_payload, dh_entity_to_distribution},
};

#[async_trait]
impl DistributionRepository for DatahubClient {
    async fn list(&self, _filters: DistributionFilters) -> Result<Vec<Distribution>, AppError> {
        // Distributions are datasets with the dcat_distribution custom property set
        let entities = self
            .list_entities(
                "dataset",
                &["datasetProperties"],
                &[("query", "customProperties.dcat_distribution:true")],
            )
            .await?;
        Ok(entities.iter().map(dh_entity_to_distribution).collect())
    }

    async fn get(&self, distribution_id: &str) -> Result<Distribution, AppError> {
        let urn = format!(
            "urn:li:dataset:(urn:li:dataPlatform:dcat,dist-{distribution_id},PROD)"
        );
        let entity = self
            .get_entity("dataset", &urn, &["datasetProperties"])
            .await
            .map_err(|e| {
                let msg = e.to_string();
                if msg.contains("NOT_FOUND") {
                    AppError::NotFound(format!("Distribution not found: {distribution_id}"))
                } else {
                    AppError::Internal(anyhow!(msg))
                }
            })?;
        Ok(dh_entity_to_distribution(&entity))
    }

    async fn create(&self, dist: Distribution) -> Result<Distribution, AppError> {
        let payload = distribution_to_dh_payload(&dist);
        let entity = self.upsert_entity("dataset", payload).await?;
        Ok(dh_entity_to_distribution(&entity))
    }

    async fn update(
        &self,
        distribution_id: &str,
        dist: Distribution,
    ) -> Result<(), AppError> {
        let mut updated = dist;
        updated.dcterms_identifier = Some(distribution_id.to_string());
        let payload = distribution_to_dh_payload(&updated);
        self.upsert_entity("dataset", payload).await?;
        Ok(())
    }

    async fn delete(&self, distribution_id: &str) -> Result<(), AppError> {
        let urn = format!(
            "urn:li:dataset:(urn:li:dataPlatform:dcat,dist-{distribution_id},PROD)"
        );
        self.delete_entity("dataset", &urn).await?;
        Ok(())
    }
}
