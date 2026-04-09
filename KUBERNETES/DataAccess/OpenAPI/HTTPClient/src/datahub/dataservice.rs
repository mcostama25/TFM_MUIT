use anyhow::anyhow;
use async_trait::async_trait;

use crate::{
    error::AppError,
    models::{
        dataservice::{DataService, DataServiceFilters},
        dataset::{Dataset, DatasetFilters},
    },
    repository::dataservice::DataServiceRepository,
};

use super::{
    client::DatahubClient,
    mapping::{
        dataservice_to_dh_payload, dataset_to_dh_payload, dh_entity_to_dataservice,
        dh_entity_to_dataset,
    },
};

#[async_trait]
impl DataServiceRepository for DatahubClient {
    async fn list(&self, _filters: DataServiceFilters) -> Result<Vec<DataService>, AppError> {
        let entities = self.list_entities("dataFlow", &[]).await?;
        Ok(entities.iter().map(dh_entity_to_dataservice).collect())
    }

    async fn get(&self, service_id: &str) -> Result<DataService, AppError> {
        let urn = if service_id.starts_with("urn:li:") {
            service_id.to_string()
        } else {
            format!("urn:li:dataFlow:(dcat,{service_id},PROD)")
        };
        let entity = self.get_entity("dataFlow", &urn).await.map_err(|e| {
            let msg = e.to_string();
            if msg.contains("NOT_FOUND") {
                AppError::NotFound(format!("DataService not found: {service_id}"))
            } else {
                AppError::Internal(anyhow!(msg))
            }
        })?;
        Ok(dh_entity_to_dataservice(&entity))
    }

    async fn create(&self, service: DataService) -> Result<DataService, AppError> {
        let payload = dataservice_to_dh_payload(&service);
        let entity = self.upsert_entity("dataFlow", payload).await?;
        Ok(dh_entity_to_dataservice(&entity))
    }

    async fn update(&self, service_id: &str, service: DataService) -> Result<(), AppError> {
        let mut updated = service;
        updated.dcterms_identifier = Some(service_id.to_string());
        let payload = dataservice_to_dh_payload(&updated);
        self.upsert_entity("dataFlow", payload).await?;
        Ok(())
    }

    async fn delete(&self, service_id: &str) -> Result<(), AppError> {
        let urn = if service_id.starts_with("urn:li:") {
            service_id.to_string()
        } else {
            format!("urn:li:dataFlow:(dcat,{service_id},PROD)")
        };
        self.delete_entity("dataFlow", &urn).await?;
        Ok(())
    }

    async fn list_datasets(
        &self,
        service_id: &str,
        _filters: DatasetFilters,
    ) -> Result<Vec<Dataset>, AppError> {
        let urn = if service_id.starts_with("urn:li:") {
            service_id.to_string()
        } else {
            format!("urn:li:dataFlow:(dcat,{service_id},PROD)")
        };
        let rels = self
            .get_relationships(&urn, &["Produces"], "OUTGOING")
            .await?;
        let datasets = rels
            .relationships
            .iter()
            .filter_map(|r| r.entity.as_ref())
            .filter(|e| e.urn.as_deref().unwrap_or("").contains(":dataset:"))
            .map(dh_entity_to_dataset)
            .collect();
        Ok(datasets)
    }

    async fn get_dataset(
        &self,
        _service_id: &str,
        dataset_id: &str,
    ) -> Result<Dataset, AppError> {
        let urn = if dataset_id.starts_with("urn:li:") {
            dataset_id.to_string()
        } else {
            format!("urn:li:dataset:(urn:li:dataPlatform:dcat,{dataset_id},PROD)")
        };
        let entity = self.get_entity("dataset", &urn).await.map_err(|e| {
            let msg = e.to_string();
            if msg.contains("NOT_FOUND") {
                AppError::NotFound(format!("Dataset not found: {dataset_id}"))
            } else {
                AppError::Internal(anyhow!(msg))
            }
        })?;
        Ok(dh_entity_to_dataset(&entity))
    }

    async fn create_dataset(
        &self,
        _service_id: &str,
        dataset: Dataset,
    ) -> Result<Dataset, AppError> {
        let payload = dataset_to_dh_payload(&dataset, "dcat", "PROD");
        let entity = self.upsert_entity("dataset", payload).await?;
        Ok(dh_entity_to_dataset(&entity))
    }

    async fn update_dataset(
        &self,
        _service_id: &str,
        dataset_id: &str,
        dataset: Dataset,
    ) -> Result<(), AppError> {
        let mut updated = dataset;
        updated.dcterms_identifier = Some(dataset_id.to_string());
        let payload = dataset_to_dh_payload(&updated, "dcat", "PROD");
        self.upsert_entity("dataset", payload).await?;
        Ok(())
    }

    async fn delete_dataset(
        &self,
        _service_id: &str,
        dataset_id: &str,
    ) -> Result<(), AppError> {
        let urn = if dataset_id.starts_with("urn:li:") {
            dataset_id.to_string()
        } else {
            format!("urn:li:dataset:(urn:li:dataPlatform:dcat,{dataset_id},PROD)")
        };
        self.delete_entity("dataset", &urn).await?;
        Ok(())
    }
}
