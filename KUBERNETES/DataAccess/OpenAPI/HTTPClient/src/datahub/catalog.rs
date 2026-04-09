use anyhow::anyhow;
use async_trait::async_trait;

use crate::{
    error::AppError,
    models::{
        catalog::{Catalog, CatalogFilters, CatalogRecord},
        dataset::Dataset,
    },
    repository::catalog::{CatalogRecordRepository, CatalogRepository},
};

use super::{
    client::DatahubClient,
    mapping::{
        catalog_to_dh_payload, dh_entity_to_catalog, dh_entity_to_catalog_record,
        dh_entity_to_dataset,
    },
};

#[async_trait]
impl CatalogRepository for DatahubClient {
    async fn list(&self, _filters: CatalogFilters) -> Result<Vec<Catalog>, AppError> {
        let entities = self.list_entities("container", &[]).await?;
        Ok(entities.iter().map(dh_entity_to_catalog).collect())
    }

    async fn get(&self, catalog_id: &str) -> Result<Catalog, AppError> {
        let urn = format!("urn:li:container:{catalog_id}");
        let entity = self.get_entity("container", &urn).await.map_err(|e| {
            let msg = e.to_string();
            if msg.contains("NOT_FOUND") {
                AppError::NotFound(format!("Catalog not found: {catalog_id}"))
            } else {
                AppError::Internal(anyhow!(msg))
            }
        })?;
        Ok(dh_entity_to_catalog(&entity))
    }

    async fn create(&self, catalog: Catalog) -> Result<Catalog, AppError> {
        let payload = catalog_to_dh_payload(&catalog);
        let entity = self.upsert_entity("container", payload).await?;
        Ok(dh_entity_to_catalog(&entity))
    }

    async fn update(&self, catalog_id: &str, catalog: Catalog) -> Result<(), AppError> {
        let mut updated = catalog;
        updated.dcterms_identifier = Some(catalog_id.to_string());
        let payload = catalog_to_dh_payload(&updated);
        self.upsert_entity("container", payload).await?;
        Ok(())
    }

    async fn delete(&self, catalog_id: &str) -> Result<(), AppError> {
        let urn = format!("urn:li:container:{catalog_id}");
        self.delete_entity("container", &urn).await?;
        Ok(())
    }

    async fn list_datasets(&self, catalog_id: &str) -> Result<Vec<Dataset>, AppError> {
        // Query datasets that belong to this container via relationship
        let urn = format!("urn:li:container:{catalog_id}");
        let rels = self
            .get_relationships(&urn, &["IsPartOf"], "INCOMING")
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
}

#[async_trait]
impl CatalogRecordRepository for DatahubClient {
    async fn list(&self, _catalog_id: Option<&str>) -> Result<Vec<CatalogRecord>, AppError> {
        // CatalogRecords are datasets tagged as "catalog-record" in DataHub
        let entities = self
            .list_entities("dataset", &[("query", "customProperties.dcat_catalog_record:true")])
            .await?;
        Ok(entities.iter().map(dh_entity_to_catalog_record).collect())
    }

    async fn list_for_catalog(&self, catalog_id: &str) -> Result<Vec<CatalogRecord>, AppError> {
        let urn = format!("urn:li:container:{catalog_id}");
        let rels = self
            .get_relationships(&urn, &["IsPartOf"], "INCOMING")
            .await?;
        let records = rels
            .relationships
            .iter()
            .filter_map(|r| r.entity.as_ref())
            .map(dh_entity_to_catalog_record)
            .collect();
        Ok(records)
    }

    async fn get(&self, record_id: &str) -> Result<CatalogRecord, AppError> {
        let urn = format!(
            "urn:li:dataset:(urn:li:dataPlatform:dcat,record-{record_id},PROD)"
        );
        let entity = self.get_entity("dataset", &urn).await.map_err(|e| {
            let msg = e.to_string();
            if msg.contains("NOT_FOUND") {
                AppError::NotFound(format!("CatalogRecord not found: {record_id}"))
            } else {
                AppError::Internal(anyhow!(msg))
            }
        })?;
        Ok(dh_entity_to_catalog_record(&entity))
    }

    async fn create(&self, record: CatalogRecord) -> Result<CatalogRecord, AppError> {
        use serde_json::json;
        let id = record.id.as_deref().unwrap_or("new");
        let urn = format!("urn:li:dataset:(urn:li:dataPlatform:dcat,record-{id},PROD)");
        let payload = json!({
            "urn": urn,
            "aspects": {
                "datasetProperties": {
                    "value": {
                        "name": record.dct_title,
                        "description": record.dct_description,
                        "customProperties": {
                            "dcat_catalog": record.dcat_catalog,
                            "foaf_primary_topic": record.foaf_primary_topic,
                            "dcat_catalog_record": "true",
                        }
                    }
                }
            }
        });
        let entity = self.upsert_entity("dataset", payload).await?;
        Ok(dh_entity_to_catalog_record(&entity))
    }

    async fn update(&self, record_id: &str, record: CatalogRecord) -> Result<(), AppError> {
        use serde_json::json;
        let urn = format!("urn:li:dataset:(urn:li:dataPlatform:dcat,record-{record_id},PROD)");
        let payload = json!({
            "urn": urn,
            "aspects": {
                "datasetProperties": {
                    "value": {
                        "name": record.dct_title,
                        "description": record.dct_description,
                    }
                }
            }
        });
        self.upsert_entity("dataset", payload).await?;
        Ok(())
    }

    async fn delete(&self, record_id: &str) -> Result<(), AppError> {
        let urn = format!(
            "urn:li:dataset:(urn:li:dataPlatform:dcat,record-{record_id},PROD)"
        );
        self.delete_entity("dataset", &urn).await?;
        Ok(())
    }
}
