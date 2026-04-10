use anyhow::anyhow;
use async_trait::async_trait;

use crate::{
    error::AppError,
    models::{
        dataservice::DataService,
        dataset::{Dataset, DatasetFilters, DatasetSeries},
        distribution::Distribution,
    },
    repository::dataset::{DatasetRepository, DatasetSeriesRepository},
};

use super::{
    client::DatahubClient,
    mapping::{
        dataset_series_to_dh_payload, dataset_to_dh_payload, dh_entity_to_dataservice,
        dh_entity_to_dataset, dh_entity_to_dataset_series, dh_entity_to_distribution,
    },
};

#[async_trait]
impl DatasetRepository for DatahubClient {
    async fn list(&self, filters: DatasetFilters) -> Result<Vec<Dataset>, AppError> {
        let mut params: Vec<(&str, &str)> = Vec::new();
        // DataHub supports full-text search via "query" param; map common filters
        let query_parts: Vec<String> = [
            filters.title.as_deref().map(|v| format!("name:{v}")),
            filters.theme.as_deref().map(|v| format!("customProperties.dcat_theme:{v}")),
            filters
                .publisher
                .as_deref()
                .map(|v| format!("customProperties.dcterms_publisher:{v}")),
        ]
        .into_iter()
        .flatten()
        .collect();

        let query_str;
        if !query_parts.is_empty() {
            query_str = query_parts.join(" AND ");
            params.push(("query", &query_str));
        }

        let entities = self
            .list_entities("dataset", &["datasetProperties", "ownership"], &params)
            .await?;
        Ok(entities.iter().map(dh_entity_to_dataset).collect())
    }

    async fn get(&self, dataset_id: &str) -> Result<Dataset, AppError> {
        // Accept either a plain name or a full URN
        let urn = if dataset_id.starts_with("urn:li:") {
            dataset_id.to_string()
        } else {
            format!("urn:li:dataset:(urn:li:dataPlatform:dcat,{dataset_id},PROD)")
        };
        let entity = self
            .get_entity("dataset", &urn, &["datasetProperties", "ownership"])
            .await
            .map_err(|e| {
                let msg = e.to_string();
                if msg.contains("NOT_FOUND") {
                    AppError::NotFound(format!("Dataset not found: {dataset_id}"))
                } else {
                    AppError::Internal(anyhow!(msg))
                }
            })?;
        Ok(dh_entity_to_dataset(&entity))
    }

    async fn create(&self, dataset: Dataset) -> Result<Dataset, AppError> {
        let payload = dataset_to_dh_payload(&dataset, "dcat", "PROD");
        let entity = self.upsert_entity("dataset", payload).await?;
        Ok(dh_entity_to_dataset(&entity))
    }

    async fn update(&self, dataset_id: &str, dataset: Dataset) -> Result<(), AppError> {
        let mut updated = dataset;
        updated.dcterms_identifier = Some(dataset_id.to_string());
        let payload = dataset_to_dh_payload(&updated, "dcat", "PROD");
        self.upsert_entity("dataset", payload).await?;
        Ok(())
    }

    async fn delete(&self, dataset_id: &str) -> Result<(), AppError> {
        let urn = if dataset_id.starts_with("urn:li:") {
            dataset_id.to_string()
        } else {
            format!("urn:li:dataset:(urn:li:dataPlatform:dcat,{dataset_id},PROD)")
        };
        self.delete_entity("dataset", &urn).await?;
        Ok(())
    }

    async fn list_distributions(&self, dataset_id: &str) -> Result<Vec<Distribution>, AppError> {
        let urn = if dataset_id.starts_with("urn:li:") {
            dataset_id.to_string()
        } else {
            format!("urn:li:dataset:(urn:li:dataPlatform:dcat,{dataset_id},PROD)")
        };
        // Distributions are upstream datasets linked via lineage
        let rels = self
            .get_relationships(&urn, &["DownstreamOf"], "OUTGOING")
            .await?;
        let dists = rels
            .relationships
            .iter()
            .filter_map(|r| r.entity.as_ref())
            .filter(|e| {
                e.aspects
                    .pointer("/aspects/datasetProperties/value/customProperties/dcat_distribution")
                    .and_then(|v| v.as_str())
                    == Some("true")
            })
            .map(dh_entity_to_distribution)
            .collect();
        Ok(dists)
    }

    async fn list_dataservices(&self, dataset_id: &str) -> Result<Vec<DataService>, AppError> {
        let urn = if dataset_id.starts_with("urn:li:") {
            dataset_id.to_string()
        } else {
            format!("urn:li:dataset:(urn:li:dataPlatform:dcat,{dataset_id},PROD)")
        };
        let rels = self
            .get_relationships(&urn, &["Consumes"], "INCOMING")
            .await?;
        let services = rels
            .relationships
            .iter()
            .filter_map(|r| r.entity.as_ref())
            .filter(|e| e.urn.as_deref().unwrap_or("").contains(":dataFlow:"))
            .map(dh_entity_to_dataservice)
            .collect();
        Ok(services)
    }
}

#[async_trait]
impl DatasetSeriesRepository for DatahubClient {
    async fn list(&self) -> Result<Vec<DatasetSeries>, AppError> {
        let entities = self
            .list_entities(
                "container",
                &["containerProperties"],
                &[("query", "customProperties.dcat_type:DatasetSeries")],
            )
            .await?;
        Ok(entities.iter().map(dh_entity_to_dataset_series).collect())
    }

    async fn get(&self, series_id: &str) -> Result<DatasetSeries, AppError> {
        let urn = format!("urn:li:container:{series_id}");
        let entity = self
            .get_entity("container", &urn, &["containerProperties"])
            .await
            .map_err(|e| {
                let msg = e.to_string();
                if msg.contains("NOT_FOUND") {
                    AppError::NotFound(format!("DatasetSeries not found: {series_id}"))
                } else {
                    AppError::Internal(anyhow!(msg))
                }
            })?;
        Ok(dh_entity_to_dataset_series(&entity))
    }

    async fn create(&self, series: DatasetSeries) -> Result<DatasetSeries, AppError> {
        let payload = dataset_series_to_dh_payload(&series);
        let entity = self.upsert_entity("container", payload).await?;
        Ok(dh_entity_to_dataset_series(&entity))
    }

    async fn update(&self, series_id: &str, series: DatasetSeries) -> Result<(), AppError> {
        let mut updated = series;
        updated.dcterms_identifier = Some(series_id.to_string());
        let payload = dataset_series_to_dh_payload(&updated);
        self.upsert_entity("container", payload).await?;
        Ok(())
    }

    async fn delete(&self, series_id: &str) -> Result<(), AppError> {
        let urn = format!("urn:li:container:{series_id}");
        self.delete_entity("container", &urn).await?;
        Ok(())
    }

    async fn list_datasets(
        &self,
        series_id: &str,
        _filters: DatasetFilters,
    ) -> Result<Vec<Dataset>, AppError> {
        let urn = format!("urn:li:container:{series_id}");
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
