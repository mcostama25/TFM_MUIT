use anyhow::anyhow;
use async_trait::async_trait;

use crate::{
    error::AppError,
    models::common::{Keyword, OwnerResources, QualifiedRelation, Reference, Relation, Resource, Theme},
    repository::common::{
        KeywordRepository, OwnerRepository, QualifiedRelationRepository, ReferenceRepository,
        RelationRepository, ResourceRepository, ThemeRepository,
    },
};

use super::{
    client::DatahubClient,
    mapping::{
        dh_entity_to_keyword, dh_entity_to_reference, dh_entity_to_resource, dh_entity_to_theme,
        dh_relationship_to_qualified_relation, dh_relationship_to_relation, keyword_to_dh_payload,
        reference_to_dh_payload, theme_to_dh_payload,
    },
};

// ---------------------------------------------------------------------------
// Keywords (tags)
// ---------------------------------------------------------------------------

#[async_trait]
impl KeywordRepository for DatahubClient {
    async fn list(&self) -> Result<Vec<Keyword>, AppError> {
        let entities = self.list_entities("tag", &["tagProperties"], &[]).await?;
        Ok(entities.iter().map(dh_entity_to_keyword).collect())
    }

    async fn create(&self, keyword: Keyword) -> Result<Keyword, AppError> {
        let payload = keyword_to_dh_payload(&keyword);
        let entity = self.upsert_entity("tag", payload).await?;
        Ok(dh_entity_to_keyword(&entity))
    }

    async fn delete(&self, keyword_id: &str) -> Result<(), AppError> {
        let urn = format!("urn:li:tag:{keyword_id}");
        self.delete_entity("tag", &urn).await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Themes (domains)
// ---------------------------------------------------------------------------

#[async_trait]
impl ThemeRepository for DatahubClient {
    async fn list(&self) -> Result<Vec<Theme>, AppError> {
        let entities = self
            .list_entities("domain", &["domainProperties"], &[])
            .await?;
        Ok(entities.iter().map(dh_entity_to_theme).collect())
    }

    async fn create(&self, theme: Theme) -> Result<Theme, AppError> {
        let payload = theme_to_dh_payload(&theme);
        let entity = self.upsert_entity("domain", payload).await?;
        Ok(dh_entity_to_theme(&entity))
    }

    async fn delete(&self, theme_id: &str) -> Result<(), AppError> {
        let urn = format!("urn:li:domain:{theme_id}");
        self.delete_entity("domain", &urn).await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// References (tags prefixed with "ref-")
// ---------------------------------------------------------------------------

#[async_trait]
impl ReferenceRepository for DatahubClient {
    async fn list(&self) -> Result<Vec<Reference>, AppError> {
        let entities = self
            .list_entities("tag", &["tagProperties"], &[("query", "urn:*ref-*")])
            .await?;
        Ok(entities.iter().map(dh_entity_to_reference).collect())
    }

    async fn get(&self, reference_id: &str) -> Result<Reference, AppError> {
        let urn = format!("urn:li:tag:ref-{reference_id}");
        let entity = self
            .get_entity("tag", &urn, &["tagProperties"])
            .await
            .map_err(|e| {
                let msg = e.to_string();
                if msg.contains("NOT_FOUND") {
                    AppError::NotFound(format!("Reference not found: {reference_id}"))
                } else {
                    AppError::Internal(anyhow!(msg))
                }
            })?;
        Ok(dh_entity_to_reference(&entity))
    }

    async fn create(&self, reference: Reference) -> Result<Reference, AppError> {
        let payload = reference_to_dh_payload(&reference);
        let entity = self.upsert_entity("tag", payload).await?;
        Ok(dh_entity_to_reference(&entity))
    }

    async fn update(&self, reference_id: &str, reference: Reference) -> Result<(), AppError> {
        let mut updated = reference;
        updated.id = Some(reference_id.to_string());
        let payload = reference_to_dh_payload(&updated);
        self.upsert_entity("tag", payload).await?;
        Ok(())
    }

    async fn delete(&self, reference_id: &str) -> Result<(), AppError> {
        let urn = format!("urn:li:tag:ref-{reference_id}");
        self.delete_entity("tag", &urn).await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Relations (DataHub relationships, OUTGOING)
// ---------------------------------------------------------------------------

#[async_trait]
impl RelationRepository for DatahubClient {
    async fn list(&self) -> Result<Vec<Relation>, AppError> {
        // No global relationship listing in DataHub; return empty
        Ok(vec![])
    }

    async fn get(&self, _relation_id: &str) -> Result<Relation, AppError> {
        Err(AppError::NotFound(
            "Individual relation lookup not supported by DataHub REST API".into(),
        ))
    }

    async fn create(&self, _relation: Relation) -> Result<Relation, AppError> {
        Err(AppError::BadRequest(
            "Relationships in DataHub are created implicitly via entity lineage".into(),
        ))
    }

    async fn update(&self, _relation_id: &str, _relation: Relation) -> Result<(), AppError> {
        Err(AppError::BadRequest(
            "Relationships in DataHub cannot be directly updated".into(),
        ))
    }

    async fn delete(&self, _relation_id: &str) -> Result<(), AppError> {
        Err(AppError::BadRequest(
            "Relationships in DataHub cannot be directly deleted".into(),
        ))
    }

    async fn list_for_resource(&self, resource_id: &str) -> Result<Vec<Relation>, AppError> {
        let urn = if resource_id.starts_with("urn:li:") {
            resource_id.to_string()
        } else {
            format!("urn:li:dataset:(urn:li:dataPlatform:dcat,{resource_id},PROD)")
        };
        let rels = self
            .get_relationships(&urn, &[], "OUTGOING")
            .await?;
        Ok(rels
            .relationships
            .iter()
            .map(|r| dh_relationship_to_relation(r, &urn))
            .collect())
    }
}

// ---------------------------------------------------------------------------
// Qualified Relations
// ---------------------------------------------------------------------------

#[async_trait]
impl QualifiedRelationRepository for DatahubClient {
    async fn list(&self) -> Result<Vec<QualifiedRelation>, AppError> {
        Ok(vec![])
    }

    async fn get(&self, _relation_id: &str) -> Result<QualifiedRelation, AppError> {
        Err(AppError::NotFound(
            "Individual qualified relation lookup not supported".into(),
        ))
    }

    async fn create(
        &self,
        _relation: QualifiedRelation,
    ) -> Result<QualifiedRelation, AppError> {
        Err(AppError::BadRequest(
            "Qualified relationships are inferred from DataHub lineage".into(),
        ))
    }

    async fn update(
        &self,
        _relation_id: &str,
        _relation: QualifiedRelation,
    ) -> Result<(), AppError> {
        Err(AppError::BadRequest(
            "Qualified relationships cannot be directly updated".into(),
        ))
    }

    async fn delete(&self, _relation_id: &str) -> Result<(), AppError> {
        Err(AppError::BadRequest(
            "Qualified relationships cannot be directly deleted".into(),
        ))
    }

    async fn list_for_resource(
        &self,
        resource_id: &str,
    ) -> Result<Vec<QualifiedRelation>, AppError> {
        let urn = if resource_id.starts_with("urn:li:") {
            resource_id.to_string()
        } else {
            format!("urn:li:dataset:(urn:li:dataPlatform:dcat,{resource_id},PROD)")
        };
        let rels = self
            .get_relationships(&urn, &[], "OUTGOING")
            .await?;
        Ok(rels
            .relationships
            .iter()
            .map(|r| dh_relationship_to_qualified_relation(r, &urn))
            .collect())
    }
}

// ---------------------------------------------------------------------------
// Owner — cross-entity search filtered by DataHub corpuser ownership
// ---------------------------------------------------------------------------

#[async_trait]
impl OwnerRepository for DatahubClient {
    async fn list_by_owner(&self, username: &str) -> Result<OwnerResources, AppError> {
        // DataHub ownership URN format: urn:li:corpuser:{username}
        // The `owners` field in DataHub's search index holds the full owner URN.
        let owner_urn = format!("urn:li:corpuser:{username}");
        let query = format!("owners:{owner_urn}");
        let params: &[(&str, &str)] = &[("query", query.as_str())];

        let mut catalogs = Vec::new();
        let mut datasets = Vec::new();
        let mut dataset_series = Vec::new();
        let mut dataservices = Vec::new();
        let mut distributions = Vec::new();

        // --- dataset entities: regular datasets + distributions (tagged by custom property) ---
        let ds_entities = self
            .list_entities("dataset", &["datasetProperties", "ownership"], params)
            .await?;
        for entity in &ds_entities {
            let is_dist = entity
                .aspects
                .pointer("/aspects/datasetProperties/value/customProperties/dcat_distribution")
                .and_then(|v| v.as_str())
                == Some("true");
            let is_record = entity
                .aspects
                .pointer("/aspects/datasetProperties/value/customProperties/dcat_catalog_record")
                .and_then(|v| v.as_str())
                == Some("true");

            if is_dist {
                distributions.push(dh_entity_to_resource(entity));
            } else if !is_record {
                // Skip catalog records — they are internal metadata entries, not user-facing resources
                datasets.push(dh_entity_to_resource(entity));
            }
        }

        // --- container entities: catalogs + dataset series (tagged by custom property) ---
        let container_entities = self
            .list_entities("container", &["containerProperties"], params)
            .await?;
        for entity in &container_entities {
            let is_series = entity
                .aspects
                .pointer("/aspects/containerProperties/value/customProperties/dcat_type")
                .and_then(|v| v.as_str())
                == Some("DatasetSeries");

            if is_series {
                dataset_series.push(dh_entity_to_resource(entity));
            } else {
                catalogs.push(dh_entity_to_resource(entity));
            }
        }

        // --- dataFlow entities: data services ---
        let svc_entities = self
            .list_entities("dataFlow", &["dataFlowProperties"], params)
            .await?;
        dataservices.extend(svc_entities.iter().map(dh_entity_to_resource));

        let total = catalogs.len()
            + datasets.len()
            + dataset_series.len()
            + dataservices.len()
            + distributions.len();

        Ok(OwnerResources {
            owner: username.to_string(),
            catalogs,
            datasets,
            dataset_series,
            dataservices,
            distributions,
            total,
        })
    }
}

// ---------------------------------------------------------------------------
// Resources (generic search across entity types)
// ---------------------------------------------------------------------------

#[async_trait]
impl ResourceRepository for DatahubClient {
    async fn list(&self) -> Result<Vec<Resource>, AppError> {
        let mut resources = Vec::new();

        let ds = self
            .list_entities("dataset", &["datasetProperties"], &[])
            .await?;
        resources.extend(ds.iter().map(dh_entity_to_resource));

        let containers = self
            .list_entities("container", &["containerProperties"], &[])
            .await?;
        resources.extend(containers.iter().map(dh_entity_to_resource));

        let flows = self
            .list_entities("dataFlow", &["dataFlowProperties"], &[])
            .await?;
        resources.extend(flows.iter().map(dh_entity_to_resource));

        Ok(resources)
    }

    async fn get(&self, resource_id: &str) -> Result<Resource, AppError> {
        // Try dataset first, then container, then dataFlow
        let candidates: &[(&str, &[&str], String)] = &[
            (
                "dataset",
                &["datasetProperties"],
                format!("urn:li:dataset:(urn:li:dataPlatform:dcat,{resource_id},PROD)"),
            ),
            (
                "container",
                &["containerProperties"],
                format!("urn:li:container:{resource_id}"),
            ),
            (
                "dataFlow",
                &["dataFlowProperties"],
                format!("urn:li:dataFlow:(dcat,{resource_id},PROD)"),
            ),
        ];
        for (entity_type, aspects, urn) in candidates {
            if let Ok(entity) = self.get_entity(entity_type, urn, aspects).await {
                return Ok(dh_entity_to_resource(&entity));
            }
        }
        Err(AppError::NotFound(format!(
            "Resource not found: {resource_id}"
        )))
    }

    async fn create(&self, _resource: Resource) -> Result<Resource, AppError> {
        Err(AppError::BadRequest(
            "Create a specific resource type (dataset, catalog, or dataservice) instead".into(),
        ))
    }

    async fn update(&self, _resource_id: &str, _resource: Resource) -> Result<(), AppError> {
        Err(AppError::BadRequest(
            "Update a specific resource type (dataset, catalog, or dataservice) instead".into(),
        ))
    }

    async fn delete(&self, _resource_id: &str) -> Result<(), AppError> {
        Err(AppError::BadRequest(
            "Delete a specific resource type (dataset, catalog, or dataservice) instead".into(),
        ))
    }
}
