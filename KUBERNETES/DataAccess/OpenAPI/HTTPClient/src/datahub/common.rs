use anyhow::anyhow;
use async_trait::async_trait;

use crate::{
    error::AppError,
    models::common::{Keyword, QualifiedRelation, Reference, Relation, Resource, Theme},
    repository::common::{
        KeywordRepository, QualifiedRelationRepository, ReferenceRepository, RelationRepository,
        ResourceRepository, ThemeRepository,
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
        let entities = self.list_entities("tag", &[]).await?;
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
        let entities = self.list_entities("domain", &[]).await?;
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
            .list_entities("tag", &[("query", "urn:*ref-*")])
            .await?;
        Ok(entities.iter().map(dh_entity_to_reference).collect())
    }

    async fn get(&self, reference_id: &str) -> Result<Reference, AppError> {
        let urn = format!("urn:li:tag:ref-{reference_id}");
        let entity = self.get_entity("tag", &urn).await.map_err(|e| {
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
// Resources (generic search across entity types)
// ---------------------------------------------------------------------------

#[async_trait]
impl ResourceRepository for DatahubClient {
    async fn list(&self) -> Result<Vec<Resource>, AppError> {
        let mut resources = Vec::new();
        for entity_type in &["dataset", "container", "dataFlow"] {
            let entities = self.list_entities(entity_type, &[]).await?;
            resources.extend(entities.iter().map(dh_entity_to_resource));
        }
        Ok(resources)
    }

    async fn get(&self, resource_id: &str) -> Result<Resource, AppError> {
        // Try dataset first, then container, then dataFlow
        for (entity_type, urn) in &[
            (
                "dataset",
                format!("urn:li:dataset:(urn:li:dataPlatform:dcat,{resource_id},PROD)"),
            ),
            ("container", format!("urn:li:container:{resource_id}")),
            (
                "dataFlow",
                format!("urn:li:dataFlow:(dcat,{resource_id},PROD)"),
            ),
        ] {
            if let Ok(entity) = self.get_entity(entity_type, urn).await {
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
