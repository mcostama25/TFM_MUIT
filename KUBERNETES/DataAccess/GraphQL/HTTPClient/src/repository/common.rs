use async_trait::async_trait;

use crate::{
    error::AppError,
    models::common::{Keyword, OwnerResources, QualifiedRelation, Reference, Relation, Resource, Theme},
};

#[async_trait]
pub trait KeywordRepository: Send + Sync {
    async fn list(&self) -> Result<Vec<Keyword>, AppError>;
    async fn create(&self, keyword: Keyword) -> Result<Keyword, AppError>;
    async fn delete(&self, keyword_id: &str) -> Result<(), AppError>;
}

#[async_trait]
pub trait ThemeRepository: Send + Sync {
    async fn list(&self) -> Result<Vec<Theme>, AppError>;
    async fn create(&self, theme: Theme) -> Result<Theme, AppError>;
    async fn delete(&self, theme_id: &str) -> Result<(), AppError>;
}

#[async_trait]
pub trait ReferenceRepository: Send + Sync {
    async fn list(&self) -> Result<Vec<Reference>, AppError>;
    async fn get(&self, reference_id: &str) -> Result<Reference, AppError>;
    async fn create(&self, reference: Reference) -> Result<Reference, AppError>;
    async fn update(&self, reference_id: &str, reference: Reference) -> Result<(), AppError>;
    async fn delete(&self, reference_id: &str) -> Result<(), AppError>;
}

#[async_trait]
pub trait RelationRepository: Send + Sync {
    async fn list(&self) -> Result<Vec<Relation>, AppError>;
    async fn get(&self, relation_id: &str) -> Result<Relation, AppError>;
    async fn create(&self, relation: Relation) -> Result<Relation, AppError>;
    async fn update(&self, relation_id: &str, relation: Relation) -> Result<(), AppError>;
    async fn delete(&self, relation_id: &str) -> Result<(), AppError>;
    async fn list_for_resource(&self, resource_id: &str) -> Result<Vec<Relation>, AppError>;
}

#[async_trait]
pub trait QualifiedRelationRepository: Send + Sync {
    async fn list(&self) -> Result<Vec<QualifiedRelation>, AppError>;
    async fn get(&self, relation_id: &str) -> Result<QualifiedRelation, AppError>;
    async fn create(&self, relation: QualifiedRelation)
        -> Result<QualifiedRelation, AppError>;
    async fn update(
        &self,
        relation_id: &str,
        relation: QualifiedRelation,
    ) -> Result<(), AppError>;
    async fn delete(&self, relation_id: &str) -> Result<(), AppError>;
    async fn list_for_resource(
        &self,
        resource_id: &str,
    ) -> Result<Vec<QualifiedRelation>, AppError>;
}

#[async_trait]
pub trait ResourceRepository: Send + Sync {
    async fn list(&self) -> Result<Vec<Resource>, AppError>;
    async fn get(&self, resource_id: &str) -> Result<Resource, AppError>;
    async fn create(&self, resource: Resource) -> Result<Resource, AppError>;
    async fn update(&self, resource_id: &str, resource: Resource) -> Result<(), AppError>;
    async fn delete(&self, resource_id: &str) -> Result<(), AppError>;
}

/// Repository for querying all resource types by owner username.
#[async_trait]
pub trait OwnerRepository: Send + Sync {
    async fn list_by_owner(&self, username: &str) -> Result<OwnerResources, AppError>;
}
