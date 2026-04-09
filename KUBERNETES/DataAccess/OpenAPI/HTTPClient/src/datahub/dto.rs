//! DataHub REST API v2 response structures.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Generic entity response from DataHub OpenAPI v2.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct DhEntity {
    pub urn: Option<String>,
    #[serde(flatten)]
    pub aspects: Value,
}

/// Paginated list response wrapping entity results.
#[derive(Debug, Deserialize, Serialize, Default)]
pub struct DhEntityList {
    #[serde(rename = "scrollId")]
    pub scroll_id: Option<String>,
    pub entities: Vec<DhEntity>,
}

/// A single aspect payload as returned by the entity aspect endpoint.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct DhAspect {
    pub value: Option<Value>,
}

/// Relationship item returned by `/openapi/relationships/v1/`.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct DhRelationship {
    pub entity: Option<DhEntity>,
    #[serde(rename = "type")]
    pub rel_type: Option<String>,
}

/// Response from `/openapi/relationships/v1/`.
#[derive(Debug, Deserialize, Serialize, Default)]
pub struct DhRelationshipList {
    pub relationships: Vec<DhRelationship>,
    pub count: Option<u64>,
    pub total: Option<u64>,
}

/// Helper: extract a string field from an aspect value nested under
/// `aspects.<aspectName>.value.<field>`.
pub fn extract_str(entity: &DhEntity, aspect: &str, field: &str) -> Option<String> {
    entity
        .aspects
        .get("aspects")
        .and_then(|a| a.get(aspect))
        .and_then(|a| a.get("value"))
        .and_then(|v| v.get(field))
        .and_then(|f| f.as_str())
        .map(String::from)
}

/// Helper: extract a string from a top-level entity field.
pub fn extract_top_str(entity: &DhEntity, field: &str) -> Option<String> {
    entity
        .aspects
        .get(field)
        .and_then(|f| f.as_str())
        .map(String::from)
}

/// Build a DataHub URN for a dataset.
/// Format: `urn:li:dataset:(urn:li:dataPlatform:<platform>,<name>,<env>)`
pub fn dataset_urn(name: &str, platform: &str, env: &str) -> String {
    format!("urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})")
}

/// Build a DataHub URN for a container.
pub fn container_urn(id: &str) -> String {
    format!("urn:li:container:{id}")
}

/// Build a DataHub URN for a tag.
pub fn tag_urn(name: &str) -> String {
    format!("urn:li:tag:{name}")
}

/// Build a DataHub URN for a domain.
pub fn domain_urn(id: &str) -> String {
    format!("urn:li:domain:{id}")
}

/// Build a DataHub URN for a dataFlow.
pub fn dataflow_urn(orchestrator: &str, flow_id: &str, cluster: &str) -> String {
    format!("urn:li:dataFlow:({orchestrator},{flow_id},{cluster})")
}

/// Extract a plain ID from a DataHub URN by taking the last segment.
pub fn id_from_urn(urn: &str) -> String {
    urn.split(',')
        .last()
        .unwrap_or(urn)
        .trim_end_matches(')')
        .to_string()
}
