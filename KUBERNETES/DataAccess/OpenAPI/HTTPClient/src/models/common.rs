use serde::{Deserialize, Serialize};

/// DCAT3 Keyword — maps to a DataHub `tag` entity.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Keyword {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// DCAT3 Theme — maps to a DataHub `domain` entity.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Theme {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// DCAT3 Reference — an external reference or standard.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Reference {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// DCAT3 Relation — a directed relationship between two resources.
/// Maps to a DataHub relationship edge.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Relation {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relation_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
}

/// DCAT3 QualifiedRelation — a relation with additional context/role.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QualifiedRelation {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relation_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub had_role: Option<String>,
}

/// DCAT3 Resource — the abstract superclass of Dataset, DataService, etc.
/// Used for generic cross-resource queries.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Resource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_description: Option<String>,
}
