use serde::{Deserialize, Serialize};

/// DCAT3 Catalog — maps to a DataHub `container` entity.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Catalog {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_identifier: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub foaf_homepage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_resource: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_creator: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_modified: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_issued: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_serves_dataset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_has_data_service: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_prev: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_next: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adms_status: Option<String>,
}

/// Query filters for listing catalogs.
#[derive(Debug, Default, Deserialize)]
pub struct CatalogFilters {
    pub theme: Option<String>,
    pub keyword: Option<String>,
    pub publisher: Option<String>,
    pub access_rights: Option<String>,
    pub license: Option<String>,
    pub conforms_to: Option<String>,
    pub status: Option<String>,
}

/// DCAT3 CatalogRecord — metadata about a dataset entry in a catalog.
/// Maps to a DataHub dataset with a specific tag/aspect marking it as a catalog record.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CatalogRecord {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dct_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dct_description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dct_issued: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub foaf_primary_topic: Option<String>,
}
