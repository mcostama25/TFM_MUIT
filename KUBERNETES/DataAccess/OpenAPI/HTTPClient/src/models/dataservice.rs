use serde::{Deserialize, Serialize};

/// DCAT3 DataService — an API or endpoint serving datasets.
/// Maps to a DataHub `dataFlow` entity.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DataService {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_identifier: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_serves_dataset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_access_rights: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_conforms_to: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_contact_point: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_issued: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_modified: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_license: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_publisher: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_endpoint_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_endpoint_description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_keyword: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_theme: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adms_status: Option<String>,
}

/// Query filters for listing data services.
#[derive(Debug, Default, Deserialize)]
pub struct DataServiceFilters {
    pub theme: Option<String>,
    pub keyword: Option<String>,
    pub dataset: Option<String>,
    pub language: Option<String>,
    pub access_rights: Option<String>,
    pub license: Option<String>,
    pub publisher: Option<String>,
    pub creator: Option<String>,
    pub conforms_to: Option<String>,
    pub status: Option<String>,
    pub title: Option<String>,
    pub replaces: Option<String>,
}
