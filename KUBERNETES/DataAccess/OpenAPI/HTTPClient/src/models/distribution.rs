use serde::{Deserialize, Serialize};

/// DCAT3 Distribution — a specific representation/download of a dataset.
/// In DataHub, modelled as an aspect on a dataset or a linked entity.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Distribution {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_identifier: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_dataset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_access_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_download_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_access_rights: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_conforms_to: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_license: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_issued: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_modified: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_rights: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_temporal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_media_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_package_format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_compress_format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spdx_checksum: Option<String>,
}

/// Query filters for listing distributions.
#[derive(Debug, Default, Deserialize)]
pub struct DistributionFilters {
    pub access_service: Option<String>,
    pub format: Option<String>,
    pub package_format: Option<String>,
    pub compress_format: Option<String>,
    pub media_type: Option<String>,
    pub conforms_to: Option<String>,
    pub title: Option<String>,
}
