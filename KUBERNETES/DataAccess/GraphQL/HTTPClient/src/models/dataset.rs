use serde::{Deserialize, Serialize};

/// DCAT3 Dataset — maps to a DataHub `dataset` entity.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Dataset {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_identifier: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_dataset_series: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_spatial_resolution_in_meters: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_spatial: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_temporal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_temporal_resolution: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prov_was_generated_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_theme: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_access_rights: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_conforms_to: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_contact_point: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_publisher: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_landing_page: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_license: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_rights: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_issued: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_modified: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_has_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_prev_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_next_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_first: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_last: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adms_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adms_version_notes: Option<String>,
}

/// Query filters for listing datasets.
#[derive(Debug, Default, Deserialize)]
pub struct DatasetFilters {
    pub theme: Option<String>,
    pub keyword: Option<String>,
    pub spatial_coverage: Option<String>,
    pub temporal_start: Option<String>,
    pub temporal_end: Option<String>,
    pub language: Option<String>,
    pub access_rights: Option<String>,
    pub license: Option<String>,
    pub publisher: Option<String>,
    pub conforms_to: Option<String>,
    pub status: Option<String>,
    pub title: Option<String>,
    pub replaces: Option<String>,
}

/// DCAT3 DatasetSeries — a container of related datasets.
/// Maps to a DataHub `container` entity with a specific sub-type.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DatasetSeries {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_identifier: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcat_theme: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_publisher: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_issued: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dcterms_modified: Option<String>,
}
