//! Conversion functions between DCAT3 models and DataHub entity/aspect payloads.

use serde_json::{json, Value};

use crate::models::{
    catalog::{Catalog, CatalogRecord},
    common::{Keyword, QualifiedRelation, Reference, Relation, Resource, Theme},
    dataservice::DataService,
    dataset::{Dataset, DatasetSeries},
    distribution::Distribution,
};

use super::dto::{extract_str, id_from_urn, DhEntity, DhRelationship};

// ---------------------------------------------------------------------------
// Catalog <-> container
// ---------------------------------------------------------------------------

pub fn dh_entity_to_catalog(entity: &DhEntity) -> Catalog {
    let urn = entity.urn.clone().unwrap_or_default();
    Catalog {
        dcterms_identifier: Some(id_from_urn(&urn)),
        dcterms_title: extract_str(entity, "containerProperties", "name"),
        dcterms_description: extract_str(entity, "containerProperties", "description"),
        dcterms_modified: extract_str(entity, "containerProperties", "lastModified"),
        adms_status: extract_str(entity, "status", "removed")
            .map(|r| if r == "false" { "active".into() } else { "removed".into() }),
        ..Default::default()
    }
}

pub fn catalog_to_dh_payload(catalog: &Catalog) -> Value {
    let name = catalog
        .dcterms_title
        .clone()
        .unwrap_or_else(|| catalog.dcterms_identifier.clone().unwrap_or_default());
    json!({
        "urn": catalog.dcterms_identifier.as_ref().map(|id| format!("urn:li:container:{id}")),
        "aspects": {
            "containerProperties": {
                "value": {
                    "name": name,
                    "description": catalog.dcterms_description,
                    "customProperties": {
                        "dcat_resource": catalog.dcat_resource,
                        "dcterms_creator": catalog.dcterms_creator,
                        "foaf_homepage": catalog.foaf_homepage,
                        "dcat_version": catalog.dcat_version,
                        "dcterms_issued": catalog.dcterms_issued,
                    }
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// CatalogRecord <-> dataset (tagged)
// ---------------------------------------------------------------------------

pub fn dh_entity_to_catalog_record(entity: &DhEntity) -> CatalogRecord {
    let urn = entity.urn.clone().unwrap_or_default();
    CatalogRecord {
        id: Some(id_from_urn(&urn)),
        dct_title: extract_str(entity, "datasetProperties", "name"),
        dct_description: extract_str(entity, "datasetProperties", "description"),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Dataset <-> dataset
// ---------------------------------------------------------------------------

pub fn dh_entity_to_dataset(entity: &DhEntity) -> Dataset {
    let urn = entity.urn.clone().unwrap_or_default();
    Dataset {
        dcterms_identifier: Some(id_from_urn(&urn)),
        dcterms_title: extract_str(entity, "datasetProperties", "name"),
        dcterms_description: extract_str(entity, "datasetProperties", "description"),
        dcat_landing_page: extract_str(entity, "datasetProperties", "externalUrl"),
        dcterms_modified: extract_str(entity, "datasetProperties", "lastModified"),
        dcterms_publisher: extract_str(entity, "ownership", "owners")
            .or_else(|| extract_str(entity, "datasetProperties", "customProperties")),
        ..Default::default()
    }
}

pub fn dataset_to_dh_payload(dataset: &Dataset, platform: &str, env: &str) -> Value {
    let name = dataset
        .dcterms_title
        .clone()
        .unwrap_or_else(|| dataset.dcterms_identifier.clone().unwrap_or_default());
    let urn = format!(
        "urn:li:dataset:(urn:li:dataPlatform:{},{},{})",
        platform,
        dataset.dcterms_identifier.as_deref().unwrap_or(&name),
        env
    );
    json!({
        "urn": urn,
        "aspects": {
            "datasetProperties": {
                "value": {
                    "name": name,
                    "description": dataset.dcterms_description,
                    "externalUrl": dataset.dcat_landing_page,
                    "customProperties": {
                        "dcterms_language": dataset.dcterms_language,
                        "dcterms_license": dataset.dcterms_license,
                        "dcterms_access_rights": dataset.dcterms_access_rights,
                        "dcterms_temporal": dataset.dcterms_temporal,
                        "dcat_theme": dataset.dcat_theme,
                        "adms_status": dataset.adms_status,
                    }
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Distribution — stored as a custom dataset with a "distribution" sub-type
// ---------------------------------------------------------------------------

pub fn dh_entity_to_distribution(entity: &DhEntity) -> Distribution {
    let urn = entity.urn.clone().unwrap_or_default();
    Distribution {
        dcterms_identifier: Some(id_from_urn(&urn)),
        dcterms_description: extract_str(entity, "datasetProperties", "description"),
        dcat_access_url: extract_str(entity, "datasetProperties", "externalUrl"),
        dcterms_format: extract_str(entity, "datasetProperties", "name"),
        ..Default::default()
    }
}

pub fn distribution_to_dh_payload(dist: &Distribution) -> Value {
    let id = dist.dcterms_identifier.as_deref().unwrap_or("unknown");
    let urn = format!("urn:li:dataset:(urn:li:dataPlatform:dcat,dist-{id},PROD)");
    json!({
        "urn": urn,
        "aspects": {
            "datasetProperties": {
                "value": {
                    "name": dist.dcterms_format.clone().unwrap_or_else(|| id.to_string()),
                    "description": dist.dcterms_description,
                    "externalUrl": dist.dcat_access_url,
                    "customProperties": {
                        "dcat_download_url": dist.dcat_download_url,
                        "dcterms_license": dist.dcterms_license,
                        "dcterms_format": dist.dcterms_format,
                        "dcat_media_type": dist.dcat_media_type,
                        "dcat_package_format": dist.dcat_package_format,
                        "dcat_compress_format": dist.dcat_compress_format,
                        "spdx_checksum": dist.spdx_checksum,
                        "dcat_distribution": "true",
                    }
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// DataService <-> dataFlow
// ---------------------------------------------------------------------------

pub fn dh_entity_to_dataservice(entity: &DhEntity) -> DataService {
    let urn = entity.urn.clone().unwrap_or_default();
    DataService {
        dcterms_identifier: Some(id_from_urn(&urn)),
        dcterms_title: extract_str(entity, "dataFlowProperties", "name"),
        dcterms_description: extract_str(entity, "dataFlowProperties", "description"),
        dcat_endpoint_url: extract_str(entity, "dataFlowProperties", "externalUrl"),
        ..Default::default()
    }
}

pub fn dataservice_to_dh_payload(svc: &DataService) -> Value {
    let id = svc.dcterms_identifier.as_deref().unwrap_or("unknown");
    let urn = format!("urn:li:dataFlow:(dcat,{id},PROD)");
    json!({
        "urn": urn,
        "aspects": {
            "dataFlowProperties": {
                "value": {
                    "name": svc.dcterms_title.clone().unwrap_or_else(|| id.to_string()),
                    "description": svc.dcterms_description,
                    "externalUrl": svc.dcat_endpoint_url,
                    "customProperties": {
                        "dcterms_publisher": svc.dcterms_publisher,
                        "dcterms_license": svc.dcterms_license,
                        "dcat_theme": svc.dcat_theme,
                        "adms_status": svc.adms_status,
                    }
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Keyword <-> tag
// ---------------------------------------------------------------------------

pub fn dh_entity_to_keyword(entity: &DhEntity) -> Keyword {
    let urn = entity.urn.clone().unwrap_or_default();
    let name = extract_str(entity, "tagProperties", "name")
        .unwrap_or_else(|| id_from_urn(&urn));
    Keyword {
        id: Some(id_from_urn(&urn)),
        name,
        description: extract_str(entity, "tagProperties", "description"),
    }
}

pub fn keyword_to_dh_payload(kw: &Keyword) -> Value {
    let urn = format!("urn:li:tag:{}", kw.name.replace(' ', "_"));
    json!({
        "urn": urn,
        "aspects": {
            "tagProperties": {
                "value": {
                    "name": kw.name,
                    "description": kw.description,
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Theme <-> domain
// ---------------------------------------------------------------------------

pub fn dh_entity_to_theme(entity: &DhEntity) -> Theme {
    let urn = entity.urn.clone().unwrap_or_default();
    let name = extract_str(entity, "domainProperties", "name")
        .unwrap_or_else(|| id_from_urn(&urn));
    Theme {
        id: Some(id_from_urn(&urn)),
        name,
        description: extract_str(entity, "domainProperties", "description"),
    }
}

pub fn theme_to_dh_payload(theme: &Theme) -> Value {
    let id = theme
        .id
        .clone()
        .unwrap_or_else(|| theme.name.replace(' ', "_"));
    let urn = format!("urn:li:domain:{id}");
    json!({
        "urn": urn,
        "aspects": {
            "domainProperties": {
                "value": {
                    "name": theme.name,
                    "description": theme.description,
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Relation <-> DataHub relationship
// ---------------------------------------------------------------------------

pub fn dh_relationship_to_relation(rel: &DhRelationship, subject_urn: &str) -> Relation {
    Relation {
        id: None,
        subject: Some(subject_urn.to_string()),
        relation_type: rel.rel_type.clone(),
        object: rel.entity.as_ref().and_then(|e| e.urn.clone()),
    }
}

pub fn dh_relationship_to_qualified_relation(
    rel: &DhRelationship,
    subject_urn: &str,
) -> QualifiedRelation {
    QualifiedRelation {
        id: None,
        subject: Some(subject_urn.to_string()),
        relation_type: rel.rel_type.clone(),
        object: rel.entity.as_ref().and_then(|e| e.urn.clone()),
        had_role: rel.rel_type.clone(),
    }
}

// ---------------------------------------------------------------------------
// Resource — generic entity
// ---------------------------------------------------------------------------

pub fn dh_entity_to_resource(entity: &DhEntity) -> Resource {
    let urn = entity.urn.clone().unwrap_or_default();
    // Infer type from URN prefix
    let resource_type = if urn.contains(":dataset:") {
        "dataset"
    } else if urn.contains(":container:") {
        "catalog"
    } else if urn.contains(":dataFlow:") {
        "dataservice"
    } else {
        "unknown"
    };
    Resource {
        id: Some(id_from_urn(&urn)),
        resource_type: Some(resource_type.to_string()),
        dcterms_title: extract_str(entity, "datasetProperties", "name")
            .or_else(|| extract_str(entity, "containerProperties", "name"))
            .or_else(|| extract_str(entity, "dataFlowProperties", "name")),
        dcterms_description: extract_str(entity, "datasetProperties", "description")
            .or_else(|| extract_str(entity, "containerProperties", "description")),
    }
}

// ---------------------------------------------------------------------------
// DatasetSeries <-> container (with sub-type "DatasetSeries")
// ---------------------------------------------------------------------------

pub fn dh_entity_to_dataset_series(entity: &DhEntity) -> DatasetSeries {
    let urn = entity.urn.clone().unwrap_or_default();
    DatasetSeries {
        dcterms_identifier: Some(id_from_urn(&urn)),
        dcterms_title: extract_str(entity, "containerProperties", "name"),
        dcterms_description: extract_str(entity, "containerProperties", "description"),
        ..Default::default()
    }
}

pub fn dataset_series_to_dh_payload(series: &DatasetSeries) -> Value {
    let id = series
        .dcterms_identifier
        .as_deref()
        .unwrap_or("unknown");
    let urn = format!("urn:li:container:{id}");
    json!({
        "urn": urn,
        "aspects": {
            "containerProperties": {
                "value": {
                    "name": series.dcterms_title.clone().unwrap_or_else(|| id.to_string()),
                    "description": series.dcterms_description,
                    "customProperties": {
                        "dcat_type": "DatasetSeries",
                        "dcat_theme": series.dcat_theme,
                        "dcterms_publisher": series.dcterms_publisher,
                    }
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Reference — stored as a custom tag with description
// ---------------------------------------------------------------------------

pub fn dh_entity_to_reference(entity: &DhEntity) -> Reference {
    let urn = entity.urn.clone().unwrap_or_default();
    Reference {
        id: Some(id_from_urn(&urn)),
        label: extract_str(entity, "tagProperties", "name"),
        description: extract_str(entity, "tagProperties", "description"),
        ..Default::default()
    }
}

pub fn reference_to_dh_payload(reference: &Reference) -> Value {
    let id = reference
        .id
        .clone()
        .unwrap_or_else(|| reference.label.clone().unwrap_or_default());
    let urn = format!("urn:li:tag:ref-{id}");
    json!({
        "urn": urn,
        "aspects": {
            "tagProperties": {
                "value": {
                    "name": reference.label.clone().unwrap_or_else(|| id.clone()),
                    "description": reference.uri.clone()
                        .map(|uri| format!("{} | {}", reference.description.as_deref().unwrap_or(""), uri))
                        .or_else(|| reference.description.clone()),
                }
            }
        }
    })
}
