use apache_avro::types::Value;
use serde_json::Value as JsonValue;
use tracing::{debug, info, warn};

use crate::datahub::events::{avro_value_to_mcl, avro_value_to_mcp, ChangeType};

// ---------------------------------------------------------------------------
// Entry point — called per Kafka message after Avro decoding
// ---------------------------------------------------------------------------

/// Route a decoded Avro value to the appropriate DCAT3 entity handler.
/// Topic name is used to distinguish MCP from MCL messages.
pub fn handle_event(topic: &str, avro_val: Value) {
    if topic.contains("MetadataChangeProposal") {
        handle_mcp(avro_val);
    } else {
        handle_mcl(avro_val);
    }
}

// ---------------------------------------------------------------------------
// MCL handler
// ---------------------------------------------------------------------------

fn handle_mcl(avro_val: Value) {
    let Some(mcl) = avro_value_to_mcl(&avro_val) else {
        warn!("Could not parse MCL envelope from Avro value");
        return;
    };

    route_entity(
        "MCL",
        &mcl.entity_type,
        &mcl.entity_urn,
        &mcl.aspect_name,
        &mcl.change_type,
        mcl.aspect_value.as_ref(),
    );
}

// ---------------------------------------------------------------------------
// MCP handler
// ---------------------------------------------------------------------------

fn handle_mcp(avro_val: Value) {
    let Some(mcp) = avro_value_to_mcp(&avro_val) else {
        warn!("Could not parse MCP envelope from Avro value");
        return;
    };

    route_entity(
        "MCP",
        &mcp.entity_type,
        &mcp.entity_urn,
        &mcp.aspect_name,
        &mcp.change_type,
        mcp.aspect_value.as_ref(),
    );
}

// ---------------------------------------------------------------------------
// Shared routing by entity_type → DCAT3 concept
// ---------------------------------------------------------------------------

fn route_entity(
    event_kind: &str,
    entity_type: &str,
    urn: &str,
    aspect_name: &str,
    change_type: &ChangeType,
    aspect_value: Option<&JsonValue>,
) {
    match entity_type {
        "container" => handle_catalog(event_kind, urn, aspect_name, change_type, aspect_value),
        "dataset" => handle_dataset(event_kind, urn, aspect_name, change_type, aspect_value),
        "dataFlow" => handle_dataservice(event_kind, urn, aspect_name, change_type, aspect_value),
        other => {
            debug!(
                event_kind,
                entity_type = other,
                urn,
                aspect_name,
                "Unhandled entity type — ignoring"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Per-entity handlers (DCAT3 concept mapping)
// ---------------------------------------------------------------------------

/// `container` → DCAT3 **Catalog**
fn handle_catalog(
    event_kind: &str,
    urn: &str,
    aspect_name: &str,
    change_type: &ChangeType,
    aspect_value: Option<&JsonValue>,
) {
    match aspect_name {
        "containerProperties" => {
            let title = aspect_value
                .and_then(|v| v.get("name"))
                .and_then(|v| v.as_str())
                .unwrap_or("<unknown>");
            let description = aspect_value
                .and_then(|v| v.get("description"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            info!(
                event_kind,
                dcat_entity = "Catalog",
                urn,
                change_type = %change_type,
                title,
                description,
                "Catalog metadata change"
            );
        }
        "status" => {
            let removed = aspect_value
                .and_then(|v| v.get("removed"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            info!(
                event_kind,
                dcat_entity = "Catalog",
                urn,
                change_type = %change_type,
                removed,
                "Catalog status change"
            );
        }
        "ownership" => {
            info!(
                event_kind,
                dcat_entity = "Catalog",
                urn,
                change_type = %change_type,
                "Catalog ownership change"
            );
        }
        other => {
            debug!(event_kind, dcat_entity = "Catalog", urn, aspect = other, "Ignored aspect");
        }
    }
}

/// `dataset` → DCAT3 **Dataset** or **Distribution** (based on custom property)
fn handle_dataset(
    event_kind: &str,
    urn: &str,
    aspect_name: &str,
    change_type: &ChangeType,
    aspect_value: Option<&JsonValue>,
) {
    match aspect_name {
        "datasetProperties" => {
            // Check if this dataset represents a DCAT3 Distribution
            let is_distribution = aspect_value
                .and_then(|v| v.pointer("/customProperties/dcat_distribution"))
                .and_then(|v| v.as_str())
                == Some("true");

            let dcat_entity = if is_distribution { "Distribution" } else { "Dataset" };

            let title = aspect_value
                .and_then(|v| v.get("name"))
                .and_then(|v| v.as_str())
                .unwrap_or("<unknown>");
            let description = aspect_value
                .and_then(|v| v.get("description"))
                .and_then(|v| v.as_str())
                .unwrap_or("");

            info!(
                event_kind,
                dcat_entity,
                urn,
                change_type = %change_type,
                title,
                description,
                "Dataset/Distribution metadata change"
            );
        }
        "schemaMetadata" => {
            let field_count = aspect_value
                .and_then(|v| v.get("fields"))
                .and_then(|v| v.as_array())
                .map(|a| a.len())
                .unwrap_or(0);
            info!(
                event_kind,
                dcat_entity = "Dataset",
                urn,
                change_type = %change_type,
                field_count,
                "Dataset schema change"
            );
        }
        "ownership" => {
            info!(
                event_kind,
                dcat_entity = "Dataset",
                urn,
                change_type = %change_type,
                "Dataset ownership change"
            );
        }
        "status" => {
            let removed = aspect_value
                .and_then(|v| v.get("removed"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            info!(
                event_kind,
                dcat_entity = "Dataset",
                urn,
                change_type = %change_type,
                removed,
                "Dataset status change"
            );
        }
        other => {
            debug!(event_kind, dcat_entity = "Dataset", urn, aspect = other, "Ignored aspect");
        }
    }
}

/// `dataFlow` → DCAT3 **DataService**
fn handle_dataservice(
    event_kind: &str,
    urn: &str,
    aspect_name: &str,
    change_type: &ChangeType,
    aspect_value: Option<&JsonValue>,
) {
    match aspect_name {
        "dataFlowProperties" => {
            let name = aspect_value
                .and_then(|v| v.get("name"))
                .and_then(|v| v.as_str())
                .unwrap_or("<unknown>");
            let description = aspect_value
                .and_then(|v| v.get("description"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            info!(
                event_kind,
                dcat_entity = "DataService",
                urn,
                change_type = %change_type,
                name,
                description,
                "DataService metadata change"
            );
        }
        "ownership" => {
            info!(
                event_kind,
                dcat_entity = "DataService",
                urn,
                change_type = %change_type,
                "DataService ownership change"
            );
        }
        "status" => {
            let removed = aspect_value
                .and_then(|v| v.get("removed"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            info!(
                event_kind,
                dcat_entity = "DataService",
                urn,
                change_type = %change_type,
                removed,
                "DataService status change"
            );
        }
        other => {
            debug!(
                event_kind,
                dcat_entity = "DataService",
                urn,
                aspect = other,
                "Ignored aspect"
            );
        }
    }
}
