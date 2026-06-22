use apache_avro::types::Value;
use tracing::{debug, info, warn};

use crate::datahub::events::{avro_value_to_mcl, avro_value_to_mcp, ChangeType};
use crate::http::HttpAction;
use crate::store::StateStore;

/// Decode and apply a Kafka event to the state store.
/// Returns (state_changed, optional HTTP action to send).
pub fn handle_event(
    topic: &str,
    avro_val: Value,
    store: &mut StateStore,
) -> (bool, Option<HttpAction>) {
    if topic.contains("MetadataChangeProposal") {
        handle_mcp(avro_val, store)
    } else {
        handle_mcl(avro_val, store)
    }
}

fn handle_mcl(avro_val: Value, store: &mut StateStore) -> (bool, Option<HttpAction>) {
    let Some(mcl) = avro_value_to_mcl(&avro_val) else {
        warn!("Could not parse MCL envelope from Avro value");
        return (false, None);
    };
    apply_to_store(
        "MCL",
        &mcl.entity_type,
        &mcl.entity_urn,
        &mcl.aspect_name,
        &mcl.change_type,
        mcl.aspect_value.as_ref(),
        store,
    )
}

fn handle_mcp(avro_val: Value, store: &mut StateStore) -> (bool, Option<HttpAction>) {
    let Some(mcp) = avro_value_to_mcp(&avro_val) else {
        warn!("Could not parse MCP envelope from Avro value");
        return (false, None);
    };
    apply_to_store(
        "MCP",
        &mcp.entity_type,
        &mcp.entity_urn,
        &mcp.aspect_name,
        &mcp.change_type,
        mcp.aspect_value.as_ref(),
        store,
    )
}

fn apply_to_store(
    event_kind: &str,
    entity_type: &str,
    urn: &str,
    aspect_name: &str,
    change_type: &ChangeType,
    aspect_value: Option<&serde_json::Value>,
    store: &mut StateStore,
) -> (bool, Option<HttpAction>) {
    let dcat_entity = match entity_type {
        "container" => "Catalog",
        "dataset" => "Dataset",
        "dataFlow" => "DataService",
        other => {
            debug!(
                event_kind,
                entity_type = other,
                urn,
                aspect_name,
                "Unhandled entity type — ignoring"
            );
            return (false, None);
        }
    };

    let http_action = compute_http_action(entity_type, urn, aspect_name, change_type, aspect_value);
    let changed = store.apply(urn, entity_type, aspect_name, aspect_value, change_type);

    if changed {
        info!(
            event_kind,
            dcat_entity,
            urn,
            aspect_name,
            change_type = %change_type,
            "State updated"
        );
    } else {
        debug!(event_kind, dcat_entity, urn, aspect_name, "Aspect ignored (no state change)");
    }

    (changed, http_action)
}

fn compute_http_action(
    entity_type: &str,
    urn: &str,
    aspect_name: &str,
    change_type: &ChangeType,
    aspect_value: Option<&serde_json::Value>,
) -> Option<HttpAction> {
    if matches!(change_type, ChangeType::Delete) {
        return Some(if entity_type == "container" {
            HttpAction::DeleteCatalog { urn: urn.to_owned() }
        } else {
            HttpAction::DeleteDataset { urn: urn.to_owned() }
        });
    }

    if aspect_name == "status" {
        let removed = aspect_value
            .and_then(|v| v.get("removed"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if removed {
            return Some(if entity_type == "container" {
                HttpAction::DeleteCatalog { urn: urn.to_owned() }
            } else {
                HttpAction::DeleteDataset { urn: urn.to_owned() }
            });
        }
        return None;
    }

    match aspect_name {
        "containerProperties" => {
            let title = aspect_value
                .and_then(|v| v.get("name"))
                .and_then(|v| v.as_str())
                .unwrap_or(urn)
                .to_owned();
            Some(HttpAction::UpsertCatalog { urn: urn.to_owned(), title })
        }
        "datasetProperties" | "dataFlowProperties" => {
            Some(HttpAction::UpsertDataset { urn: urn.to_owned() })
        }
        "ownership" => {
            let publisher = aspect_value
                .and_then(|v| v.get("owners"))
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|o| o.get("owner"))
                .and_then(|v| v.as_str())
                .map(str::to_owned)?;
            Some(HttpAction::PatchPublisher { urn: urn.to_owned(), publisher })
        }
        "schemaMetadata" => {
            let fields: Vec<String> = aspect_value
                .and_then(|v| v.get("fields"))
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|f| {
                            f.get("fieldPath").and_then(|v| v.as_str()).map(str::to_owned)
                        })
                        .collect()
                })
                .unwrap_or_default();
            if fields.is_empty() {
                None
            } else {
                Some(HttpAction::PatchSchema { urn: urn.to_owned(), fields })
            }
        }
        _ => None,
    }
}
