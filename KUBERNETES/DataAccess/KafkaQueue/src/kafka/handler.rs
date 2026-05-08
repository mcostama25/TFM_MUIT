use apache_avro::types::Value;
use tracing::{debug, info, warn};

use crate::datahub::events::{avro_value_to_mcl, avro_value_to_mcp, ChangeType};
use crate::store::StateStore;

/// Decode and apply a Kafka event to the state store.
/// Returns true if the store state changed.
pub fn handle_event(topic: &str, avro_val: Value, store: &mut StateStore) -> bool {
    if topic.contains("MetadataChangeProposal") {
        handle_mcp(avro_val, store)
    } else {
        handle_mcl(avro_val, store)
    }
}

fn handle_mcl(avro_val: Value, store: &mut StateStore) -> bool {
    let Some(mcl) = avro_value_to_mcl(&avro_val) else {
        warn!("Could not parse MCL envelope from Avro value");
        return false;
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

fn handle_mcp(avro_val: Value, store: &mut StateStore) -> bool {
    let Some(mcp) = avro_value_to_mcp(&avro_val) else {
        warn!("Could not parse MCP envelope from Avro value");
        return false;
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
) -> bool {
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
            return false;
        }
    };

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

    changed
}
