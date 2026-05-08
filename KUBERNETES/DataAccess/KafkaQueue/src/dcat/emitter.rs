use serde_json::{json, Value as JsonValue};

use crate::datahub::events::ChangeType;

// ---------------------------------------------------------------------------
// URN helpers (mirrors HTTPClient/src/datahub/dto.rs)
// ---------------------------------------------------------------------------

/// Extract a plain ID from a DataHub URN by taking the last comma-separated
/// segment and stripping the trailing `)`.
///
/// "urn:li:dataset:(urn:li:dataPlatform:iceberg,sales_transactions,PROD)" → "PROD"
/// "urn:li:container:18ec2dadc6c4ea2119fa86dad53f44a7"                  → full URN
pub fn id_from_urn(urn: &str) -> String {
    urn.split(',')
        .last()
        .unwrap_or(urn)
        .trim_end_matches(')')
        .to_string()
}

// ---------------------------------------------------------------------------
// DCAT3 emission
// ---------------------------------------------------------------------------

/// Build a DCAT3 Resource JSON object from a Kafka aspect payload.
///
/// Returns `Some` only when:
/// - change_type is Upsert or Restate
/// - aspect_name is a properties aspect (containerProperties / datasetProperties / dataFlowProperties)
/// - aspect_value is present and contains a non-empty `name` field
///
/// Returns `None` for status, ownership, schemaMetadata, DELETE, and unknown types.
pub fn emit_resource(
    entity_type: &str,
    urn: &str,
    aspect_name: &str,
    change_type: &ChangeType,
    aspect_value: Option<&JsonValue>,
) -> Option<JsonValue> {
    match change_type {
        ChangeType::Upsert | ChangeType::Restate => {}
        _ => return None,
    }

    let aspect = aspect_value?;

    match (entity_type, aspect_name) {
        ("container", "containerProperties") => {
            let title = aspect.get("name").and_then(|v| v.as_str())?;
            Some(json!({
                "id": id_from_urn(urn),
                "resource_type": "catalog",
                "dcterms_title": title,
            }))
        }

        ("dataset", "datasetProperties") => {
            let title = aspect.get("name").and_then(|v| v.as_str())?;
            let is_dist = aspect
                .pointer("/customProperties/dcat_distribution")
                .and_then(|v| v.as_str())
                == Some("true");
            let resource_type = if is_dist { "distribution" } else { "dataset" };
            Some(json!({
                "id": id_from_urn(urn),
                "resource_type": resource_type,
                "dcterms_title": title,
            }))
        }

        ("dataFlow", "dataFlowProperties") => {
            let title = aspect.get("name").and_then(|v| v.as_str())?;
            Some(json!({
                "id": id_from_urn(urn),
                "resource_type": "dataservice",
                "dcterms_title": title,
            }))
        }

        _ => None,
    }
}
