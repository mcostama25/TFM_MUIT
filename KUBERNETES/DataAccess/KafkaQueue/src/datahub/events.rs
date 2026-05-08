use apache_avro::types::Value;
use serde_json::Value as JsonValue;
use tracing::warn;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// High-level enum wrapping either a MetadataChangeLog or MetadataChangeProposal.
#[derive(Debug, Clone)]
pub enum DataHubEvent {
    Mcl(MclEnvelope),
    Mcp(McpEnvelope),
}

/// Parsed envelope for a MetadataChangeLog event (versioned or timeseries).
#[derive(Debug, Clone)]
pub struct MclEnvelope {
    pub entity_type: String,
    pub entity_urn: String,
    pub aspect_name: String,
    pub change_type: ChangeType,
    /// The decoded aspect payload (JSON).  `None` on DELETE or when absent.
    pub aspect_value: Option<JsonValue>,
    /// Epoch-millis timestamp from the `createdOn` field.
    pub created_on: Option<i64>,
}

/// Parsed envelope for a MetadataChangeProposal event.
#[derive(Debug, Clone)]
pub struct McpEnvelope {
    pub entity_type: String,
    pub entity_urn: String,
    pub aspect_name: String,
    pub change_type: ChangeType,
    pub aspect_value: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ChangeType {
    Upsert,
    Delete,
    Restate,
    Unknown(String),
}

impl std::fmt::Display for ChangeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChangeType::Upsert => write!(f, "UPSERT"),
            ChangeType::Delete => write!(f, "DELETE"),
            ChangeType::Restate => write!(f, "RESTATE"),
            ChangeType::Unknown(s) => write!(f, "UNKNOWN({s})"),
        }
    }
}

// ---------------------------------------------------------------------------
// Extraction helpers
// ---------------------------------------------------------------------------

/// Try to parse an MCL `Record` into `MclEnvelope`.
pub fn avro_value_to_mcl(val: &Value) -> Option<MclEnvelope> {
    let fields = record_fields(val)?;

    let entity_type = extract_string(fields, "entityType")?;
    let entity_urn = extract_string(fields, "entityUrn")?;
    let aspect_name = extract_string(fields, "aspectName")?;
    let change_type = extract_change_type(fields);
    let aspect_value = extract_aspect_value(fields);
    let created_on = extract_long(fields, "createdOn");

    Some(MclEnvelope {
        entity_type,
        entity_urn,
        aspect_name,
        change_type,
        aspect_value,
        created_on,
    })
}

/// Try to parse an MCP `Record` into `McpEnvelope`.
pub fn avro_value_to_mcp(val: &Value) -> Option<McpEnvelope> {
    let fields = record_fields(val)?;

    let entity_type = extract_string(fields, "entityType")?;
    let entity_urn = extract_string(fields, "entityUrn")?;
    let aspect_name = extract_string(fields, "aspectName")?;
    let change_type = extract_change_type(fields);
    let aspect_value = extract_aspect_value(fields);

    Some(McpEnvelope {
        entity_type,
        entity_urn,
        aspect_name,
        change_type,
        aspect_value,
    })
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn record_fields(val: &Value) -> Option<&Vec<(String, Value)>> {
    if let Value::Record(fields) = val {
        Some(fields)
    } else {
        None
    }
}

fn find_field<'a>(fields: &'a [(String, Value)], name: &str) -> Option<&'a Value> {
    fields.iter().find(|(k, _)| k == name).map(|(_, v)| v)
}

fn extract_string(fields: &[(String, Value)], name: &str) -> Option<String> {
    match find_field(fields, name)? {
        Value::String(s) => Some(s.clone()),
        // Sometimes wrapped in a Union
        Value::Union(_, inner) => {
            if let Value::String(s) = inner.as_ref() {
                Some(s.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

fn extract_long(fields: &[(String, Value)], name: &str) -> Option<i64> {
    match find_field(fields, name)? {
        Value::Long(n) => Some(*n),
        Value::Union(_, inner) => {
            if let Value::Long(n) = inner.as_ref() {
                Some(*n)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn extract_change_type(fields: &[(String, Value)]) -> ChangeType {
    match find_field(fields, "changeType") {
        Some(Value::Enum(_, s)) => match s.as_str() {
            "UPSERT" => ChangeType::Upsert,
            "DELETE" => ChangeType::Delete,
            "RESTATE" => ChangeType::Restate,
            other => ChangeType::Unknown(other.to_owned()),
        },
        Some(Value::String(s)) => match s.as_str() {
            "UPSERT" => ChangeType::Upsert,
            "DELETE" => ChangeType::Delete,
            "RESTATE" => ChangeType::Restate,
            other => ChangeType::Unknown(other.to_owned()),
        },
        _ => ChangeType::Unknown("missing".into()),
    }
}

/// Extracts the `aspect` field from an MCL/MCP record.
///
/// DataHub's `aspect` is typed as `union[null, GenericAspect]`.
/// `GenericAspect` is a record with:
///   - `contentType`: string (always "application/json" for modern DataHub)
///   - `value`:       bytes (UTF-8 JSON string)
fn extract_aspect_value(fields: &[(String, Value)]) -> Option<JsonValue> {
    let aspect_field = find_field(fields, "aspect")?;

    // Unwrap Union variant
    let inner = match aspect_field {
        Value::Union(_, inner) => inner.as_ref(),
        other => other,
    };

    // Should be a Record (GenericAspect)
    let generic_fields = match inner {
        Value::Record(f) => f,
        Value::Null => return None,
        _ => {
            warn!("Unexpected aspect value type: {:?}", inner);
            return None;
        }
    };

    // Extract the "value" bytes field
    let value_bytes = match find_field(generic_fields, "value")? {
        Value::Bytes(b) => b,
        _ => return None,
    };

    let json_str = match std::str::from_utf8(value_bytes) {
        Ok(s) => s,
        Err(e) => {
            warn!("Aspect value is not valid UTF-8: {e}");
            return None;
        }
    };

    match serde_json::from_str(json_str) {
        Ok(v) => Some(v),
        Err(e) => {
            warn!("Aspect value is not valid JSON: {e}");
            None
        }
    }
}
