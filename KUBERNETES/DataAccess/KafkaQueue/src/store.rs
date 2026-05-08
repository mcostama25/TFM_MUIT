use std::collections::HashMap;
use std::io::{BufRead, Write};

use serde_json::Value as JsonValue;
use tracing::{info, warn};

use crate::datahub::events::ChangeType;
use crate::models::common::Resource;

#[derive(Debug, Default, Clone)]
pub struct EntityState {
    pub urn: String,
    pub entity_type: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub is_distribution: bool,
    pub owners: Vec<String>,
    pub schema_fields: Vec<String>,
}

pub struct StateStore {
    entities: HashMap<String, EntityState>,
    output_path: String,
}

impl StateStore {
    pub fn new(output_path: String) -> Self {
        Self {
            entities: HashMap::new(),
            output_path,
        }
    }

    /// Apply an incoming aspect to the store. Returns true if state changed.
    pub fn apply(
        &mut self,
        urn: &str,
        entity_type: &str,
        aspect_name: &str,
        aspect_value: Option<&JsonValue>,
        change_type: &ChangeType,
    ) -> bool {
        if matches!(change_type, ChangeType::Delete) {
            return self.delete(urn);
        }

        if aspect_name == "status" {
            let removed = aspect_value
                .and_then(|v| v.get("removed"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            return if removed { self.delete(urn) } else { false };
        }

        let state = self.entities.entry(urn.to_owned()).or_insert_with(|| EntityState {
            urn: urn.to_owned(),
            entity_type: entity_type.to_owned(),
            ..Default::default()
        });

        match aspect_name {
            "containerProperties" | "datasetProperties" | "dataFlowProperties" => {
                if let Some(v) = aspect_value {
                    state.title = v.get("name").and_then(|n| n.as_str()).map(str::to_owned);
                    state.description =
                        v.get("description").and_then(|d| d.as_str()).map(str::to_owned);
                    state.is_distribution = v
                        .pointer("/customProperties/dcat_distribution")
                        .and_then(|v| v.as_str())
                        == Some("true");
                }
                true
            }
            "ownership" => {
                if let Some(v) = aspect_value {
                    state.owners = v
                        .get("owners")
                        .and_then(|o| o.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|o| {
                                    o.get("owner").and_then(|v| v.as_str()).map(str::to_owned)
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                }
                true
            }
            "schemaMetadata" => {
                if let Some(v) = aspect_value {
                    state.schema_fields = v
                        .get("fields")
                        .and_then(|f| f.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|f| {
                                    f.get("fieldPath").and_then(|v| v.as_str()).map(str::to_owned)
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                }
                true
            }
            _ => false,
        }
    }

    /// Remove an entity from the store. Returns true if it existed.
    pub fn delete(&mut self, urn: &str) -> bool {
        self.entities.remove(urn).is_some()
    }

    /// Build DCAT3 Resource records for all entities in the store.
    pub fn snapshot(&self) -> Vec<Resource> {
        self.entities.values().filter_map(entity_to_resource).collect()
    }

    /// Rewrite the NDJSON output file with the current snapshot.
    pub fn flush(&self) {
        let path = &self.output_path;
        match std::fs::File::create(path) {
            Ok(mut file) => {
                for r in self.snapshot() {
                    match serde_json::to_string(&r) {
                        Ok(line) => {
                            if let Err(e) = writeln!(file, "{}", line) {
                                warn!(path, error = %e, "Failed to write NDJSON line");
                            }
                        }
                        Err(e) => warn!(error = %e, "Failed to serialize resource"),
                    }
                }
            }
            Err(e) => warn!(path, error = %e, "Failed to open NDJSON output file for write"),
        }
    }

    /// Populate the store from an existing NDJSON file (warm restart).
    pub fn load_from_ndjson(&mut self) {
        let path = self.output_path.clone();
        let file = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(_) => return,
        };
        let mut count = 0usize;
        for line in std::io::BufReader::new(file).lines() {
            let Ok(line) = line else { continue };
            if line.trim().is_empty() {
                continue;
            }
            let Ok(resource) = serde_json::from_str::<Resource>(&line) else {
                continue;
            };
            let (Some(urn), Some(rt)) = (resource.id, resource.resource_type) else {
                continue;
            };
            let entity_type = match rt.as_str() {
                "catalog" => "container",
                "dataset" | "distribution" => "dataset",
                "dataservice" => "dataFlow",
                _ => continue,
            };
            let state = EntityState {
                urn: urn.clone(),
                entity_type: entity_type.to_owned(),
                title: resource.dcterms_title,
                description: resource.dcterms_description,
                is_distribution: rt == "distribution",
                owners: resource.owners.unwrap_or_default(),
                schema_fields: resource.schema_fields.unwrap_or_default(),
            };
            self.entities.insert(urn, state);
            count += 1;
        }
        if count > 0 {
            info!(count, path, "Restored entity state from NDJSON");
        }
    }
}

fn entity_to_resource(state: &EntityState) -> Option<Resource> {
    let resource_type = match state.entity_type.as_str() {
        "container" => "catalog",
        "dataset" => {
            if state.is_distribution {
                "distribution"
            } else {
                "dataset"
            }
        }
        "dataFlow" => "dataservice",
        _ => return None,
    };
    Some(Resource {
        id: Some(state.urn.clone()),
        resource_type: Some(resource_type.to_owned()),
        dcterms_title: state.title.clone(),
        dcterms_description: state.description.clone(),
        owners: if state.owners.is_empty() { None } else { Some(state.owners.clone()) },
        schema_fields: if state.schema_fields.is_empty() {
            None
        } else {
            Some(state.schema_fields.clone())
        },
    })
}
