//! Low-level DataHub HTTP client.
//!
//! **Read operations** (`list_entities`, `get_entity`) use the GraphQL API
//! (`POST /api/graphql`).  GraphQL reads from the Elasticsearch search index
//! and never deserialises the raw `SystemMetadata` Jackson type from primary
//! storage, so it is unaffected by the "missing type id '__type'" 400 error
//! that appears on the OpenAPI v2 list endpoints when entities were ingested
//! by an older SDK that did not write the polymorphic discriminator field.
//!
//! **Write/delete operations** (`upsert_entity`, `delete_entity`, …) remain
//! on OpenAPI v2 because those code paths do not read systemMetadata back
//! from storage and therefore do not hit the bug.

use anyhow::{anyhow, Result};
use reqwest::{header, Client, StatusCode};
use serde_json::{json, Map, Value};

use crate::config::DatahubConfig;

use super::dto::{DhEntity, DhRelationshipList};

#[derive(Clone)]
pub struct DatahubClient {
    pub(crate) http: Client,
    pub(crate) base_url: String,
}

impl DatahubClient {
    /// Construct a new client with a pre-configured Authorization header.
    ///
    /// Content-Type is NOT set as a default header — it is only sent on
    /// requests that carry a JSON body (POST/PUT), where reqwest's `.json()`
    /// builder sets it automatically.
    pub fn new(cfg: &DatahubConfig) -> Result<Self> {
        let mut headers = header::HeaderMap::new();
        let auth_value = format!("Bearer {}", cfg.token);
        let mut auth_header = header::HeaderValue::from_str(&auth_value)
            .map_err(|e| anyhow!("Invalid token: {e}"))?;
        auth_header.set_sensitive(true);
        headers.insert(header::AUTHORIZATION, auth_header);
        headers.insert(
            header::ACCEPT,
            header::HeaderValue::from_static("application/json"),
        );

        let http = Client::builder().default_headers(headers).build()?;

        Ok(Self {
            http,
            base_url: cfg.base_url.trim_end_matches('/').to_string(),
        })
    }

    // -------------------------------------------------------------------------
    // GraphQL helpers (private)
    // -------------------------------------------------------------------------

    /// Execute a GraphQL query against `POST /api/graphql`.
    /// Surfaces both HTTP-level errors and GraphQL `errors` arrays as `Err`.
    async fn graphql(&self, query: &str, variables: Value) -> Result<Value> {
        let url = format!("{}/api/graphql", self.base_url);
        let body = json!({ "query": query, "variables": variables });
        let resp = self.http.post(&url).json(&body).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("DataHub GraphQL request failed ({status}): {text}"));
        }
        let val: Value = resp.json().await?;
        if let Some(errors) = val.get("errors") {
            return Err(anyhow!("DataHub GraphQL errors: {errors}"));
        }
        Ok(val)
    }

    /// Return the GraphQL enum name and typed inline fragment for an entity type.
    ///
    /// The fragment selects the aspect fields that the rest of the codebase
    /// expects to find inside `DhEntity.aspects["aspects"][aspectName]["value"]`.
    fn gql_type_and_fragment(entity_type: &str) -> (&'static str, &'static str) {
        match entity_type {
            "dataset" => (
                "DATASET",
                "... on Dataset {
                    properties {
                        name
                        description
                        externalUrl
                        customProperties { key value }
                    }
                    ownership {
                        owners {
                            owner {
                                ... on CorpUser { username }
                                ... on CorpGroup { name }
                            }
                        }
                    }
                }",
            ),
            "container" => (
                "CONTAINER",
                "... on Container {
                    properties {
                        name
                        description
                        customProperties { key value }
                    }
                }",
            ),
            "dataFlow" => (
                "DATA_FLOW",
                "... on DataFlow {
                    properties {
                        name
                        description
                        externalUrl
                    }
                }",
            ),
            "domain" => (
                "DOMAIN",
                "... on Domain {
                    properties {
                        name
                        description
                    }
                }",
            ),
            "tag" => (
                "TAG",
                "... on Tag {
                    properties {
                        name
                        description
                    }
                }",
            ),
            _ => ("DATASET", ""),
        }
    }

    /// Convert a `customProperties` GraphQL array (`[{key, value}]`) into the
    /// flat object (`{key: value}`) that the v2-style mapping functions expect.
    fn gql_custom_props(props_node: &Value) -> Value {
        props_node
            .get("customProperties")
            .and_then(|v| v.as_array())
            .map(|arr| {
                let mut map = Map::new();
                for item in arr {
                    if let (Some(k), Some(v)) = (
                        item.get("key").and_then(Value::as_str),
                        item.get("value").and_then(Value::as_str),
                    ) {
                        map.insert(k.to_string(), Value::String(v.to_string()));
                    }
                }
                Value::Object(map)
            })
            .unwrap_or(Value::Null)
    }

    /// Convert a single GraphQL entity node into the `DhEntity` shape that
    /// the rest of the codebase expects:
    ///
    /// ```json
    /// {
    ///   "urn": "urn:li:…",
    ///   "aspects": {
    ///     "<aspectName>": { "value": { … } }
    ///   }
    /// }
    /// ```
    ///
    /// Returns `None` when the node has no `urn` (e.g. a null entity in the
    /// search results).
    fn gql_entity_to_dh(entity_type: &str, gql: &Value) -> Option<DhEntity> {
        let urn = gql.get("urn")?.as_str().map(String::from)?;
        let props = gql.get("properties").unwrap_or(&Value::Null);

        let aspects = match entity_type {
            "dataset" => json!({
                "aspects": {
                    "datasetProperties": {
                        "value": {
                            "name":        props.get("name"),
                            "description": props.get("description"),
                            "externalUrl": props.get("externalUrl"),
                            "customProperties": Self::gql_custom_props(props),
                        }
                    },
                    "ownership": {
                        "value": gql.get("ownership").unwrap_or(&Value::Null)
                    }
                }
            }),
            "container" => json!({
                "aspects": {
                    "containerProperties": {
                        "value": {
                            "name":        props.get("name"),
                            "description": props.get("description"),
                            "customProperties": Self::gql_custom_props(props),
                        }
                    }
                }
            }),
            "dataFlow" => json!({
                "aspects": {
                    "dataFlowProperties": {
                        "value": {
                            "name":        props.get("name"),
                            "description": props.get("description"),
                            "externalUrl": props.get("externalUrl"),
                        }
                    }
                }
            }),
            "domain" => json!({
                "aspects": {
                    "domainProperties": {
                        "value": {
                            "name":        props.get("name"),
                            "description": props.get("description"),
                        }
                    }
                }
            }),
            "tag" => json!({
                "aspects": {
                    "tagProperties": {
                        "value": {
                            "name":        props.get("name"),
                            "description": props.get("description"),
                        }
                    }
                }
            }),
            _ => json!({ "aspects": {} }),
        };

        Some(DhEntity { urn: Some(urn), aspects })
    }

    // -------------------------------------------------------------------------
    // Entity CRUD helpers (public)
    // -------------------------------------------------------------------------

    /// List entities via GraphQL search.
    ///
    /// GraphQL reads from the Elasticsearch search index, avoiding the
    /// Jackson `SystemMetadata` polymorphism error that affects the v2 list
    /// endpoint.
    ///
    /// The `aspects` parameter is ignored here (GraphQL fragments are fixed
    /// per entity type) but kept for API compatibility with all callers.
    ///
    /// An optional `("query", "…")` entry in `query_params` is forwarded as
    /// the GraphQL search expression; `"*"` is used when none is provided.
    pub async fn list_entities(
        &self,
        entity_type: &str,
        _aspects: &[&str],
        query_params: &[(&str, &str)],
    ) -> Result<Vec<DhEntity>> {
        let search_query = query_params
            .iter()
            .find(|(k, _)| *k == "query")
            .map(|(_, v)| *v)
            .unwrap_or("*");

        let (gql_type, fragment) = Self::gql_type_and_fragment(entity_type);

        let gql = format!(
            r#"query Search($q: String!) {{
                search(input: {{ type: {gql_type}, query: $q, start: 0, count: 200 }}) {{
                    searchResults {{
                        entity {{
                            urn
                            {fragment}
                        }}
                    }}
                }}
            }}"#
        );

        let resp = self.graphql(&gql, json!({ "q": search_query })).await?;

        let results = resp
            .pointer("/data/search/searchResults")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        Ok(results
            .iter()
            .filter_map(|r| r.get("entity"))
            .filter_map(|e| Self::gql_entity_to_dh(entity_type, e))
            .collect())
    }

    /// Fetch a single entity by URN via GraphQL.
    ///
    /// Returns `Err("NOT_FOUND:…")` when DataHub returns a null entity,
    /// matching the contract that all callers expect.
    pub async fn get_entity(
        &self,
        entity_type: &str,
        urn: &str,
        _aspects: &[&str],
    ) -> Result<DhEntity> {
        let (_, fragment) = Self::gql_type_and_fragment(entity_type);

        let gql = format!(
            r#"query GetEntity($urn: String!) {{
                entity(urn: $urn) {{
                    urn
                    {fragment}
                }}
            }}"#
        );

        let resp = self
            .graphql(&gql, json!({ "urn": urn }))
            .await
            .map_err(|e| {
                // Preserve NOT_FOUND sentinel so callers can map it to 404.
                if e.to_string().contains("NOT_FOUND") {
                    anyhow!("NOT_FOUND:{urn}")
                } else {
                    e
                }
            })?;

        let entity_node = resp.pointer("/data/entity");
        match entity_node {
            None | Some(Value::Null) => Err(anyhow!("NOT_FOUND:{urn}")),
            Some(node) => Self::gql_entity_to_dh(entity_type, node)
                .ok_or_else(|| anyhow!("NOT_FOUND:{urn}")),
        }
    }

    /// POST /openapi/v2/entity/{entity_type}  (upsert one or more entities)
    pub async fn upsert_entity(&self, entity_type: &str, payload: Value) -> Result<DhEntity> {
        let url = format!("{}/openapi/v2/entity/{}", self.base_url, entity_type);
        let body = if payload.is_array() {
            payload
        } else {
            Value::Array(vec![payload])
        };
        let resp = self.http.post(&url).json(&body).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "DataHub upsert {entity_type} failed ({status}): {body}"
            ));
        }
        let mut results: Vec<DhEntity> = resp.json().await?;
        results.pop().ok_or_else(|| anyhow!("Empty upsert response"))
    }

    /// DELETE /openapi/v2/entity/{entity_type}/{urn}
    pub async fn delete_entity(&self, entity_type: &str, urn: &str) -> Result<()> {
        let encoded_urn = urlencoding::encode(urn);
        let url = format!(
            "{}/openapi/v2/entity/{}/{}",
            self.base_url, entity_type, encoded_urn
        );
        let resp = self.http.delete(&url).send().await?;
        if !resp.status().is_success() && resp.status() != StatusCode::NOT_FOUND {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "DataHub delete {entity_type}/{urn} failed ({status}): {body}"
            ));
        }
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Aspect helpers (v2 — write-path only, unaffected by the read-side bug)
    // -------------------------------------------------------------------------

    /// GET /openapi/v2/entity/{entity_type}/{urn}/{aspect_name}
    pub async fn get_aspect(
        &self,
        entity_type: &str,
        urn: &str,
        aspect_name: &str,
    ) -> Result<Value> {
        let encoded_urn = urlencoding::encode(urn);
        let url = format!(
            "{}/openapi/v2/entity/{}/{}/{}",
            self.base_url, entity_type, encoded_urn, aspect_name
        );
        let resp = self.http.get(&url).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "DataHub get aspect {aspect_name} failed ({status}): {body}"
            ));
        }
        Ok(resp.json().await?)
    }

    /// POST /openapi/v2/entity/{entity_type}/{urn}/{aspect_name}
    pub async fn upsert_aspect(
        &self,
        entity_type: &str,
        urn: &str,
        aspect_name: &str,
        payload: Value,
    ) -> Result<()> {
        let encoded_urn = urlencoding::encode(urn);
        let url = format!(
            "{}/openapi/v2/entity/{}/{}/{}",
            self.base_url, entity_type, encoded_urn, aspect_name
        );
        let resp = self.http.post(&url).json(&payload).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "DataHub upsert aspect {aspect_name} failed ({status}): {body}"
            ));
        }
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Relationship helpers
    // -------------------------------------------------------------------------

    /// GET /openapi/relationships/v1/?urn=...&relationshipTypes=...&direction=...
    pub async fn get_relationships(
        &self,
        urn: &str,
        relationship_types: &[&str],
        direction: &str,
    ) -> Result<DhRelationshipList> {
        let url = format!("{}/openapi/relationships/v1/", self.base_url);
        let rel_types = relationship_types.join(",");
        let resp = self
            .http
            .get(&url)
            .query(&[
                ("urn", urn),
                ("relationshipTypes", &rel_types),
                ("direction", direction),
                ("start", "0"),
                ("count", "200"),
            ])
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("DataHub relationships failed ({status}): {body}"));
        }
        Ok(resp.json().await?)
    }

    // -------------------------------------------------------------------------
    // Legacy entities v1 helper
    // -------------------------------------------------------------------------

    /// GET /openapi/entities/v1/latest?urns=...&aspectNames=...
    pub async fn get_entity_aspects(&self, urn: &str, aspect_names: &[&str]) -> Result<Value> {
        let url = format!("{}/openapi/entities/v1/latest", self.base_url);
        let aspects = aspect_names.join(",");
        let resp = self
            .http
            .get(&url)
            .query(&[("urns", urn), ("aspectNames", &aspects)])
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "DataHub entity aspects failed ({status}): {body}"
            ));
        }
        Ok(resp.json().await?)
    }
}
