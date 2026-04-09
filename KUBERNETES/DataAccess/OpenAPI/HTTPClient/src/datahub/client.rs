//! Low-level DataHub REST API v2 HTTP client.
//!
//! All higher-level repository implementations delegate to this client
//! for the actual HTTP calls.

use anyhow::{anyhow, Result};
use reqwest::{header, Client, StatusCode};
use serde_json::Value;

use crate::config::DatahubConfig;

use super::dto::{DhEntity, DhEntityList, DhRelationshipList};

#[derive(Clone)]
pub struct DatahubClient {
    pub(crate) http: Client,
    pub(crate) base_url: String,
}

impl DatahubClient {
    /// Construct a new client with a pre-configured Authorization header.
    pub fn new(cfg: &DatahubConfig) -> Result<Self> {
        let mut headers = header::HeaderMap::new();
        let auth_value = format!("Bearer {}", cfg.token);
        let mut auth_header = header::HeaderValue::from_str(&auth_value)
            .map_err(|e| anyhow!("Invalid token: {e}"))?;
        auth_header.set_sensitive(true);
        headers.insert(header::AUTHORIZATION, auth_header);
        headers.insert(header::ACCEPT, header::HeaderValue::from_static("application/json"));
        headers.insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/json"));

        let http = Client::builder()
            .default_headers(headers)
            .build()?;

        Ok(Self {
            http,
            base_url: cfg.base_url.trim_end_matches('/').to_string(),
        })
    }

    // -------------------------------------------------------------------------
    // Entity CRUD helpers — wrap DataHub OpenAPI v2 entity endpoints
    // -------------------------------------------------------------------------

    /// GET /openapi/v2/entity/{entity_type}?count=200
    pub async fn list_entities(
        &self,
        entity_type: &str,
        query_params: &[(&str, &str)],
    ) -> Result<Vec<DhEntity>> {
        let url = format!("{}/openapi/v2/entity/{}", self.base_url, entity_type);
        let mut req = self.http.get(&url).query(&[("count", "200")]);
        for (k, v) in query_params {
            req = req.query(&[(k, v)]);
        }
        let resp = req.send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("DataHub list {entity_type} failed ({status}): {body}"));
        }
        let list: DhEntityList = resp.json().await?;
        Ok(list.entities)
    }

    /// GET /openapi/v2/entity/{entity_type}/{urn}
    pub async fn get_entity(&self, entity_type: &str, urn: &str) -> Result<DhEntity> {
        let encoded_urn = urlencoding::encode(urn);
        let url = format!("{}/openapi/v2/entity/{}/{}", self.base_url, entity_type, encoded_urn);
        let resp = self.http.get(&url).send().await?;
        match resp.status() {
            StatusCode::NOT_FOUND => {
                return Err(anyhow!("NOT_FOUND:{urn}"));
            }
            s if !s.is_success() => {
                let body = resp.text().await.unwrap_or_default();
                return Err(anyhow!("DataHub get {entity_type}/{urn} failed ({s}): {body}"));
            }
            _ => {}
        }
        Ok(resp.json().await?)
    }

    /// POST /openapi/v2/entity/{entity_type}  (upsert one or more entities)
    pub async fn upsert_entity(&self, entity_type: &str, payload: Value) -> Result<DhEntity> {
        let url = format!("{}/openapi/v2/entity/{}", self.base_url, entity_type);
        // DataHub expects an array of entity upsert requests
        let body = if payload.is_array() { payload } else { Value::Array(vec![payload]) };
        let resp = self.http.post(&url).json(&body).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("DataHub upsert {entity_type} failed ({status}): {body}"));
        }
        // DataHub returns an array; take the first element
        let mut results: Vec<DhEntity> = resp.json().await?;
        results.pop().ok_or_else(|| anyhow!("Empty upsert response"))
    }

    /// DELETE /openapi/v2/entity/{entity_type}/{urn}
    pub async fn delete_entity(&self, entity_type: &str, urn: &str) -> Result<()> {
        let encoded_urn = urlencoding::encode(urn);
        let url = format!("{}/openapi/v2/entity/{}/{}", self.base_url, entity_type, encoded_urn);
        let resp = self.http.delete(&url).send().await?;
        if !resp.status().is_success() && resp.status() != StatusCode::NOT_FOUND {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("DataHub delete {entity_type}/{urn} failed ({status}): {body}"));
        }
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Aspect helpers
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
            return Err(anyhow!("DataHub get aspect {aspect_name} failed ({status}): {body}"));
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
            return Err(anyhow!("DataHub upsert aspect {aspect_name} failed ({status}): {body}"));
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
    // Legacy entities v1 helper (used for aspect-level fetches)
    // -------------------------------------------------------------------------

    /// GET /openapi/entities/v1/latest?urns=...&aspectNames=...
    pub async fn get_entity_aspects(
        &self,
        urn: &str,
        aspect_names: &[&str],
    ) -> Result<Value> {
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
            return Err(anyhow!("DataHub entity aspects failed ({status}): {body}"));
        }
        Ok(resp.json().await?)
    }
}
