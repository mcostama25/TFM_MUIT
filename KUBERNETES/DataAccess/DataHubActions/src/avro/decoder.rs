use std::collections::HashMap;

use apache_avro::{from_avro_datum, Schema};
use serde::Deserialize;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::error::AppError;

/// Confluent Schema Registry wire format:
///   byte 0   : magic byte = 0x00
///   bytes 1-4: schema ID (big-endian u32)
///   bytes 5..: Avro binary payload
const MAGIC_BYTE: u8 = 0x00;
const HEADER_LEN: usize = 5;

/// Fetched schema JSON from the SR API.
#[derive(Deserialize)]
struct SrSchemaResponse {
    schema: String,
}

/// Async Avro decoder backed by Confluent Schema Registry.
///
/// Schemas are cached indefinitely (schema IDs are immutable once registered).
pub struct AvroDecoder {
    /// Base URL of the Schema Registry (e.g. "http://datahub-datahub-gms:8080").
    /// Schema fetch: GET {sr_url}/schema-registry/api/schemas/ids/{id}
    sr_url: String,
    http: reqwest::Client,
    cache: RwLock<HashMap<u32, Schema>>,
}

impl AvroDecoder {
    pub fn new(sr_url: &str) -> Self {
        Self {
            sr_url: sr_url.trim_end_matches('/').to_owned(),
            http: reqwest::Client::new(),
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Decode a Confluent wire-format Avro payload into an apache-avro `Value`.
    pub async fn decode(&self, payload: &[u8]) -> Result<apache_avro::types::Value, AppError> {
        if payload.is_empty() {
            return Err(AppError::AvroDecode("empty payload".into()));
        }
        if payload[0] != MAGIC_BYTE {
            return Err(AppError::AvroDecode(format!(
                "invalid magic byte: 0x{:02x} (expected 0x00)",
                payload[0]
            )));
        }
        if payload.len() < HEADER_LEN {
            return Err(AppError::AvroDecode(format!(
                "payload too short: {} bytes",
                payload.len()
            )));
        }

        let schema_id = u32::from_be_bytes(
            payload[1..5]
                .try_into()
                .map_err(|_| AppError::AvroDecode("cannot read schema ID bytes".into()))?,
        );

        let schema = self.get_schema(schema_id).await?;
        let avro_bytes = &payload[HEADER_LEN..];
        let value = from_avro_datum(&schema, &mut &avro_bytes[..], None)?;

        Ok(value)
    }

    /// Returns a cached schema or fetches it from the Schema Registry.
    async fn get_schema(&self, schema_id: u32) -> Result<Schema, AppError> {
        // Fast path: read lock
        {
            let cache = self.cache.read().await;
            if let Some(schema) = cache.get(&schema_id) {
                return Ok(schema.clone());
            }
        }

        // Slow path: fetch and insert
        let schema = self.fetch_schema(schema_id).await?;
        self.cache.write().await.insert(schema_id, schema.clone());
        Ok(schema)
    }

    async fn fetch_schema(&self, schema_id: u32) -> Result<Schema, AppError> {
        let url = format!(
            "{}/schema-registry/api/schemas/ids/{}",
            self.sr_url, schema_id
        );
        debug!(schema_id, url = %url, "Fetching schema from registry");

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| AppError::SchemaRegistry(format!("SR request failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!(schema_id, %status, "Schema registry returned error: {body}");
            return Err(AppError::SchemaRegistry(format!(
                "SR returned {status} for schema {schema_id}: {body}"
            )));
        }

        let sr_resp: SrSchemaResponse = resp
            .json()
            .await
            .map_err(|e| AppError::SchemaRegistry(format!("SR response parse failed: {e}")))?;

        Schema::parse_str(&sr_resp.schema)
            .map_err(|e| AppError::AvroDecode(format!("Schema parse failed: {e}")))
    }
}
