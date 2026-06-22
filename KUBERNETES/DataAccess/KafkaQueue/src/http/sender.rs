use std::time::Duration;

use reqwest::Client;
use serde_json::{json, Value};
use tracing::{debug, warn};

use super::HttpAction;

pub struct HttpSender {
    client: Client,
    base_url: String,
}

impl HttpSender {
    pub fn new(server_url: &str) -> Self {
        Self {
            client: Client::new(),
            base_url: server_url.trim_end_matches('/').to_owned(),
        }
    }

    pub async fn execute(&self, action: &HttpAction) {
        match action {
            HttpAction::UpsertCatalog { urn, title } => self.upsert_catalog(urn, title).await,
            HttpAction::UpsertDataset { urn } => self.upsert_dataset(urn).await,
            HttpAction::DeleteCatalog { urn } => self.delete_resource("/catalogs", urn).await,
            HttpAction::DeleteDataset { urn } => self.delete_resource("/datasets", urn).await,
            HttpAction::PatchPublisher { urn, publisher } => {
                self.patch_publisher(urn, publisher).await
            }
            HttpAction::PatchSchema { urn, fields } => self.patch_schema(urn, fields).await,
        }
    }

    async fn upsert_catalog(&self, urn: &str, title: &str) {
        let payload = json!({
            "dcterms_identifier": urn,
            "dcterms_title": title,
        });
        match self.post("/catalogs", &payload).await {
            Ok(resp) if resp.status().as_u16() == 409 => {
                let path = format!("/catalogs/{}", encode_urn(urn));
                if let Some(mut existing) = self.get_json(&path).await {
                    merge_json(&mut existing, &payload);
                    if let Err(e) = self.put(&path, &existing).await {
                        warn!(urn, error = %e, "PUT catalog failed after 409");
                    }
                }
            }
            Ok(resp) => debug!(urn, status = resp.status().as_u16(), "POST /catalogs"),
            Err(e) => warn!(urn, error = %e, "POST /catalogs failed"),
        }
    }

    async fn upsert_dataset(&self, urn: &str) {
        let payload = json!({
            "dcterms_identifier": urn,
            "dcterms_title": urn,
        });

        let first_resp = if let Some(platform) = extract_platform(urn) {
            let catalog_id = encode_urn(&format!("urn:li:dataPlatform:{}", platform));
            let path = format!("/catalogs/{}/datasets", catalog_id);
            match self.post(&path, &payload).await {
                Ok(resp) if resp.status().as_u16() == 404 => {
                    self.post("/datasets", &payload).await
                }
                other => other,
            }
        } else {
            self.post("/datasets", &payload).await
        };

        match first_resp {
            Ok(resp) if resp.status().as_u16() == 409 => {
                let path = format!("/datasets/{}", encode_urn(urn));
                if let Some(mut existing) = self.get_json(&path).await {
                    merge_json(&mut existing, &payload);
                    if let Err(e) = self.put(&path, &existing).await {
                        warn!(urn, error = %e, "PUT dataset failed after 409");
                    }
                }
            }
            Ok(resp) => debug!(urn, status = resp.status().as_u16(), "POST dataset"),
            Err(e) => warn!(urn, error = %e, "POST dataset failed"),
        }
    }

    async fn delete_resource(&self, prefix: &str, urn: &str) {
        let url = format!("{}{}/{}", self.base_url, prefix, encode_urn(urn));
        match self.client.delete(&url).timeout(Duration::from_secs(10)).send().await {
            Ok(resp) => debug!(urn, status = resp.status().as_u16(), "DELETE"),
            Err(e) => warn!(urn, error = %e, "DELETE failed"),
        }
    }

    async fn patch_publisher(&self, urn: &str, publisher: &str) {
        let path = format!("/datasets/{}", encode_urn(urn));
        if let Some(mut existing) = self.get_json(&path).await {
            existing["dcterms_publisher"] = json!(publisher);
            if let Err(e) = self.put(&path, &existing).await {
                warn!(urn, error = %e, "PUT publisher patch failed");
            }
        }
    }

    async fn patch_schema(&self, urn: &str, fields: &[String]) {
        let path = format!("/datasets/{}", encode_urn(urn));
        if let Some(mut existing) = self.get_json(&path).await {
            let current = existing.get("dcterms_description").and_then(|v| v.as_str()).unwrap_or("");
            let appended = if current.is_empty() {
                fields.join("; ")
            } else {
                format!("{}; {}", current, fields.join("; "))
            };
            existing["dcterms_description"] = json!(appended);
            if let Err(e) = self.put(&path, &existing).await {
                warn!(urn, error = %e, "PUT schema patch failed");
            }
        }
    }

    async fn post(&self, path: &str, payload: &Value) -> Result<reqwest::Response, reqwest::Error> {
        let url = format!("{}{}", self.base_url, path);
        self.client.post(&url).json(payload).timeout(Duration::from_secs(10)).send().await
    }

    async fn put(&self, path: &str, payload: &Value) -> Result<reqwest::Response, reqwest::Error> {
        let url = format!("{}{}", self.base_url, path);
        self.client.put(&url).json(payload).timeout(Duration::from_secs(10)).send().await
    }

    async fn get_json(&self, path: &str) -> Option<Value> {
        let url = format!("{}{}", self.base_url, path);
        match self.client.get(&url).timeout(Duration::from_secs(10)).send().await {
            Ok(resp) if resp.status().is_success() => resp.json().await.ok(),
            Ok(resp) => {
                debug!(path, status = resp.status().as_u16(), "GET returned non-success");
                None
            }
            Err(e) => {
                warn!(path, error = %e, "GET failed");
                None
            }
        }
    }
}

fn encode_urn(urn: &str) -> String {
    urn.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
                (b as char).to_string()
            } else {
                format!("%{:02X}", b)
            }
        })
        .collect()
}

fn extract_platform(dataset_urn: &str) -> Option<&str> {
    let prefix = "urn:li:dataset:(urn:li:dataPlatform:";
    dataset_urn.strip_prefix(prefix)?.split(',').next()
}

fn merge_json(base: &mut Value, updates: &Value) {
    if let (Value::Object(b), Value::Object(u)) = (base, updates) {
        for (k, v) in u {
            b.insert(k.clone(), v.clone());
        }
    }
}
