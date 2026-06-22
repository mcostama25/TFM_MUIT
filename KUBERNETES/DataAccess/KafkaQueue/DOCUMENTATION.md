# KafkaQueue — DataHub to DCAT3 Event Bridge

## 1. Overview

`datahub-kafka-consumer` is a Rust binary that bridges DataHub's internal Kafka event bus to the external data space layer. It runs as a Kubernetes Deployment alongside the DataHub cluster and performs two complementary functions for every metadata change it observes:

1. **NDJSON snapshot** — maintains an in-memory state store and rewrites a flat `dcat_output.ndjson` file, where each line is one DCAT3 `Resource` record representing the current state of a DataHub entity.
2. **HTTP forwarding** — sends the corresponding REST call (POST, PUT, DELETE) to an external DCAT3 server (`http://10.0.2.15:8080`), using the same API contract as the `DataHubActions` component.

The two outputs are independent: the NDJSON write is triggered only when local state actually changes; the HTTP call is triggered on every recognized aspect event, relying on the server's idempotency (409 → GET + PUT upsert) to handle duplicates.

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────┐
│                  DataHub Cluster (datahub1)          │
│                                                      │
│  UI / Ingestion ──► GMS ──► Kafka broker             │
│                     │        (prerequisites-kafka)   │
│                     │               │                │
│                     │   ┌───────────┴────────────┐   │
│                     │   │ MetadataChangeLog_*     │   │
│                     │   │ MetadataChangeProposal_*│   │
│                     │   └───────────┬────────────┘   │
│                     │               │                │
│                     │    ┌──────────▼──────────┐     │
│                     │    │ datahub-kafka-       │     │
│    Schema Registry  │    │ consumer  (this pod) │     │
│    /schema-registry/│◄───│                      │     │
│    api/schemas/ids/ │    │  AvroDecoder         │     │
│                     │    │  StateStore (HashMap) │     │
│                     │    │         │             │     │
└─────────────────────┘    └─────────┼─────────────┘    
                                     │                  
                    ┌────────────────┼────────────────┐ 
                    │                │                │ 
                    ▼                ▼                │ 
           dcat_output.ndjson   HTTP calls            │ 
           (/app/output)        POST /catalogs        │ 
                                POST /datasets        │ 
                                GET+PUT (upsert)      │ 
                                DELETE /{id}          │ 
                                    │                 │ 
                                    ▼                 │ 
                           DCAT3 REST Server          │ 
                           (host: 10.0.2.15:8080)     │ 
                                                      │ 
                 ───────────────────────────────────── 
```

The pod connects to `prerequisites-kafka:9092` for events and to `datahub-datahub-gms:8080` for schema resolution. The DCAT3 server runs on the host machine, reachable from inside the Minikube VM at `10.0.2.15`.

---

## 3. DCAT3 Mapping

### Entity type mapping

| DataHub entity type | DCAT3 class          | Condition |
|---------------------|----------------------|-----------|
| `container`         | `dcat:Catalog`       | Always |
| `dataset`           | `dcat:Dataset`       | Default |
| `dataset`           | `dcat:Distribution`  | Custom property `dcat_distribution = "true"` |
| `dataFlow`          | `dcat:DataService`   | Always |

All other entity types (users, groups, tags, domains) are silently dropped.

### Aspect mapping

| DataHub aspect        | Extracted fields                              | Effect on DCAT3 resource |
|-----------------------|-----------------------------------------------|--------------------------|
| `containerProperties` | `name`, `description`, `dcat_distribution`   | title, description, resource type |
| `datasetProperties`   | `name`, `description`, `dcat_distribution`   | title, description, resource type |
| `dataFlowProperties`  | `name`, `description`                        | title, description |
| `ownership`           | `owners[].owner` (URN strings)               | `owners` list |
| `schemaMetadata`      | `fields[].fieldPath`                         | `schema_fields` list |
| `status`              | `removed` (bool)                             | entity deletion when `true` |

---

## 4. Data Flow

```
Kafka message (Confluent wire format)
    │
    ▼
AvroDecoder::decode()
    ├── reads 5-byte header (magic + schema_id)
    ├── fetches schema from GMS (cached by schema_id)
    └── returns apache_avro::types::Value
    │
    ▼
datahub/events.rs   avro_value_to_mcl() / avro_value_to_mcp()
    └── MclEnvelope { entity_type, entity_urn, aspect_name, change_type, aspect_value: JsonValue }
    │
    ▼
kafka/handler.rs   handle_event()
    ├── filter: entity_type ∈ {container, dataset, dataFlow}  → else discard
    ├── compute_http_action(aspect_name, entity_type, change_type, aspect_value) → Option<HttpAction>
    └── store.apply(urn, entity_type, aspect_name, aspect_value, change_type) → bool changed
    │
    ├──[changed = true]─────► store.flush()
    │                              └── rewrites /app/output/dcat_output.ndjson
    │                                   (one JSON line per entity, full snapshot)
    │
    └──[HttpAction = Some]──► http_sender.execute(action)
                                   └── POST / PUT / GET+PUT / DELETE
                                        → DCAT3 REST server at 10.0.2.15:8080
```

The two branches are independent: a schema update may produce an `HttpAction` without changing local state (if the fields are identical to what is stored), and vice versa.

---

## 5. Module Breakdown

### `src/config.rs`
Layered configuration: base `config.toml` (baked into the image) overridden by environment variables with `__` separator. All scalar connection parameters (Kafka broker, Schema Registry URL, HTTP server URL) can be overridden at deploy time through a Kubernetes ConfigMap without rebuilding the image.

### `src/avro/decoder.rs`
Implements the Confluent Schema Registry wire format. Extracts the 4-byte schema ID from the message header, fetches the schema JSON from DataHub GMS (`GET /schema-registry/api/schemas/ids/{id}`), and decodes the Avro binary payload. Schemas are cached in a `RwLock<HashMap<u32, Schema>>` for the lifetime of the process — schema IDs are immutable once registered.

### `src/datahub/events.rs`
Translates the raw `apache_avro::types::Value` tree into typed Rust structs: `MclEnvelope` (for MetadataChangeLog) and `McpEnvelope` (for MetadataChangeProposal). The `aspect` field follows DataHub's `GenericAspect` format: a bytes field carrying a UTF-8 JSON string, which this module decodes into a `serde_json::Value`.

### `src/kafka/handler.rs`
Routes events by topic name (MCP vs MCL), filters by entity type, and computes the `HttpAction` variant from the aspect name and payload. Returns `(bool changed, Option<HttpAction>)`. The HTTP action is derived purely from event inputs — it is computed before and independently of the store mutation.

### `src/kafka/consumer.rs`
Main event loop. On startup calls `store.load_from_ndjson()` for warm restart, then subscribes to all configured topics and iterates the async Kafka stream. For each message: decodes Avro, calls `handle_event()`, flushes NDJSON if state changed, executes the HTTP action if one was computed. Decode failures are logged as warnings and skipped — the loop never panics on bad messages. Offset commits are automatic (`enable.auto.commit = true`).

### `src/store.rs`
Core state store. `EntityState` is a flat struct per entity (urn, entity_type, title, description, is_distribution, owners, schema_fields). `StateStore` wraps a `HashMap<String, EntityState>` keyed by URN. The `apply()` method merges one aspect into the existing state; `flush()` rewrites the entire NDJSON file from the current snapshot. The full-rewrite strategy (instead of append) ensures the output is always a de-duplicated snapshot with exactly one line per entity.

### `src/http/sender.rs`
`HttpSender` wraps a `reqwest::Client` and translates `HttpAction` variants into REST calls against the DCAT3 server. Implements the same API contract as the `DataHubActions` component:

| `HttpAction` | HTTP call |
|---|---|
| `UpsertCatalog { urn, title }` | `POST /catalogs`; on 409 → `GET /catalogs/{id}` + `PUT` |
| `UpsertDataset { urn }` | `POST /catalogs/{platform}/datasets`; on 404 → `POST /datasets`; on 409 → `GET+PUT` |
| `DeleteCatalog { urn }` | `DELETE /catalogs/{encoded_urn}` |
| `DeleteDataset { urn }` | `DELETE /datasets/{encoded_urn}` |
| `PatchPublisher { urn, publisher }` | `GET /datasets/{id}` + `PUT` with `dcterms_publisher` |
| `PatchSchema { urn, fields }` | `GET /datasets/{id}` + `PUT` appending fields to `dcterms_description` |

All URNs are percent-encoded before being used as path segments. HTTP errors are logged as `warn` and do not interrupt the Kafka consumer loop.

### `src/health/routes.rs`
Minimal `axum` server on port 3001. Exposes `GET /health → 200 {"status":"ok"}` for Kubernetes liveness and readiness probes. Runs concurrently with the consumer loop via `tokio::join!`.

---

## 6. NDJSON Output Format

Each line of `dcat_output.ndjson` is a self-contained JSON object:

```json
{"id":"urn:li:dataset:(urn:li:dataPlatform:iceberg,sales_transactions,PROD)","resource_type":"dataset","dcterms_title":"Sales Transactions","dcterms_description":"Daily sales records","owners":["urn:li:corpuser:alice"],"schema_fields":["transaction_id","amount","currency"]}
{"id":"urn:li:container:18ec2dadc6c4ea2119fa86dad53f44a7","resource_type":"catalog","dcterms_title":"Iceberg Lakehouse"}
```

Optional fields (`dcterms_description`, `owners`, `schema_fields`) are omitted when absent. The file is a complete snapshot: any reader can scan it linearly to reconstruct the full catalog state.

---

## 7. Configuration

### `config.toml` (base, baked into the image)

```toml
[kafka]
bootstrap_servers = "localhost:9092"
group_id          = "datahub-dcat-consumer-dev"
topics = [
  "MetadataChangeProposal_v1",
  "MetadataChangeLog_Versioned_v1",
  "MetadataChangeLog_Timeseries_v1"
]
auto_offset_reset = "earliest"

[schema_registry]
url = "http://localhost:8080"

[server]
port = 3001

[output]
dcat_file = "dcat_output.ndjson"

[http_sender]
server_url = "http://10.0.2.15:8080"
```

### Environment variable overrides (`k8s/configmap.yaml`)

| Variable | Production value | Description |
|---|---|---|
| `KAFKA__BOOTSTRAP_SERVERS` | `prerequisites-kafka:9092` | Kafka broker |
| `KAFKA__GROUP_ID` | `datahub-dcat-consumer` | Consumer group |
| `KAFKA__AUTO_OFFSET_RESET` | `earliest` | Offset policy |
| `SCHEMA_REGISTRY__URL` | `http://datahub-datahub-gms:8080` | DataHub GMS |
| `SERVER__PORT` | `3001` | Health probe port |
| `HTTP_SENDER__SERVER_URL` | `http://10.0.2.15:8080` | DCAT3 REST server |

The `__` separator maps nested TOML keys (`KAFKA__BOOTSTRAP_SERVERS` → `[kafka] bootstrap_servers`). The Kafka topic list is a TOML array and cannot be overridden via environment variables.

---

## 8. Deployment

The script `runK8s.sh` automates the full deploy cycle:

```bash
# 1. Build the Docker image (two-stage: Rust builder + debian:bookworm-slim runtime)
docker build --no-cache -t datahub-kafka-consumer:latest .

# 2. Load the image into the Minikube profile "datahub"
minikube image load datahub-kafka-consumer:latest -p datahub

# 3. Tear down existing resources and redeploy
kubectl delete configmap/datahub-kafka-consumer-config -n datahub1 --ignore-not-found
kubectl delete secret/datahub-kafka-consumer-secret   -n datahub1 --ignore-not-found
kubectl delete deployment/datahub-kafka-consumer       -n datahub1 --ignore-not-found

kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/deployment.yaml
```

**Kubernetes resources:**

| Resource | Name | Purpose |
|---|---|---|
| `Deployment` | `datahub-kafka-consumer` | 1 replica, health probes on `:3001/health` |
| `ConfigMap` | `datahub-kafka-consumer-config` | Runtime environment variable overrides |
| `Secret` | `datahub-kafka-consumer-secret` | Placeholder for future Kafka auth credentials |

The NDJSON output file is written to an `emptyDir` volume mounted at `/app/output`. The volume is preserved across pod restarts within the same lifecycle; on a fresh pod creation the `load_from_ndjson()` warm-restart mechanism recovers state before resuming Kafka consumption.

---

## 9. Testing

### Step 1 — Deploy the consumer

```bash
./runK8s.sh
kubectl rollout status deployment/datahub-kafka-consumer -n datahub1
```

### Step 2 — Start an HTTP server on the host machine

The consumer sends HTTP requests to `10.0.2.15:8080` (host machine IP as seen from inside the Minikube VM). Start any HTTP server on port 8080 that logs incoming requests. A minimal option:

```bash
# Python — logs method, path, and body to stdout
python3 -c "
from http.server import BaseHTTPRequestHandler, HTTPServer
class H(BaseHTTPRequestHandler):
    def do_GET(self): self._respond()
    def do_POST(self): self._respond()
    def do_PUT(self): self._respond()
    def do_DELETE(self): self._respond()
    def _respond(self):
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length)
        print(self.command, self.path, body.decode() if body else '')
        self.send_response(201); self.end_headers()
HTTPServer(('0.0.0.0', 8080), H).serve_forever()
"
```

### Step 3 — Ingest data into DataHub

Use the DataHub UI or an ingestion recipe to register a dataset, container, or data flow. The ingestion triggers Kafka events that the consumer will pick up.

### Step 4 — Observe the consumer logs

```bash
kubectl logs -n datahub1 -l app=datahub-kafka-consumer -f
```

Expected log entries on a successful event:

```
INFO  State updated  event_kind=MCL  dcat_entity=Dataset  urn=urn:li:dataset:...  aspect_name=datasetProperties  change_type=UPSERT
```

HTTP errors appear as `WARN` and do not stop the consumer.

### Step 5 — Verify HTTP requests on the host

The Python server should print lines such as:

```
POST /catalogs/urn%3Ali%3Acontainer%3A... {"dcterms_identifier": "urn:li:container:...", "dcterms_title": "My Catalog"}
POST /catalogs/urn%3Ali%3AdataPlatform%3Aiceberg/datasets {"dcterms_identifier": "urn:li:dataset:...", "dcterms_title": "..."}
```

### Step 6 — Verify the NDJSON file inside the pod

```bash
kubectl exec -n datahub1 \
  $(kubectl get pod -n datahub1 -l app=datahub-kafka-consumer -o jsonpath='{.items[0].metadata.name}') \
  -- cat /app/output/dcat_output.ndjson
```

Each line must correspond to a known DataHub entity with its current metadata.
