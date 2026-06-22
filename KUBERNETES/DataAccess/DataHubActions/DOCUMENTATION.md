# DataHubActions — DCAT3 Event Forwarder

## 1. Overview and Purpose

The `DataHubActions` component is a Python extension for the [DataHub Actions framework](https://github.com/acryldata/datahub-actions) that translates DataHub's internal business-level change events into live HTTP calls against a DCAT3 REST API. Its role is to keep an external DCAT3 server synchronized with the state of the DataHub catalog by actively pushing every relevant change the moment it occurs.

Where the `KafkaQueue` component takes a passive approach — maintaining a local NDJSON snapshot that external systems can read — this component takes an **active push** approach: each event from DataHub immediately triggers one or more REST API calls to an external DCAT server, propagating the change in near real time.

The component packages two custom DataHub Actions plugins:

| Plugin name | Class | Purpose |
|---|---|---|
| `event_logger` | `EventLoggerAction` | Debug sink — appends all raw events to an NDJSON file |
| `dcat_forwarder` | `DcatForwardAction` | Production action — translates events into DCAT3 REST API calls |

---

## 2. Position in the Overall Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    DataHub Cluster                       │
│                                                         │
│  DataHub GMS ──► Kafka broker (prerequisites-kafka)     │
│                        │                                │
│                        │  EntityChangeEvent_v1          │
│                        │  (high-level business events)  │
└────────────────────────┼────────────────────────────────┘
                         │
                ┌────────▼────────┐
                │  DataHub Actions │
                │  framework       │
                │  (acryl image)   │
                │                  │
                │  ┌────────────┐  │
                │  │event_logger│  │──► /data/eventLogs.mdjson  (debug)
                │  └────────────┘  │
                │  ┌─────────────┐ │
                │  │dcat_forwarder│ │──► HTTP POST/PUT/DELETE
                │  └─────────────┘ │         │
                └──────────────────┘         ▼
                                     External DCAT3 REST API
                                     (http://10.0.2.15:8080)
```

The DataHub Actions framework runs as a sidecar-style service. It connects to the same Kafka broker as DataHub (`prerequisites-kafka:9092`) and consumes the `EntityChangeEvent_v1` topic — a high-level event stream that DataHub generates on top of raw metadata change events, expressing business-level operations (entity created, owner added, schema field added, entity removed).

---

## 3. The DataHub Actions Framework

DataHub Actions is Acryl Data's official event-reaction framework for DataHub. It is built on top of the same Kafka infrastructure as DataHub itself, but operates at a higher abstraction level than raw metadata change logs.

### Event type consumed: `EntityChangeEvent_v1`

Unlike the raw `MetadataChangeLog` events consumed by the KafkaQueue component, `EntityChangeEvent_v1` events express semantic changes:

| Field | Description |
|---|---|
| `entityType` | The type of the changed entity (`dataset`, `container`, etc.) |
| `entityUrn` | The DataHub URN identifying the entity |
| `category` | The category of change (`LIFECYCLE`, `OWNER`, `TECHNICAL_SCHEMA`, etc.) |
| `operation` | The specific operation (`CREATE`, `REMOVE`, `ADD`, `MODIFY`, `REINSTATE`) |
| `modifier` | Additional context (e.g., the schema field path for `TECHNICAL_SCHEMA` events) |
| `auditStamp` | Timestamp (`time` in epoch-ms) and `actor` URN of who triggered the change |

### Plugin mechanism

Custom actions are registered as Python entry points under the `datahub_actions.action.plugins` group. The framework discovers them by name and instantiates the class via the static `create(config_dict, ctx)` factory method. Each plugin implements three methods:

```python
def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action": ...  # factory
def act(self, event: EventEnvelope) -> None: ...                           # called per event
def close(self) -> None: ...                                               # cleanup on shutdown
```

---

## 4. Plugin: `EventLoggerAction`

**File:** `src/event_logger/action.py`

A minimal debugging sink. Every incoming event is serialized to JSON and appended as one line to a configurable NDJSON file:

```json
{"event_type": "EntityChangeEvent_v1", "event": { ... raw event object ... }}
```

This plugin is useful for inspecting the raw event stream during development and debugging, without any filtering or transformation. It was used to understand the structure of `EntityChangeEvent_v1` payloads before implementing the `dcat_forwarder`.

**Configuration:**

```yaml
action:
  type: "event_logger"
  config:
    output_path: "/data/eventLogs.mdjson"
```

---

## 5. Plugin: `DcatForwardAction`

**File:** `src/dcat_forwarder/action.py`

The production plugin. It filters the event stream to `EntityChangeEvent_v1` events only, dispatches by entity type and category/operation, and issues corresponding HTTP calls to the external DCAT3 REST API.

### 5.1 Event Mapping

The full dispatch table, from DataHub event to DCAT3 HTTP call:

| Entity type | Category | Operation | HTTP call |
|---|---|---|---|
| `container` | `LIFECYCLE` | `CREATE` / `REINSTATE` | `POST /catalogs` (on 409: `GET` + `PUT`) |
| `container` | `LIFECYCLE` | `REMOVE` | `DELETE /catalogs/{encoded_urn}` |
| `dataset` | `LIFECYCLE` | `CREATE` / `REINSTATE` | `POST /catalogs/{platform}/datasets` (on 404: `POST /datasets`; on 409: `GET` + `PUT`) |
| `dataset` | `LIFECYCLE` | `REMOVE` | `DELETE /datasets/{encoded_urn}` |
| `dataset` | `OWNER` | `ADD` / `MODIFY` | `GET /datasets/{id}` + `PUT` (sets `dcterms_publisher`) |
| `dataset` | `TECHNICAL_SCHEMA` | `ADD` | `GET /datasets/{id}` + `PUT` (appends field name to `dcterms_description`) |

All other entity types and category/operation combinations are silently ignored.

### 5.2 URN Encoding

DataHub URNs contain characters that are not safe in URL path segments (colons, commas, parentheses). The `_encode_id()` helper percent-encodes the full URN before embedding it in API paths:

```
urn:li:dataset:(urn:li:dataPlatform:iceberg,sales_transactions,PROD)
→ urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Aiceberg%2Csales_transactions%2CPROD%29
```

### 5.3 Platform Extraction

For datasets, the data platform identifier (e.g., `iceberg`, `hive`, `postgres`) is extracted from the URN using a regex against the canonical URN format:

```
urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})
```

The extracted platform name is used to associate the dataset with the correct DCAT Catalog when creating it:

```
POST /catalogs/urn%3Ali%3AdataPlatform%3Aiceberg/datasets
```

If the catalog does not yet exist (404), the dataset is created at the top-level `/datasets` endpoint instead.

### 5.4 Upsert / Conflict Handling

The action handles HTTP 409 Conflict responses with a read-then-update pattern:

1. `POST` to create the resource.
2. If `409` is returned (resource already exists): `GET` the current resource body, merge in the new fields, then `PUT` the updated body.

This makes the action idempotent with respect to duplicate `CREATE` events, which DataHub may emit on restarts or reprocessing.

### 5.5 Schema Field Handling

When a `TECHNICAL_SCHEMA / ADD` event arrives, the field name is extracted from the `modifier` field. DataHub's schema field modifier follows the format:

```
.[type=string].fieldName)
```

The regex `_SCHEMA_FIELD_RE` extracts the last word before the closing `)` as the plain field name. This field name is then appended to the dataset's `dcterms_description` field (with a `; ` separator), as a lightweight way to surface schema information in the DCAT representation without a dedicated schema field in the API.

### 5.6 DCAT3 Payload Structure

**Container → Catalog:**
```json
{
  "dcterms_identifier": "urn:li:container:18ec2dadc6c4ea2119fa86dad53f44a7",
  "dcterms_title": "Container ...f44a7",
  "dcterms_issued": "2024-11-15",
  "dcterms_creator": "urn:li:corpuser:datahub"
}
```

**Dataset → Dataset:**
```json
{
  "dcterms_identifier": "urn:li:dataset:(urn:li:dataPlatform:iceberg,sales_transactions,PROD)",
  "dcterms_title": "urn:li:dataset:(urn:li:dataPlatform:iceberg,sales_transactions,PROD)",
  "dcterms_issued": "2024-11-15"
}
```

Note: at creation time, the title is set to the URN (a placeholder). Richer metadata (display name, description) was handled in the KafkaQueue component which reads `*Properties` aspects directly.

---

## 6. Module Structure

```
DataHubActions/
├── src/
│   ├── event_logger/
│   │   ├── __init__.py
│   │   └── action.py          ← EventLoggerAction (debug sink)
│   └── dcat_forwarder/
│       ├── __init__.py
│       └── action.py          ← DcatForwardAction (production forwarder)
├── setup.py                   ← entry point registration for both plugins
├── Dockerfile                 ← extends acryldata/datahub-actions image
├── action.yml                 ← pipeline config for event_logger
├── configmap.yml              ← Kubernetes ConfigMap (both pipeline configs)
└── k8s/
    └── pvc.yaml               ← PersistentVolumeClaim for event log storage
```

---

## 7. Data Flow

```
DataHub Kafka (prerequisites-kafka:9092)
    │
    │  EntityChangeEvent_v1 topic
    │  (Avro, decoded by DataHub Actions framework)
    ▼
DataHub Actions framework
    │  Filters: event.event_type == "EntityChangeEvent_v1"
    ▼
DcatForwardAction.act(event)
    │
    │  _dispatch(raw_event_dict)
    │      entityType == "dataset"  ──► _handle_dataset()
    │      entityType == "container" ─► _handle_container()
    │
    ├── LIFECYCLE CREATE/REINSTATE ──► POST /catalogs/{platform}/datasets
    │                                      └─ 404 ──► POST /datasets
    │                                      └─ 409 ──► GET + PUT
    │
    ├── LIFECYCLE REMOVE ────────────► DELETE /datasets/{id}
    │
    ├── OWNER ADD/MODIFY ────────────► GET /datasets/{id}
    │                                  PUT /datasets/{id}  (dcterms_publisher updated)
    │
    └── TECHNICAL_SCHEMA ADD ────────► GET /datasets/{id}
                                       PUT /datasets/{id}  (field name appended to description)
```

---

## 8. Configuration

### Pipeline configuration files

Each plugin runs as a separate DataHub Actions pipeline, configured in YAML.

**`action.yml`** — EventLogger pipeline:
```yaml
name: "DataHub Event Logger Action"
source:
  type: "kafka"
  config:
    connection:
      bootstrap: "prerequisites-kafka:9092"
      schema_registry_url: "http://datahub-datahub-gms:8080/schema-registry/api/"
action:
  type: "event_logger"
  config:
    output_path: "/data/eventLogs.mdjson"
```

**`dcat_forwarder.yml`** (in `configmap.yml`) — DCAT Forwarder pipeline:
```yaml
name: "DCAT Forwarder Action"
source:
  type: "kafka"
  config:
    connection:
      bootstrap: "prerequisites-kafka:9092"
      schema_registry_url: "http://datahub-datahub-gms:8080/schema-registry/api/"
action:
  type: "dcat_forwarder"
  config:
    server_url: "http://10.0.2.15:8080"
```

The `server_url` points to `10.0.2.15` — the host machine's IP address as reachable from inside the Minikube/VirtualBox VM. This replaces `localhost`, which inside the pod would refer to the pod itself rather than the host. The comment in the source code records this rationale explicitly.

---

## 9. Deployment

### Docker image

The Dockerfile extends the official DataHub Actions image (`acryldata/datahub-actions:698bd78-slim`) and installs the custom plugin package into the existing virtual environment at `/home/datahub/.venv`:

```dockerfile
FROM acryldata/datahub-actions:698bd78-slim
USER root
COPY setup.py /app/event-logger/setup.py
COPY src/     /app/event-logger/src/
RUN chown -R datahub:datahub /app/event-logger
WORKDIR /app/event-logger
RUN /home/datahub/.venv/bin/pip3 install . && mkdir -p /etc/datahub/actions/conf
USER datahub
```

The `pip install .` command triggers entry point registration, making `event_logger` and `dcat_forwarder` discoverable by the DataHub Actions framework by name.

### Kubernetes resources

| Resource | Name | Purpose |
|---|---|---|
| `ConfigMap` | `datahub-actions-config` | Mounts pipeline YAML configs (`action.yml`, `dcat_forwarder.yml`) |
| `PersistentVolumeClaim` | `datahub-actions-event-logs` | 1 Gi volume for event log NDJSON (used by `event_logger`) |

The ConfigMap is mounted into the container at the path where the DataHub Actions framework expects to find pipeline configuration files (`/etc/datahub/actions/conf/`). Each `.yml` file in that directory is treated as one pipeline and runs as an independent consumer.

---

## 10. Design Decisions

### Push model vs. pull model

This component takes an active push approach: each DataHub event immediately triggers one or more HTTP calls to the DCAT server. This contrasts with the KafkaQueue component, which writes a local NDJSON file that external systems must poll. The push model achieves lower latency between a DataHub change and its reflection in the DCAT catalog, at the cost of tight coupling to the DCAT server's availability — if the server is down, events are processed but HTTP calls fail (and are not retried beyond the current delivery).

### High-level vs. low-level events

The DataHub Actions framework provides `EntityChangeEvent_v1`, which is a semantic business event ("this dataset was created", "this owner was added"). This is simpler to work with than the raw `MetadataChangeLog` events consumed by KafkaQueue, which carry aspect-level diffs that must be assembled into a coherent entity state. The trade-off is that `EntityChangeEvent_v1` carries less structured metadata than the raw aspects — for example, dataset properties (name, description) are not available at `CREATE` event time, so the initial title is set to the URN as a placeholder.

### Stateless design

Unlike KafkaQueue, this component maintains no in-memory state between events. Each event is handled independently. This simplifies the implementation and makes the component trivially restartable, but it means that richer entity state (e.g., combining properties, ownership, and schema into one resource) requires multiple round-trips to the DCAT server (GET then PUT) rather than a single authoritative write.

### Idempotent upsert pattern

The POST → 409 → GET+PUT pattern ensures the action can be replayed safely. DataHub may redeliver events after a pod restart (depending on the consumer group offset state), and the upsert pattern prevents duplicate creation errors from propagating as failures. Similarly, the 404 fallback for nested dataset creation (`/catalogs/{platform}/datasets` → `/datasets`) handles the case where the parent catalog has not yet been created.
