# DataHub DCAT3 Data Space — TFM Setup Guide

> **Thesis context:** Master's Final Project (TFM) — Universitat Politècnica de Madrid (UPM)  
> This repository implements a local Data Space prototype. It deploys a complete metadata management platform (DataHub) on a Minikube Kubernetes cluster, ingests data from Iceberg tables (stored on MinIO via Nessie) and external OpenAPI sources, and exposes a DCAT3-compliant REST API through a thin Rust HTTP client that translates DataHub's internal model to the standard DCAT3 vocabulary.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Prerequisites](#prerequisites)
3. [Step 1 — Deploy the Kubernetes environment](#step-1--deploy-the-kubernetes-environment)
4. [Step 2 — Deploy MinIO and Nessie](#step-2--deploy-minio-and-nessie)
5. [Step 3 — Port-forward cluster services](#step-3--port-forward-cluster-services)
6. [Step 4 — Create the MinIO warehouse bucket](#step-4--create-the-minio-warehouse-bucket)
7. [Step 5 — Generate and store a DataHub access token](#step-5--generate-and-store-a-datahub-access-token)
8. [Step 6 — Bootstrap the Iceberg/Nessie catalog](#step-6--bootstrap-the-icebergnessie-catalog)
9. [Step 7 — Ingest metadata into DataHub](#step-7--ingest-metadata-into-datahub)
10. [Step 8 — Query DataHub via the DCAT3 Rust HTTP Client](#step-8--query-datahub-via-the-dcat3-rust-http-client)
11. [Repository Structure](#repository-structure)
12. [Credentials Reference](#credentials-reference)

---

## Architecture Overview

> _Architecture diagram to be inserted here._

The platform is composed of the following layers:

| Layer | Components | Purpose |
|---|---|---|
| **Storage** | MinIO (S3-compatible) | Stores raw Iceberg data files (Parquet + metadata) |
| **Catalog** | Nessie (REST catalog) | Provides versioned, Git-like table catalog for Iceberg |
| **Data Processing** | PySpark + Apache Iceberg | Writes structured tables to MinIO via Nessie |
| **Metadata Platform** | DataHub (GMS + Frontend) | Central metadata graph: datasets, lineage, ownership, tags |
| **Ingestion** | acryl-datahub CLI / K8s Jobs | Pushes metadata from Iceberg, AEMET, EU Open Data → DataHub |
| **DCAT3 API** | Rust HTTP Client (Axum) | Translates DataHub REST API v2 into DCAT3-compliant endpoints |

All components run inside a single Minikube cluster on the `datahub1` namespace.  
The Rust client runs locally (or as a container) and connects to the cluster via port-forwarding.

---

## Prerequisites

Ensure all of the following are installed on the host machine before starting.

### Required tools

| Tool | Version | Install |
|---|---|---|
| `minikube` | ≥ 1.32 | https://minikube.sigs.k8s.io/docs/start/ |
| `kubectl` | ≥ 1.28 | https://kubernetes.io/docs/tasks/tools/ |
| `helm` | ≥ 3.12 | https://helm.sh/docs/intro/install/ |
| `docker` | ≥ 24 | https://docs.docker.com/engine/install/ |
| `python3` | ≥ 3.10 | https://www.python.org/downloads/ |
| `rust` + `cargo` | ≥ 1.78 | https://rustup.rs |
| `curl` + `jq` | any | via package manager (`apt`, `brew`) |

### Minimum hardware

| Resource | Minimum | Recommended |
|---|---|---|
| CPU cores | 4 | 6 |
| RAM | 10 GB | 12 GB |
| Disk | 20 GB free | 40 GB |

> The Minikube cluster is configured with **6 CPUs and 12 GB RAM** by default in `deployEnv.sh`. Adjust `CPUS` and `MEMORY` at the top of that file if your machine has different resources.

---

## Step 1 — Deploy the Kubernetes environment

This step starts a fresh Minikube cluster (named `datahub`), creates the `datahub1` namespace, injects the required Kubernetes secrets, and deploys DataHub using the official Helm chart.

```bash
cd KUBERNETES/DATAHUB
chmod +x deployEnv.sh
./deployEnv.sh -n 1
```

**What `-n 1` does:** Creates a single DataHub instance in namespace `datahub1`.  
The script will take **10–30 minutes** on first run because it downloads all Helm chart images.

### What the script does internally

1. Checks that `minikube`, `kubectl`, and `helm` are in `PATH`
2. Destroys any existing Minikube environment (`minikube delete --all --purge`)
3. Starts a new Minikube profile called `datahub` with 6 CPUs / 12 GB RAM
4. Creates namespace `datahub1`
5. Creates Kubernetes secrets:
   - `mysql-secrets` — MySQL root/user password (`datahub`)
   - `neo4j-secrets` — Neo4j password (`datahub`)
   - `datahub-auth-secrets` — auto-generated signing keys for the DataHub token service
6. Creates a `datahub-operator-sa` ServiceAccount with `cluster-admin` privileges (required by the DataHub system-update Helm hook)
7. Runs `helm install prerequisites` (MySQL, Elasticsearch, Kafka, Neo4j, Zookeeper)
8. Runs `helm install datahub` using `helm/values.yaml` and `helm/prerequisites/values.yaml`

### Verify deployment

```bash
kubectl get pods -n datahub1
```

All pods should be `Running` or `Completed` before proceeding. The `datahub-datahub-gms` pod takes the longest to become ready (up to 5 minutes after all others are up).

---

## Step 2 — Deploy MinIO and Nessie

MinIO provides S3-compatible object storage for Iceberg data files. Nessie provides the REST catalog that Spark (Iceberg) uses to discover and version tables.

```bash
cd KUBERNETES/DATAHUB/ingestion/iceberg/k8sJobs
kubectl apply -f nessie-minio-stack.yaml
```

This manifest deploys, all into namespace `datahub1`:

| Resource | Type | Description |
|---|---|---|
| `nessie-config` | ConfigMap | Nessie properties: S3 endpoint, warehouse location (`s3://warehouse/`), MinIO credentials |
| `minio` | Deployment | MinIO object store, image `minio/minio:RELEASE.2024-01-28T22-35-53Z` |
| `minio-service` | Service | ClusterIP, ports 9000 (API) and 9001 (Console) |
| `nessie` | Deployment | Project Nessie v0.103.0, REST catalog backed by in-memory store |
| `nessie-service` | Service | ClusterIP, port 19120 |

### Verify

```bash
kubectl get pods -n datahub1 | grep -E "minio|nessie"
# Expected output:
# minio-<hash>   1/1   Running   ...
# nessie-<hash>  1/1   Running   ...
```

---

## Step 3 — Port-forward cluster services

The cluster services are only accessible inside the Kubernetes network. Port-forwarding exposes them on `localhost` for local development.

```bash
cd KUBERNETES/DATAHUB
chmod +x ports_forward.sh
./ports_forward.sh
```

This opens five background port-forwards:

| Service | Cluster port | Local port | URL |
|---|---|---|---|
| DataHub Frontend | 9002 | **9005** | http://localhost:9005 |
| DataHub GMS | 8080 | **8080** | http://localhost:8080 |
| Nessie REST | 19120 | **19120** | http://localhost:19120 |
| MinIO API | 9000 | **9000** | http://localhost:9000 |
| MinIO Console | 9001 | **9001** | http://localhost:9001 |

> The port-forwards run as background processes tied to the current terminal session. If you close the terminal or the forwards drop, re-run `ports_forward.sh`.

**Tip — check that all forwards are live:**
```bash
curl -s http://localhost:8080/health      # DataHub GMS → {"status":"ok"}
curl -s http://localhost:19120/api/v1/config  # Nessie
curl -s http://localhost:9000/minio/health/live  # MinIO API
```

---

## Step 4 — Create the MinIO warehouse bucket

Nessie is configured to store Iceberg table data at `s3://warehouse/`. This bucket must exist in MinIO before any Iceberg table can be written.

### Option A — MinIO Web Console (recommended for first setup)

1. Open http://localhost:9001 in your browser
2. Log in: **username** `admin` / **password** `password`
3. Click **Buckets → Create Bucket**
4. Enter bucket name: `warehouse`
5. Leave all other settings at defaults and click **Create**

### Option B — MinIO CLI (`mc`)

```bash
# Install mc if needed
curl -O https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc && sudo mv mc /usr/local/bin/

# Configure alias and create bucket
mc alias set local http://localhost:9000 admin password
mc mb local/warehouse
mc ls local   # should list: [date] warehouse
```

---

## Step 5 — Generate and store a DataHub access token

DataHub requires a Personal Access Token (PAT) for all API calls. Tokens are time-limited (configurable TTL). You must store the token in two places:

1. **A Kubernetes Secret** — used by ingestion K8s Jobs running inside the cluster
2. **`config.toml`** — used by the local Rust HTTP client

### 5.1 — Generate a token from the DataHub UI

1. Open http://localhost:9005
2. Log in: **username** `datahub` / **password** `datahub`
3. Click your avatar (top-right) → **Settings → Access Tokens**
4. Click **Generate new token**
5. Set a name (e.g., `local-dev`), set TTL (e.g., 90 days), click **Create**
6. Copy the full token — it is shown only once

### 5.2 — Store in Kubernetes Secret

Open `KUBERNETES/DATAHUB/ingestion/iceberg/k8sJobs/secret-datahub-token.yaml` and replace the token value:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: datahub-token
  namespace: datahub1
type: Opaque
stringData:
  token: "YOUR_TOKEN_HERE"   # <-- replace this
```

Apply it:
```bash
cd KUBERNETES/DATAHUB/ingestion/iceberg/k8sJobs
kubectl apply -f secret-datahub-token.yaml
```

Verify:
```bash
kubectl get secret datahub-token -n datahub1
```

### 5.3 — Store in config.toml (Rust client)

Open `KUBERNETES/DataAccess/OpenAPI/HTTPClient/config.toml`:

```toml
[datahub]
base_url = "http://localhost:8080"
token = "YOUR_TOKEN_HERE"   # <-- paste the same token here

[server]
port = 3000
```

### Automatic token rotation (CI/scripted use)

The script `KUBERNETES/DATAHUB/ingestion/iceberg/k8sJobs/ingest.sh` can automatically generate a fresh PAT from the system credentials and update the K8s secret before running the ingestion job:

```bash
cd KUBERNETES/DATAHUB/ingestion/iceberg/k8sJobs
./ingest.sh             # generates new token + runs job
./ingest.sh -s          # skip token rotation, reuse existing secret
./ingest.sh -d 7        # generate a token valid for 7 days
```

---

## Step 6 — Bootstrap the Iceberg/Nessie catalog

This step creates the Iceberg tables in the Nessie catalog using a PySpark ETL pipeline that writes Parquet data to MinIO.

### Option A — Local Python

**Install dependencies:**
```bash
pip install pyspark==3.5.1 \
            pyarrow \
            "pyiceberg[nessie,s3fs]>=0.8.0"
```

> PySpark also requires Java 11 or 17:
> ```bash
> sudo apt install openjdk-17-jre-headless
> export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
> ```

**Run the pipeline** (port-forwards must be active — Step 3):
```bash
cd KUBERNETES/DataSources/iceberg/hadoop
python3 deployIceberg.py
```

The script:
1. Initialises a Spark session connecting to Nessie at `http://localhost:19120/iceberg` and MinIO at `http://localhost:9000`
2. Generates a sample CSV (`data_input/transactions.csv`) with 5 transaction records
3. Reads the CSV with a strict schema (no `inferSchema`)
4. Applies a transformation: adds an `ingestion_ts` column, filters `amount > 0`
5. Writes the result as an Iceberg table `local_iceberg.default.sales_transactions` partitioned by `days(tx_ts)` in MinIO bucket `warehouse/`
6. Prints a snapshot history to verify the write succeeded

**Expected output:**
```
[10:12:05] Leyendo CSV con esquema estricto...
[10:12:07] Aplicando transformaciones...
Catalogs disponibles: [DatabaseSummary(name='default', ...)]
[10:12:09] Escribiendo en Iceberg: local_iceberg.default.sales_transactions
>>> Escritura exitosa: Tabla creada/reemplazada.
+-------+---------+------+-----------+-------------------+-------------------+
|  tx_id|client_id|amount|   category|              tx_ts|       ingestion_ts|
+-------+---------+------+-----------+-------------------+-------------------+
|  TX001|      101|450.50|Electronics|2023-10-21 10:00:00|2024-01-15 10:12:09|
...
```

### Option B — Docker (no local Java/Spark required)

Build and run the PySpark pipeline in a container that has all dependencies pre-installed:

```bash
# Build the image (from project root)
docker build -t iceberg-etl \
  -f KUBERNETES/DataSources/iceberg/hadoop/Dockerfile \
  KUBERNETES/DataSources/iceberg/hadoop/

# Run — the container connects to the host's port-forwarded services
docker run --rm \
  --network host \
  iceberg-etl \
  python3 deployIceberg.py
```

> `--network host` is required so that `localhost:19120` (Nessie) and `localhost:9000` (MinIO) resolve correctly inside the container.

---

## Step 7 — Ingest metadata into DataHub

Once data exists in Iceberg/Nessie, you push its metadata (schema, lineage, statistics) into DataHub using the `acryl-datahub` ingestion framework. Two methods are available.

### Method A — acryl-datahub CLI (local, manual)

This method runs ingestion from your local machine against the port-forwarded GMS (Step 3).

**Install the CLI:**
```bash
pip install 'acryl-datahub[iceberg]>=0.14.0' \
            'pyiceberg[nessie,pyarrow]>=0.8.0'
```

**Run an ingestion recipe:**
```bash
# Iceberg tables from Nessie/MinIO
python3 -m datahub ingest run \
  -c KUBERNETES/DATAHUB/ingestion/iceberg/receptaIceberg.yaml

# AEMET OpenData (weather datasets via OpenAPI)
python3 -m datahub ingest run \
  -c KUBERNETES/DATAHUB/ingestion/iceberg/AEMET.yaml
```

> Before running, open each recipe YAML and replace the `token:` field under `sink.config` with your current DataHub token (from Step 5).

**Available recipes:**

| File | Source | Description |
|---|---|---|
| `ingestion/iceberg/receptaIceberg.yaml` | Iceberg (Nessie) | Ingests Iceberg table schemas from the Nessie catalog |
| `ingestion/iceberg/AEMET.yaml` | OpenAPI (AEMET) | Ingests AEMET weather API spec from `opendata.aemet.es` |
| `ingestion/openapi/recipes/AEMET_FEDE.yaml` | DataHub → DataHub | Federation: copies metadata from one DataHub instance to another |
| `ingestion/openapi/recipes/EU_FEDE.yaml` | DataHub → DataHub | Federation from EU Open Data Portal |

### Method B — Kubernetes Job (automated, in-cluster)

This method runs ingestion entirely inside the cluster. The ingestion pod has direct access to Nessie and DataHub GMS via their internal ClusterIP services (no port-forwarding needed).

#### Build the ingestion Docker image

The ingestion job uses a custom Docker image that bundles `acryl-datahub` and `pyiceberg`.

```bash
# Point Docker to Minikube's internal registry
eval $(minikube docker-env -p datahub)

# Build the image (must be done inside Minikube's Docker context)
docker build \
  -t datahub-ingestion-iceberg:latest \
  KUBERNETES/DATAHUB/ingestion/iceberg/k8sJobs/
```

> The `Dockerfile` in `k8sJobs/` installs `acryl-datahub[iceberg]>=0.14.0` and `pyiceberg[nessie,pyarrow]>=0.8.0` on top of `python:3.10-slim`.

#### Update the recipe token

The recipe is stored in a ConfigMap (`iceberg_ingestion.yaml`). Open it and replace the `token:` value under `sink.config` with your current token:

```bash
# Edit the ConfigMap file
vi KUBERNETES/DATAHUB/ingestion/iceberg/k8sJobs/iceberg_ingestion.yaml
# Change: token: "YOUR_TOKEN_HERE"
```

#### Run the job

```bash
cd KUBERNETES/DATAHUB/ingestion/iceberg/k8sJobs

# Option 1 — Full automation script (handles token refresh + job lifecycle)
./ingest.sh

# Option 2 — Manual apply
kubectl apply -f secret-datahub-token.yaml
kubectl apply -f iceberg_ingestion.yaml      # ConfigMap with recipe
kubectl apply -f iceberg_ingestion_job.yaml  # Batch Job

# Watch logs
kubectl logs job/iceberg-ingestion-job -n datahub1 --follow
```

**Re-running the job** (K8s Jobs are immutable — delete and re-apply):
```bash
kubectl delete job/iceberg-ingestion-job -n datahub1 --ignore-not-found
kubectl apply -f iceberg_ingestion_job.yaml
```

Or use the convenience script:
```bash
./recastJob.sh   # deletes old job + secret, re-applies all three manifests, tails logs
```

#### Verify ingestion in DataHub

Open the DataHub UI at http://localhost:9005 → **Datasets**. You should see the Iceberg table `sales_transactions` (platform: `iceberg`, platform instance: `default`).

---

## Step 8 — Query DataHub via the DCAT3 Rust HTTP Client

The Rust HTTP client is a standalone binary that:
- **Listens** on `localhost:3000` as an Axum HTTP server exposing the DCAT3 REST API (defined in `REST_API.yml`)
- **Translates** every DCAT3 request into one or more DataHub REST API v2 calls against `localhost:8080` (GMS)
- **Maps** DataHub entity types to DCAT3 concepts (container → Catalog, dataset → Dataset, tag → Keyword, domain → Theme, dataFlow → DataService)

### 8.1 — Install Rust

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
rustc --version   # should print: rustc 1.78.x or later
```

### 8.2 — Configure the client

Edit `KUBERNETES/DataAccess/OpenAPI/HTTPClient/config.toml`:

```toml
[datahub]
base_url = "http://localhost:8080"   # GMS via port-forward (Step 3)
token    = "YOUR_DATAHUB_TOKEN"      # token from Step 5

[server]
port = 3000   # port this service listens on locally
```

You can also configure via environment variables (useful for scripting):
```bash
export DATAHUB__BASE_URL="http://localhost:8080"
export DATAHUB__TOKEN="eyJhbGciOiJIUzI1NiJ9..."
```

### 8.3 — Build and run

```bash
cd KUBERNETES/DataAccess/OpenAPI/HTTPClient

# First build (downloads and compiles ~200 dependencies — takes 2–5 minutes)
cargo build --release

# Run the server
cargo run --release
```

You should see:
```
INFO datahub_dcat_client: Connecting to DataHub at http://localhost:8080
INFO datahub_dcat_client: DCAT API server listening on 0.0.0.0:3000
```

The server is now ready. All DCAT3 endpoints defined in `REST_API.yml` are live at `http://localhost:3000`.

### 8.4 — DCAT3 API endpoint reference

The following table shows every available endpoint, the DCAT3 entity it returns, and the DataHub entity/operation it calls internally.

#### Catalogs

```bash
# List all catalogs (DataHub containers)
curl -s http://localhost:3000/catalogs | jq .

# Get a specific catalog by ID
curl -s http://localhost:3000/catalogs/{catalog_id} | jq .

# Create a catalog
curl -s -X POST http://localhost:3000/catalogs \
  -H "Content-Type: application/json" \
  -d '{"dcterms_title": "My Catalog", "dcterms_description": "Test catalog"}' | jq .

# Update a catalog
curl -s -X PUT http://localhost:3000/catalogs/{catalog_id} \
  -H "Content-Type: application/json" \
  -d '{"dcterms_title": "Updated Title"}' | jq .

# Delete a catalog
curl -s -X DELETE http://localhost:3000/catalogs/{catalog_id}

# List datasets belonging to a catalog
curl -s http://localhost:3000/catalogs/{catalog_id}/datasets | jq .

# List catalog records of a catalog
curl -s http://localhost:3000/catalogs/{catalog_id}/catalog_records | jq .
```

#### Datasets

```bash
# List all datasets (supports filters via query params)
curl -s "http://localhost:3000/datasets" | jq .
curl -s "http://localhost:3000/datasets?theme=environment" | jq .
curl -s "http://localhost:3000/datasets?publisher=AEMET&title=temperature" | jq .

# Get a specific dataset by ID (plain name or full DataHub URN)
curl -s "http://localhost:3000/datasets/sales_transactions" | jq .
curl -s "http://localhost:3000/datasets/urn:li:dataset:(urn:li:dataPlatform:iceberg,default.sales_transactions,PROD)" | jq .

# Create a dataset
curl -s -X POST http://localhost:3000/datasets \
  -H "Content-Type: application/json" \
  -d '{
    "dcterms_identifier": "my-dataset",
    "dcterms_title": "My Dataset",
    "dcterms_description": "A test dataset",
    "dcat_theme": "environment",
    "dcterms_publisher": "MyOrg"
  }' | jq .

# Update a dataset
curl -s -X PUT http://localhost:3000/datasets/{dataset_id} \
  -H "Content-Type: application/json" \
  -d '{"dcterms_title": "Updated Name"}' | jq .

# Delete a dataset
curl -s -X DELETE http://localhost:3000/datasets/{dataset_id}

# List distributions of a dataset
curl -s "http://localhost:3000/datasets/{dataset_id}/distributions" | jq .

# List data services associated with a dataset
curl -s "http://localhost:3000/datasets/{dataset_id}/dataservices" | jq .
```

#### Distributions

```bash
# List all distributions (stored as datasets with dcat_distribution=true)
curl -s "http://localhost:3000/distributions" | jq .

# Get a specific distribution
curl -s "http://localhost:3000/distributions/{distribution_id}" | jq .

# Create a distribution
curl -s -X POST http://localhost:3000/distributions \
  -H "Content-Type: application/json" \
  -d '{
    "dcterms_identifier": "dist-001",
    "dcat_access_url": "https://example.org/data.parquet",
    "dcterms_format": "Parquet",
    "dcat_media_type": "application/octet-stream"
  }' | jq .
```

#### Data Services

```bash
# List all data services (DataHub dataFlow entities)
curl -s "http://localhost:3000/dataservices" | jq .

# Get a specific data service
curl -s "http://localhost:3000/dataservices/{service_id}" | jq .

# List datasets served by a data service
curl -s "http://localhost:3000/dataservices/{service_id}/datasets" | jq .

# Create a data service
curl -s -X POST http://localhost:3000/dataservices \
  -H "Content-Type: application/json" \
  -d '{
    "dcterms_identifier": "aemet-openapi",
    "dcterms_title": "AEMET OpenAPI Service",
    "dcat_endpoint_url": "https://opendata.aemet.es/openapi"
  }' | jq .
```

#### Keywords, Themes, and Controlled Vocabularies

```bash
# Keywords (DataHub tags)
curl -s "http://localhost:3000/keywords" | jq .
curl -s -X POST http://localhost:3000/keywords \
  -H "Content-Type: application/json" \
  -d '{"name": "climate", "description": "Climate-related datasets"}' | jq .
curl -s -X DELETE "http://localhost:3000/keywords/climate"

# Themes (DataHub domains)
curl -s "http://localhost:3000/themes" | jq .
curl -s -X POST http://localhost:3000/themes \
  -H "Content-Type: application/json" \
  -d '{"name": "Environment", "description": "Environmental data"}' | jq .

# References
curl -s "http://localhost:3000/references" | jq .
curl -s "http://localhost:3000/references/{reference_id}" | jq .
```

#### Relations and Resources

```bash
# List relations for a specific resource (traverses DataHub relationships)
curl -s "http://localhost:3000/resources/{resource_id}/relations" | jq .

# Qualified relations for a resource
curl -s "http://localhost:3000/resources/{resource_id}/qualified_relations" | jq .

# Generic resource listing (searches across datasets, containers, dataFlows)
curl -s "http://localhost:3000/resources" | jq .
curl -s "http://localhost:3000/resources/{resource_id}" | jq .

# Dataset series (DataHub containers of type DatasetSeries)
curl -s "http://localhost:3000/datasetseries" | jq .
curl -s "http://localhost:3000/datasetseries/{series_id}/datasets" | jq .
```

### 8.5 — Run as a Docker container

```bash
cd KUBERNETES/DataAccess/OpenAPI/HTTPClient

docker build -t datahub-dcat-client .

docker run --rm \
  --network host \
  -e DATAHUB__BASE_URL="http://localhost:8080" \
  -e DATAHUB__TOKEN="YOUR_TOKEN" \
  datahub-dcat-client
```

> `--network host` allows the container to reach the port-forwarded GMS at `localhost:8080`.

### 8.6 — Troubleshooting the Rust client

| Symptom | Cause | Fix |
|---|---|---|
| `Connection refused` on startup | GMS port-forward not active | Re-run `ports_forward.sh` |
| `401 Unauthorized` from DataHub | Token expired or wrong | Generate a new token (Step 5) and update `config.toml` |
| Empty `[]` response on `/datasets` | No data ingested yet | Complete Steps 6–7 first |
| `cargo build` fails with OpenSSL error | Missing system libs | `sudo apt install pkg-config libssl-dev` |
| Port 3000 already in use | Another process | Change `server.port` in `config.toml` |

---

## Repository Structure

```
.
├── REST_API.yml                          # DCAT3 OpenAPI 3.0 specification
├── README.md                             # This file
└── KUBERNETES/
    ├── DATAHUB/
    │   ├── deployEnv.sh                  # Step 1: Deploy Minikube + DataHub via Helm
    │   ├── ports_forward.sh              # Step 3: Port-forward all cluster services
    │   ├── helm/
    │   │   ├── values.yaml               # DataHub Helm chart values
    │   │   └── prerequisites/values.yaml # Prerequisites (MySQL, Kafka, ES...) values
    │   └── ingestion/
    │       ├── iceberg/
    │       │   ├── receptaIceberg.yaml   # CLI ingestion recipe: Iceberg → DataHub
    │       │   ├── AEMET.yaml            # CLI ingestion recipe: AEMET OpenAPI → DataHub
    │       │   └── k8sJobs/
    │       │       ├── nessie-minio-stack.yaml     # Step 2: Deploy MinIO + Nessie
    │       │       ├── dockerfile                  # Ingestion container image
    │       │       ├── iceberg_ingestion.yaml      # ConfigMap with in-cluster recipe
    │       │       ├── iceberg_ingestion_job.yaml  # Kubernetes Batch Job
    │       │       ├── secret-datahub-token.yaml   # Step 5: K8s Secret for the PAT
    │       │       ├── ingest.sh                   # Full automation: token + job
    │       │       └── recastJob.sh                # Re-run helper script
    │       └── openapi/
    │           └── recipes/
    │               ├── AEMET_FEDE.yaml   # Federation recipe: DataHub → DataHub
    │               └── EU_FEDE.yaml      # Federation recipe: EU Open Data Portal
    ├── DataSources/
    │   └── iceberg/hadoop/
    │       ├── deployIceberg.py          # Step 6: PySpark ETL → Nessie/MinIO
    │       └── data_input/
    │           └── transactions.csv      # Sample input data
    └── DataAccess/
        ├── GraphQL/                      # GraphQL query examples (listPlatforms, etc.)
        └── OpenAPI/
            ├── curl.sh                   # Raw DataHub API v2 curl examples
            └── HTTPClient/               # Step 8: DCAT3 Rust HTTP client
                ├── Cargo.toml
                ├── config.toml           # DataHub URL + token + server port
                ├── Dockerfile
                └── src/
                    ├── main.rs           # Entry point
                    ├── config.rs         # Config loading
                    ├── error.rs          # Error types
                    ├── models/           # DCAT3 domain structs
                    ├── repository/       # Repository trait interfaces
                    ├── datahub/          # DataHub REST API v2 implementations
                    └── api/              # Axum HTTP route handlers
```

---

## Credentials Reference

| Service | URL | Username | Password |
|---|---|---|---|
| DataHub UI | http://localhost:9005 | `datahub` | `datahub` |
| DataHub GMS API | http://localhost:8080 | — | Bearer token |
| MinIO Console | http://localhost:9001 | `admin` | `password` |
| MinIO API | http://localhost:9000 | `admin` | `password` |
| Nessie REST | http://localhost:19120 | — | no auth |
| DCAT3 API | http://localhost:3000 | — | no auth |
| MySQL (internal) | cluster: 3306 | `datahub` | `datahub` |
| Neo4j (internal) | cluster: 7687 | `neo4j` | `datahub` |

> **Security note:** All credentials above are development defaults suitable only for local/lab environments. Do not use in production.

---

## Quick Reference — Full Setup Sequence

```bash
# 1. Deploy cluster and DataHub
cd KUBERNETES/DATAHUB && ./deployEnv.sh -n 1

# 2. Deploy MinIO + Nessie
kubectl apply -f ingestion/iceberg/k8sJobs/nessie-minio-stack.yaml

# 3. Port-forward services (keep this terminal open)
./ports_forward.sh

# 4. Create MinIO bucket (in a new terminal)
mc alias set local http://localhost:9000 admin password && mc mb local/warehouse

# 5. Generate token in DataHub UI → paste into:
#    - ingestion/iceberg/k8sJobs/secret-datahub-token.yaml
#    - DataAccess/OpenAPI/HTTPClient/config.toml
kubectl apply -f ingestion/iceberg/k8sJobs/secret-datahub-token.yaml

# 6. Bootstrap Iceberg tables
cd ../DataSources/iceberg/hadoop && python3 deployIceberg.py

# 7. Ingest metadata into DataHub
python3 -m datahub ingest run -c KUBERNETES/DATAHUB/ingestion/iceberg/receptaIceberg.yaml
# OR via K8s job:
cd KUBERNETES/DATAHUB/ingestion/iceberg/k8sJobs && ./ingest.sh

# 8. Start the DCAT3 API
cd KUBERNETES/DataAccess/OpenAPI/HTTPClient && cargo run --release
# → API live at http://localhost:3000
curl -s http://localhost:3000/datasets | jq .
```
