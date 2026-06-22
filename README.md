# DataHub — Gestión del Ciclo de Vida de Metadatos y Federación de Catálogos

> **Proyecto Final de Máster (TFM) — Universidad Politécnica de Madrid (UPM)**  

---

## Índice

### Parte I — Gestión del Ciclo de Vida de Metadatos (Kubernetes)
1. [Arquitectura](#arquitectura)
2. [Prerequisitos](#prerequisitos)
3. [Paso 1 — Desplegar el entorno Kubernetes](#paso-1--desplegar-el-entorno-kubernetes)
4. [Paso 2 — Desplegar MinIO y Nessie](#paso-2--desplegar-minio-y-nessie)
5. [Paso 3 — Port-forward de servicios del clúster](#paso-3--port-forward-de-servicios-del-clúster)
6. [Paso 4 — Crear el bucket warehouse en MinIO](#paso-4--crear-el-bucket-warehouse-en-minio)
7. [Paso 5 — Generar y almacenar el token de acceso DataHub](#paso-5--generar-y-almacenar-el-token-de-acceso-datahub)
8. [Paso 6 — Inicializar el catálogo Iceberg/Nessie](#paso-6--inicializar-el-catálogo-icebergnessie)
9. [Paso 7 — Ingestar metadatos en DataHub](#paso-7--ingestar-metadatos-en-datahub)
10. [Paso 8 — Capa de acceso DCAT3](#paso-8--capa-de-acceso-dcat3)
11. [Estructura de archivos — Kubernetes](#estructura-de-archivos--kubernetes)
12. [Credenciales de referencia](#credenciales-de-referencia)

### Parte II — Federación de Catálogos (Docker)
13. [Arquitectura de federación](#arquitectura-de-federación)
14. [Prerequisitos de federación](#prerequisitos-de-federación)
15. [Paso 1 — Configurar variables de entorno](#paso-1--configurar-variables-de-entorno)
16. [Paso 2 — Levantar las instancias DataHub](#paso-2--levantar-las-instancias-datahub)
17. [Paso 3 — Ingestión inicial de fuentes de datos](#paso-3--ingestión-inicial-de-fuentes-de-datos)
18. [Paso 4 — Ingestión periódica con Apache Airflow](#paso-4--ingestión-periódica-con-apache-airflow)
19. [Paso 5 — Federación automática mediante DataHub Actions](#paso-5--federación-automática-mediante-datahub-actions)
20. [Paso 6 — Federación manual (alternativa)](#paso-6--federación-manual-alternativa)
21. [Estructura de archivos — Docker](#estructura-de-archivos--docker)

### Referencia rápida
22. [Quick Start — Kubernetes](#quick-start--kubernetes)
23. [Quick Start — Docker](#quick-start--docker)

---

## Parte I — Gestión del Ciclo de Vida de Metadatos (Kubernetes)

### Arquitectura

La plataforma se despliega sobre un clúster Minikube (perfil `datahub`, namespace `datahub1`) y está compuesta por las siguientes capas:

| Capa | Componentes | Propósito |
|---|---|---|
| **Almacenamiento** | MinIO (S3-compatible) | Almacena ficheros Iceberg en formato Parquet |
| **Catálogo** | Nessie (REST catalog) | Catálogo de tablas Iceberg con versionado Git-like |
| **Procesado** | PySpark + Apache Iceberg | Escribe tablas estructuradas en MinIO vía Nessie |
| **Plataforma de metadatos** | DataHub (GMS + Frontend) | Grafo central de metadatos: datasets, linaje, propietarios, etiquetas |
| **Ingestión** | acryl-datahub CLI / K8s Jobs | Carga metadatos desde Iceberg, AEMET y EU Open Data hacia DataHub |
| **Acceso DCAT3** | HTTP Client (Rust/Axum) | API REST DCAT3 que traduce DataHub REST API v2 al vocabulario estándar |
| **Sincronización DCAT3** | KafkaQueue (Rust) | Consume eventos Kafka de DataHub y mantiene un snapshot NDJSON; reenvía cambios vía HTTP |
| **Sincronización DCAT3** | DataHub Actions (Python) | Escucha eventos de ciclo de vida de DataHub y ejecuta llamadas HTTP en tiempo real |

Los tres componentes de la capa de acceso/sincronización DCAT3 son complementarios y se documentan en el [Paso 8](#paso-8--capa-de-acceso-dcat3).

---

### Prerequisitos

Todas las herramientas siguientes deben estar instaladas en la máquina anfitriona antes de comenzar.

#### Herramientas requeridas

| Herramienta | Versión mínima | Instalación |
|---|---|---|
| `minikube` | ≥ 1.32 | https://minikube.sigs.k8s.io/docs/start/ |
| `kubectl` | ≥ 1.28 | https://kubernetes.io/docs/tasks/tools/ |
| `helm` | ≥ 3.12 | https://helm.sh/docs/intro/install/ |
| `docker` | ≥ 24 | https://docs.docker.com/engine/install/ |
| `python3` | ≥ 3.10 | https://www.python.org/downloads/ |
| `rust` + `cargo` | ≥ 1.78 | https://rustup.rs |
| `curl` + `jq` | cualquiera | gestor de paquetes del SO |

#### Hardware mínimo

| Recurso | Mínimo | Recomendado |
|---|---|---|
| Núcleos CPU | 4 | 6 |
| RAM | 10 GB | 12 GB |
| Disco libre | 20 GB | 40 GB |

> El clúster Minikube se configura con **6 CPUs y 12 GB de RAM** por defecto en `deployEnv.sh`. Ajusta las variables `CPUS` y `MEMORY` al inicio de ese script si la máquina tiene recursos distintos.

---

### Paso 1 — Desplegar el entorno Kubernetes

Este paso arranca un clúster Minikube limpio (perfil `datahub`), crea el namespace `datahub1`, inyecta los Secrets de Kubernetes necesarios y despliega DataHub mediante el chart Helm oficial.

```bash
cd KUBERNETES/DATAHUB
chmod +x deployEnv.sh
./deployEnv.sh -n 1
```

El flag `-n 1` crea una única instancia en `datahub1`. La primera ejecución tarda **10–30 minutos** porque descarga todas las imágenes del chart Helm.

**Lo que hace el script internamente:**

1. Comprueba que `minikube`, `kubectl` y `helm` están en el `PATH`
2. Destruye cualquier entorno Minikube existente (`minikube delete --all --purge`)
3. Arranca un nuevo perfil `datahub` con 6 CPUs / 12 GB RAM
4. Crea el namespace `datahub1`
5. Crea los Secrets de Kubernetes: `mysql-secrets`, `neo4j-secrets`, `datahub-auth-secrets` (claves generadas con `openssl rand`)
6. Crea la `ServiceAccount` `datahub-operator-sa` con privilegios `cluster-admin` (requerida por el hook `datahub-system-update` de Helm)
7. Ejecuta `helm install prerequisites` (MySQL, Elasticsearch, Kafka, Neo4j, Zookeeper) con `helm/prerequisites/values.yaml`
8. Ejecuta `helm install datahub` con `helm/values.yaml`

**Verificación:**

```bash
kubectl get pods -n datahub1
```

Todos los pods deben estar en estado `Running` o `Completed`. El pod `datahub-datahub-gms` es el último en estar listo (hasta 5 minutos después de que el resto esté en marcha).

---

### Paso 2 — Desplegar MinIO y Nessie

MinIO proporciona almacenamiento de objetos compatible con S3 para los ficheros Iceberg. Nessie actúa como catálogo REST con versionado de tablas.

```bash
kubectl apply -f KUBERNETES/DATAHUB/ingestion/iceberg/k8sJobs/nessie-minio-stack.yaml
```

Este manifiesto despliega los siguientes recursos en el namespace `datahub1`:

| Recurso | Tipo | Descripción |
|---|---|---|
| `nessie-config` | ConfigMap | Propiedades de Nessie: endpoint S3, localización del warehouse (`s3://warehouse/`), credenciales MinIO |
| `minio` | Deployment | MinIO object store — imagen `minio/minio:RELEASE.2024-01-28T22-35-53Z` |
| `minio-service` | Service | ClusterIP, puertos 9000 (API) y 9001 (consola web) |
| `nessie` | Deployment | Project Nessie v0.103.0, catálogo REST con backend en memoria |
| `nessie-service` | Service | ClusterIP, puerto 19120 |

**Verificación:**

```bash
kubectl get pods -n datahub1 | grep -E "minio|nessie"
# Esperado:
# minio-<hash>   1/1   Running   ...
# nessie-<hash>  1/1   Running   ...
```

---

### Paso 3 — Port-forward de servicios del clúster

Los servicios del clúster solo son accesibles desde dentro de la red Kubernetes. El port-forwarding los expone en `localhost` para el desarrollo local.

```bash
cd KUBERNETES/DATAHUB
chmod +x ports_forward.sh
./ports_forward.sh
```

El script abre cinco port-forwards en segundo plano:

| Servicio | Puerto en clúster | Puerto local | URL |
|---|---|---|---|
| DataHub Frontend | 9002 | **9005** | http://localhost:9005 |
| DataHub GMS | 8080 | **8080** | http://localhost:8080 |
| Nessie REST | 19120 | **19120** | http://localhost:19120 |
| MinIO API | 9000 | **9000** | http://localhost:9000 |
| MinIO Consola | 9001 | **9001** | http://localhost:9001 |


---

### Paso 4 — Crear el bucket warehouse en MinIO

Nessie está configurado para almacenar los datos de las tablas Iceberg en `s3://warehouse/`. Este bucket debe existir antes de escribir cualquier tabla.

**Opción A — Consola web de MinIO (recomendada para la primera instalación):**

1. Abre http://localhost:9001
2. Credenciales: usuario `admin` / contraseña `password`
3. Haz clic en **Buckets → Create Bucket**
4. Nombre del bucket: `warehouse`
5. Deja el resto de opciones por defecto y haz clic en **Create**

**Opción B — CLI de MinIO (`mc`):**

```bash
# Instalar mc si no está disponible
curl -O https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc && sudo mv mc /usr/local/bin/

# Configurar alias y crear bucket
mc alias set local http://localhost:9000 admin password
mc mb local/warehouse
mc ls local   # debe aparecer: [fecha] warehouse
```

---

### Paso 5 — Generar y almacenar el token de acceso DataHub

DataHub requiere un Personal Access Token (PAT) para todas las llamadas a la API. Los tokens tienen un TTL configurable. Es necesario almacenarlo en dos lugares:

1. **Secret de Kubernetes** — usado por los K8s Jobs de ingestión dentro del clúster
2. **`config.toml`** — usado por los componentes Rust que se ejecutan localmente

#### 5.1 — Generar el token desde la UI de DataHub

1. Abre http://localhost:9005
2. Credenciales: usuario `datahub` / contraseña `datahub`
3. Haz clic en tu avatar (arriba a la derecha) → **Settings → Access Tokens**
4. Haz clic en **Generate new token**
5. Asigna un nombre (por ejemplo, `local-dev`) y un TTL (por ejemplo, 90 días), luego haz clic en **Create**
6. Copia el token completo — solo se muestra una vez

#### 5.2 — Almacenar en el Secret de Kubernetes

Edita `KUBERNETES/DATAHUB/ingestion/iceberg/k8sJobs/secret-datahub-token.yaml` y sustituye el valor del token:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: datahub-token
  namespace: datahub1
type: Opaque
stringData:
  token: "TU_TOKEN_AQUI"
```

Aplica el Secret:

```bash
kubectl apply -f KUBERNETES/DATAHUB/ingestion/iceberg/k8sJobs/secret-datahub-token.yaml
kubectl get secret datahub-token -n datahub1
```

#### 5.3 — Almacenar en config.toml (clientes Rust)

Edita `KUBERNETES/DataAccess/OpenAPI/HTTPClient/config.toml`:

```toml
[datahub]
base_url = "http://localhost:8080"
token    = "TU_TOKEN_AQUI"

[server]
port = 3000
```

Edita `KUBERNETES/DataAccess/KafkaQueue/config.toml` de forma análoga si vas a usar el componente KafkaQueue.

---

### Paso 6 — Inicializar el catálogo Iceberg/Nessie

Este paso crea las tablas Iceberg en el catálogo Nessie usando un pipeline ETL de PySpark que escribe datos Parquet en MinIO.


```bash
# Instalar dependencias
pip install pyspark==3.5.1 pyarrow "pyiceberg[nessie,s3fs]>=0.8.0"

# PySpark requiere Java 11 o 17
sudo apt install openjdk-17-jre-headless
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Ejecutar el pipeline (los port-forwards del Paso 3 deben estar activos)
cd KUBERNETES/DataSources/iceberg/hadoop
python3 deployIceberg.py
```

El script inicializa una sesión Spark conectada a Nessie en `localhost:19120` y a MinIO en `localhost:9000`, genera un CSV de transacciones de ejemplo, aplica transformaciones y escribe la tabla Iceberg `local_iceberg.default.sales_transactions` en el bucket `warehouse/`.



### Paso 7 — Ingestar metadatos en DataHub

Una vez que existen datos en Iceberg/Nessie, se envían sus metadatos (esquema, linaje, estadísticas) a DataHub usando el framework `acryl-datahub`. Hay dos métodos disponibles.

#### Método A — CLI de acryl-datahub (local, manual)

```bash
pip install 'acryl-datahub[iceberg]>=0.14.0' 'pyiceberg[nessie,pyarrow]>=0.8.0'

# Tablas Iceberg desde Nessie/MinIO
python3 -m datahub ingest run -c KUBERNETES/DATAHUB/ingestion/iceberg/receptaIceberg.yaml

# AEMET OpenData (datasets meteorológicos vía OpenAPI)
python3 -m datahub ingest run -c KUBERNETES/DATAHUB/ingestion/iceberg/AEMET.yaml
```

> Antes de ejecutar, abre cada receta YAML y sustituye el campo `token:` bajo `sink.config` con el token generado en el Paso 5.

**Recetas disponibles:**

| Fichero | Fuente | Descripción |
|---|---|---|
| `ingestion/iceberg/receptaIceberg.yaml` | Iceberg (Nessie) | Ingesta esquemas de tablas Iceberg desde el catálogo Nessie |
| `ingestion/iceberg/AEMET.yaml` | OpenAPI (AEMET) | Ingesta la especificación de la API meteorológica de AEMET |
| `ingestion/openapi/recipes/AEMET_FEDE.yaml` | DataHub → DataHub | Federación: copia metadatos de una instancia DataHub a otra |
| `ingestion/openapi/recipes/EU_FEDE.yaml` | DataHub → DataHub | Federación desde el Portal de Datos Abiertos de la UE |

#### Método B — Kubernetes Job (automatizado, dentro del clúster)

El job de ingestión se ejecuta completamente dentro del clúster. El pod de ingestión tiene acceso directo a Nessie y al GMS de DataHub a través de sus servicios ClusterIP internos (no se requiere port-forward).

```bash
# Apuntar Docker al registro interno de Minikube
eval $(minikube docker-env -p datahub)

# Construir la imagen de ingestión dentro del contexto Docker de Minikube
docker build -t datahub-ingestion-iceberg:latest \
  KUBERNETES/DATAHUB/ingestion/iceberg/k8sJobs/

# Actualizar el token en el ConfigMap de la receta
# Editar KUBERNETES/DATAHUB/ingestion/iceberg/k8sJobs/iceberg_ingestion.yaml
# → sustituir: token: "TU_TOKEN_AQUI"

# Opción 1 — Script de automatización completa
cd KUBERNETES/DATAHUB/ingestion/iceberg/k8sJobs
./ingest.sh

# Opción 2 — Aplicación manual
kubectl apply -f secret-datahub-token.yaml
kubectl apply -f iceberg_ingestion.yaml       # ConfigMap con la receta
kubectl apply -f iceberg_ingestion_job.yaml   # Batch Job

# Seguir los logs
kubectl logs job/iceberg-ingestion-job -n datahub1 --follow
```

**Volver a ejecutar el job** (los K8s Jobs son inmutables — eliminar y reaplicar):

```bash
kubectl delete job/iceberg-ingestion-job -n datahub1 --ignore-not-found
kubectl apply -f iceberg_ingestion_job.yaml
# O usar el script de conveniencia:
./recastJob.sh
```

**Verificación en la UI de DataHub:**  
Abre http://localhost:9005 → **Datasets**. Debe aparecer la tabla `sales_transactions` (plataforma: `iceberg`, instancia: `default`).

---

### Paso 8 — Capa de acceso DCAT3

Esta capa expone los metadatos almacenados en DataHub a través de la ontología [DCAT3](https://www.w3.org/TR/vocab-dcat-3/). Está compuesta por tres componentes que se pueden usar de forma independiente o simultánea. La especificación completa de la API REST está en [`REST_API.yml`](./REST_API.yml).

---

#### 8a — HTTP Client DCAT3 (Rust / Axum)

Servidor HTTP que actúa como capa de traducción entre la API REST DCAT3 y la REST API v2 de DataHub. Cada solicitud DCAT3 entrante se transforma en una o varias llamadas al GMS de DataHub.

**Mapeo de entidades:**

| Entidad DataHub | Concepto DCAT3 |
|---|---|
| `container` | `dcat:Catalog` |
| `dataset` | `dcat:Dataset` |
| `dataset` (con propiedad `dcat_distribution=true`) | `dcat:Distribution` |
| `dataFlow` | `dcat:DataService` |
| `tag` | `dcat:keyword` |
| `domain` | `dcat:theme` |

**Configuración** (`KUBERNETES/DataAccess/OpenAPI/HTTPClient/config.toml`):

```toml
[datahub]
base_url = "http://localhost:8080"   # GMS vía port-forward (Paso 3)
token    = "TU_TOKEN_AQUI"           # token del Paso 5

[server]
port = 3000
```

También se puede configurar mediante variables de entorno (útil en scripts):

```bash
export DATAHUB__BASE_URL="http://localhost:8080"
export DATAHUB__TOKEN="eyJhbGciOiJIUzI1NiJ9..."
```

**Lanzamiento local:**

```bash
cd KUBERNETES/DataAccess/OpenAPI/HTTPClient
cargo build --release   # primera compilación: 2–5 minutos
cargo run --release
# → API disponible en http://localhost:3000
```

**Lanzamiento como contenedor Docker:**

```bash
cd KUBERNETES/DataAccess/OpenAPI/HTTPClient
docker build -t datahub-dcat-client .
docker run --rm --network host \
  -e DATAHUB__BASE_URL="http://localhost:8080" \
  -e DATAHUB__TOKEN="TU_TOKEN_AQUI" \
  datahub-dcat-client
```

---

#### 8b — KafkaQueue (Rust / consumidor Kafka)

Binario Rust que consume directamente los topics Kafka de DataHub, mantiene un estado en memoria y genera dos flujos de salida independientes:

1. **Snapshot NDJSON** (`dcat_output.ndjson`) — una línea por entidad con su estado completo actual
2. **Reenvío HTTP** — llamadas REST hacia un servidor DCAT3 externo

A diferencia del HTTP Client (8a), KafkaQueue no recibe peticiones; _empuja_ activamente los cambios hacia afuera en cuanto los detecta en Kafka.

**Mapeo de entidades:**

| Entidad DataHub | Clase DCAT3 | Condición |
|---|---|---|
| `container` | `dcat:Catalog` | Siempre |
| `dataset` | `dcat:Dataset` | Por defecto |
| `dataset` | `dcat:Distribution` | Si `dcat_distribution = "true"` (propiedad custom) |
| `dataFlow` | `dcat:DataService` | Siempre |

**Configuración** (`KUBERNETES/DataAccess/KafkaQueue/config.toml`):

```toml
[kafka]
bootstrap_servers = "localhost:9092"
group_id          = "datahub-dcat-consumer-dev"
topics            = ["MetadataChangeProposal_v1", "MetadataChangeLog_Versioned_v1", "MetadataChangeLog_Timeseries_v1"]
auto_offset_reset = "earliest"

[schema_registry]
url = "http://localhost:8080"

[server]
port = 3001        # endpoint de health check

[output]
dcat_file = "dcat_output.ndjson"

[http_sender]
server_url = "http://10.0.2.15:8080"   # servidor DCAT3 externo
```

Los valores del ConfigMap de Kubernetes sobreescriben la configuración usando el separador `__` (por ejemplo, `KAFKA__BOOTSTRAP_SERVERS`).

**Lanzamiento local:**

```bash
cd KUBERNETES/DataAccess/KafkaQueue
cargo run
# Health check: curl http://localhost:3001/health
```

**Despliegue en Kubernetes:**

```bash
cd KUBERNETES/DataAccess/KafkaQueue
chmod +x runK8s.sh
./runK8s.sh
# O manualmente:
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/deployment.yaml
```

---

#### 8c — DataHub Actions (Python / forwarder de eventos)

Extensión del framework [DataHub Actions](https://datahubproject.io/docs/actions/) que traduce los eventos de ciclo de vida de alto nivel de DataHub (`EntityChangeEvent_v1`) en llamadas HTTP directas hacia un servidor DCAT3, manteniendo la sincronización en tiempo casi real.

A diferencia de KafkaQueue (que trabaja con eventos de bajo nivel de Kafka), DataHub Actions opera a nivel de negocio: recibe eventos semánticos ya procesados por el GMS.

**Eventos manejados y su traducción:**

| Evento DataHub | Llamada DCAT3 |
|---|---|
| Container `LIFECYCLE CREATE` | `POST /catalogs` (con fallback upsert si 409) |
| Container `LIFECYCLE REMOVE` | `DELETE /catalogs/{urn}` |
| Dataset `LIFECYCLE CREATE` | `POST /catalogs/{platform}/datasets` |
| Dataset `LIFECYCLE REMOVE` | `DELETE /datasets/{urn}` |
| Dataset `OWNER ADD/MODIFY` | `GET /datasets/{id}` + `PUT` (actualiza `dcterms_publisher`) |
| Dataset `TECHNICAL_SCHEMA ADD` | `GET /datasets/{id}` + `PUT` (añade campo al description) |

**Despliegue en Kubernetes:**

```bash
# Construir la imagen dentro del contexto Docker de Minikube
eval $(minikube docker-env -p datahub)
docker build -t datahub-actions-dcat:latest KUBERNETES/DataAccess/DataHubActions/

# Aplicar los manifiestos
kubectl apply -f KUBERNETES/DataAccess/DataHubActions/configmap.yml
kubectl apply -f KUBERNETES/DataAccess/DataHubActions/k8s/pvc.yaml
# El Deployment se incluye en el values.yaml de DataHub Helm (como sidecar o workload independiente)
```
---

### Estructura de archivos — Kubernetes

```
KUBERNETES/
├── DATAHUB/
│   ├── deployEnv.sh                       # Paso 1: arranca Minikube + despliega DataHub vía Helm
│   ├── ports_forward.sh                   # Paso 3: port-forward de todos los servicios del clúster
│   ├── helm/
│   │   ├── values.yaml                    # Valores del chart Helm de DataHub
│   │   └── prerequisites/values.yaml     # Valores de prerequisitos (MySQL, Kafka, ES, Neo4j...)
│   └── ingestion/
│       ├── iceberg/
│       │   ├── receptaIceberg.yaml        # Receta CLI: Iceberg → DataHub
│       │   ├── AEMET.yaml                 # Receta CLI: AEMET OpenAPI → DataHub
│       │   └── k8sJobs/
│       │       ├── nessie-minio-stack.yaml     # Paso 2: despliega MinIO + Nessie
│       │       ├── Dockerfile                  # Imagen del contenedor de ingestión
│       │       ├── iceberg_ingestion.yaml      # ConfigMap con la receta in-cluster
│       │       ├── iceberg_ingestion_job.yaml  # Kubernetes Batch Job
│       │       ├── secret-datahub-token.yaml   # Paso 5: Secret K8s con el PAT
│       │       └── recastJob.sh                # Rehace el job (delete + apply)
│       └── openapi/
│           └── recipes/
│               ├── AEMET_FEDE.yaml        # Federación: DataHub → DataHub (AEMET)
│               └── EU_FEDE.yaml           # Federación: DataHub → DataHub (EU Open Data)
│
├── DataSources/
│   └── iceberg/hadoop/
│       ├── deployIceberg.py               # Paso 6: ETL PySpark → Nessie/MinIO
│       └── data_input/transactions.csv    # Datos de muestra de entrada
│
└── DataAccess/
    ├── GraphQL/HTTPClient/               # Ejemplos de consultas GraphQL a DataHub
    └── OpenAPI/
        └── HTTPClient/                    # Paso 8a: cliente HTTP DCAT3 en Rust
            ├── Cargo.toml
            ├── config.toml               # URL de DataHub + token + puerto del servidor
            ├── Dockerfile
            └── src/
                ├── main.rs               # Punto de entrada
                ├── config.rs             # Carga de configuración (TOML + env vars)
                ├── error.rs              # Tipos de error
                ├── models/               # Structs de dominio DCAT3
                ├── repository/           # Interfaces de repositorio (trait)
                ├── datahub/              # Implementaciones REST API v2 de DataHub
                └── api/                  # Handlers HTTP de Axum (rutas DCAT3)
    └── KafkaQueue/                        # Paso 8b: consumidor Kafka DCAT3 en Rust
        ├── Cargo.toml
        ├── config.toml                   # Kafka bootstrap, schema registry, HTTP sink
        ├── Dockerfile
        ├── runK8s.sh                     # Script de despliegue en Kubernetes
        └── k8s/
            ├── configmap.yaml            # Overrides de configuración vía env vars
            ├── secret.yaml               # Secretos (autenticación futura)
            └── deployment.yaml           # Deployment K8s con health probes
    └── DataHubActions/                    # Paso 8c: forwarder de eventos Python
        ├── Dockerfile
        ├── action.yml                    # Definición del pipeline de acciones
        ├── configmap.yml                 # ConfigMap K8s con la configuración de la acción
        ├── setup.py
        └── src/
            ├── dcat_forwarder/           # Plugin de producción: eventos → HTTP DCAT3
            └── event_logger/             # Plugin de depuración: eventos → NDJSON
```

---

### Credenciales de referencia

| Servicio | URL | Usuario | Contraseña |
|---|---|---|---|
| DataHub UI | http://localhost:9005 | `datahub` | `datahub` |
| DataHub GMS API | http://localhost:8080 | — | Bearer token |
| MinIO Consola | http://localhost:9001 | `admin` | `password` |
| MinIO API | http://localhost:9000 | `admin` | `password` |
| Nessie REST | http://localhost:19120 | — | sin autenticación |
| DCAT3 API | http://localhost:3000 | — | sin autenticación |
| MySQL (interno) | clúster: 3306 | `datahub` | `datahub` |
| Neo4j (interno) | clúster: 7687 | `neo4j` | `datahub` |

> **Aviso de seguridad:** Las credenciales anteriores son valores por defecto de desarrollo, únicamente aptos para entornos locales/laboratorio. No usar en producción.

---

## Parte II — Federación de Catálogos (Docker)

### Arquitectura de federación

La federación conecta tres instancias DataHub independientes. Cada instancia fuente ingesta datos de un dominio concreto; la instancia de federación agrega los metadatos de ambas fuentes en un único punto de consulta.

```
┌─────────────────────────────┐
│  Instancia 1 — AEMET        │   GMS: :8080  │ Frontend: :9002
│  Fuente: opendata.aemet.es  │
└─────────────┬───────────────┘
              │  MetadataChangeLog_Versioned_v1 (Kafka)
              │  DataHub Actions (federation_forwarder)
              │
              ▼
┌─────────────────────────────────┐
│  Instancia Federación           │   GMS: :8084  │ Frontend: :9004
│  Agrega metadatos de ambas      │◄──────────────────────────────────┐
│  fuentes                        │                                   │
└─────────────────────────────────┘               │  MetadataChangeLog_Versioned_v1 (Kafka)
                                                  │  DataHub Actions (federation_forwarder)
                                                  │
                                    ┌─────────────┴───────────────┐
                                    │  Instancia 2 — EU Open Data │   GMS: :8082  │ Frontend: :9003
                                    │  Fuente: data.europa.eu     │
                                    └─────────────────────────────┘
```

**Resumen de instancias:**

| Instancia | Fichero `.env` | GMS | Frontend | Fuente de datos |
|---|---|---|---|---|
| Instancia 1 | `.env.1` | :8080 | :9002 | AEMET OpenData |
| Instancia 2 | `.env.2` | :8082 | :9003 | EU Open Data Portal |
| Federación | `.env.fede` | :8084 | :9004 | Recibe de las instancias 1 y 2 |

Cada instancia es completamente independiente: tiene su propio Kafka, Schema Registry, MySQL y Elasticsearch.

---

### Prerequisitos de federación

| Herramienta | Versión mínima |
|---|---|
| `docker` | ≥ 24 |
| `docker compose` | v2 (plugin integrado) |
| `python3` + `acryl-datahub` | ≥ 0.14.0 (para ingestión manual) |

**Hardware:** El host debe tener recursos suficientes para ejecutar las tres instancias simultáneamente. Estimación orientativa: 8 GB RAM y 4 núcleos CPU como mínimo; 16 GB RAM recomendados.

---

### Paso 1 — Configurar variables de entorno

Cada instancia DataHub se configura mediante un fichero `.env` diferente ubicado en `DOCKER/`.

**Variables principales:**

| Variable | Descripción |
|---|---|
| `INSTANCE` | Identificador de instancia (`1`, `2` o `fede`) — se usa como sufijo de puertos y nombres de red |
| `GMS_PORT` | Puerto externo del servicio GMS de DataHub |
| `FRONTEND_PORT` | Puerto externo del frontend React |
| `KAFKA_EXTERNAL_PORT` | Puerto externo del broker Kafka |
| `MYSQL_PORT` | Puerto externo de MySQL |
| `DATAHUB_TOKEN` | PAT de DataHub usado por los componentes internos |
| `AEMET_API_KEY` | Clave de la API REST de AEMET (solo en `.env.1`) |
| `FEDERATION_GMS_URL` | URL del GMS de la instancia de federación (usado por el forwarder de Actions) |

Actualiza los valores de `DATAHUB_TOKEN` y `AEMET_API_KEY` según tu entorno antes de arrancar.

---

### Paso 2 — Levantar las instancias DataHub

```bash
cd DOCKER

# Instancia 1 (AEMET)
docker compose --env-file .env.1 up -d

# Instancia 2 (EU Open Data)
docker compose --env-file .env.2 up -d

# Instancia de federación
docker compose --env-file .env.fede up -d
```

Cada invocación levanta un stack completo: Zookeeper, Kafka, Schema Registry, MySQL, Elasticsearch, DataHub GMS, DataHub Frontend, DataHub Actions y DataHub Upgrade.

**Verificación:**

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

**URLs de los frontends:**

| Instancia | URL | Usuario | Contraseña |
|---|---|---|---|
| Instancia 1 | http://localhost:9002 | `datahub` | `datahub` |
| Instancia 2 | http://localhost:9003 | `datahub` | `datahub` |
| Federación | http://localhost:9004 | `datahub` | `datahub` |

> En la primera ejecución DataHub tarda varios minutos en completar la inicialización. Espera a que el contenedor `datahub-actions` esté en estado `healthy` antes de continuar.

---

### Paso 3 — Ingestión inicial de fuentes de datos

Este paso carga los metadatos de las fuentes externas en sus respectivas instancias DataHub.

```bash
pip install 'acryl-datahub[datahub,openapi]>=0.14.0'
```

> Antes de ejecutar cada receta, abre el fichero YAML y sustituye el campo `token:` en `sink.config` por el token de la instancia DataHub destino.

```bash
# AEMET OpenData → Instancia 1
python3 -m datahub ingest run -c DOCKER/ingestion/manual/AEMET.yaml

# EU Open Data Portal → Instancia 2
python3 -m datahub ingest run -c DOCKER/ingestion/manual/DataEU.yaml
```

---

### Paso 4 — Ingestión periódica con Apache Airflow

Apache Airflow orquesta la ingestión periódica hacia las instancias DataHub. El stack de Airflow se conecta a las redes de Docker de las tres instancias.

```bash
cd DOCKER/ingestion/airflow
docker compose up -d
```

**Acceso a la UI de Airflow:**  
http://localhost:8086 — usuario `airflow` / contraseña `airflow`

El DAG `datahub_periodic_ingestion` ejecuta la ingestión cada 30 minutos.

**Configuración de la receta de ingestión:**  
Edita `DOCKER/ingestion/airflow/recipes/recipe.yml` con la fuente y el destino deseados antes de arrancar Airflow.

**Imagen de Airflow:**  
El `Dockerfile` en `DOCKER/ingestion/airflow/` extiende `apache/airflow:2.10.5-python3.11` añadiendo `acryl-datahub` para que el DAG pueda ejecutar comandos `datahub ingest`.

---

### Paso 5 — Federación automática mediante DataHub Actions

El componente `federation_forwarder` se incluye en el stack de docker-compose de cada instancia fuente (construido desde `DOCKER/ingestion/DataHubActions/`). Se encarga de replicar automáticamente los cambios de metadatos hacia la instancia de federación.

**Mecanismo de funcionamiento:**

1. Escucha el topic Kafka `MetadataChangeLog_Versioned_v1` de la instancia fuente
2. Aplica un debounce de 30 segundos para agrupar cambios consecutivos
3. Construye dinámicamente una receta de ingestión `datahub → datahub-rest` usando variables de entorno
4. Ejecuta `datahub ingest` con ingestión con estado (_stateful_) para evitar duplicados

**Filtrado de metadatos internos:**  
Los URNs internos de DataHub se excluyen de la federación para evitar contaminar la instancia de federación con configuración de sistema:

- `urn:li:dataHubPageModule:*`
- `urn:li:dataHubIngestionSource:*`
- `urn:li:dataHubExecutionRequest:*`
- `urn:li:dataHubSecret:*`
- `urn:li:dataHubAccessToken:*`
- `urn:li:inviteToken:*`
- `urn:li:globalSettings:*`
- `urn:li:dataHubStepState:*`

**Variables de entorno relevantes** (configuradas en cada `.env`):

| Variable | Descripción |
|---|---|
| `FEDERATION_GMS_URL` | URL del GMS de la instancia de federación (por ejemplo, `http://host.docker.internal:8084`) |
| `SOURCE_MYSQL_HOST_PORT` | Host y puerto MySQL de la instancia fuente |
| `SOURCE_KAFKA_BOOTSTRAP` | Bootstrap servers del broker Kafka de la instancia fuente |
| `SOURCE_SCHEMA_REGISTRY_URL` | URL del Schema Registry de la instancia fuente |

---

### Paso 6 — Federación manual (alternativa)

Como alternativa o complemento a la federación automática del Paso 5, es posible ejecutar la federación manualmente mediante recetas CLI:

```bash
# Instancia 1 → Federación
python3 -m datahub ingest run -c DOCKER/ingestion/manual/AEMET_FEDE.yaml

# Instancia 2 → Federación
python3 -m datahub ingest run -c DOCKER/ingestion/manual/EU_FEDE.yaml
```

Ambas recetas usan el conector `datahub` como fuente (lee desde MySQL + Kafka de la instancia origen) y `datahub-rest` como sink (escribe en la instancia de federación). Aplican los mismos filtros de URNs que el forwarder automático.

---

### Estructura de archivos — Docker

```
DOCKER/
├── .env.1                                # Variables de entorno — Instancia 1 (AEMET)
├── .env.2                                # Variables de entorno — Instancia 2 (EU Open Data)
├── .env.fede                             # Variables de entorno — Instancia de federación
├── docker-compose.yml                    # Stack DataHub: Kafka, MySQL, ES, GMS, Frontend, Actions
└── ingestion/
    ├── DataHubActions/                   # Paso 5: forwarder de federación event-driven
    │   ├── Dockerfile                    # Imagen basada en datahub-actions:head-slim
    │   ├── entrypoint.sh                 # Genera action.yml desde env vars y arranca datahub-actions
    │   ├── federation_forwarder.py       # Plugin de acción: Kafka → datahub ingest
    │   └── setup.py                      # Entry point del plugin
    ├── airflow/                          # Paso 4: orquestación periódica
    │   ├── docker-compose.yaml           # Stack Airflow (Postgres + Webserver + Scheduler)
    │   ├── Dockerfile                    # apache/airflow + acryl-datahub
    │   ├── dags/
    │   │   └── datagub_ingestion.py      # DAG: datahub_periodic_ingestion (cada 30 min)
    │   └── recipes/
    │       └── recipe.yml                # Receta de ingestión usada por el DAG
    └── manual/                           # Paso 3 y 6: recetas de ingestión manual
        ├── AEMET.yaml                    # AEMET OpenData → Instancia 1
        ├── DataEU.yaml                   # EU Open Data Portal → Instancia 2
        ├── AEMET_FEDE.yaml               # Instancia 1 → Federación
        └── EU_FEDE.yaml                  # Instancia 2 → Federación
```

---

## Referencia rápida (Quick Start)

### Quick Start — Kubernetes

```bash
# 1. Desplegar clúster y DataHub
cd KUBERNETES/DATAHUB && ./deployEnv.sh -n 1

# 2. Desplegar MinIO + Nessie
kubectl apply -f ingestion/iceberg/k8sJobs/nessie-minio-stack.yaml

# 3. Port-forward de servicios (mantener este terminal abierto)
./ports_forward.sh

# 4. Crear bucket warehouse en MinIO (en otro terminal)
mc alias set local http://localhost:9000 admin password && mc mb local/warehouse

# 5. Generar token en la UI de DataHub → copiar en:
#    - ingestion/iceberg/k8sJobs/secret-datahub-token.yaml
#    - DataAccess/OpenAPI/HTTPClient/config.toml
#    - DataAccess/KafkaQueue/config.toml
kubectl apply -f ingestion/iceberg/k8sJobs/secret-datahub-token.yaml

# 6. Inicializar tablas Iceberg
cd ../DataSources/iceberg/hadoop && python3 deployIceberg.py

# 7. Ingestar metadatos en DataHub
python3 -m datahub ingest run -c KUBERNETES/DATAHUB/ingestion/iceberg/receptaIceberg.yaml
# O mediante K8s Job:
cd KUBERNETES/DATAHUB/ingestion/iceberg/k8sJobs
./setupKubeJobImage.sh
./recastJob.sh

# 8a. Arrancar la API DCAT3 (HTTP Client)
cd KUBERNETES/DataAccess/OpenAPI/HTTPClient && cargo run --release
# → API disponible en http://localhost:3000

# 8b. Arrancar KafkaQueue (opcional, sincronización NDJSON + HTTP)
cd KUBERNETES/DataAccess/KafkaQueue && cargo run

# 8c. Desplegar DataHub Actions (opcional, forwarding en tiempo real)
kubectl apply -f KUBERNETES/DataAccess/DataHubActions/configmap.yml
```

---

### Quick Start — Docker

```bash
cd DOCKER

# Levantar las tres instancias DataHub
docker compose --env-file .env.1 up -d
docker compose --env-file .env.2 up -d
docker compose --env-file .env.fede up -d

# Esperar a que los stacks estén healthy (~3–5 minutos)
docker ps

# Ingestión inicial de fuentes de datos
python3 -m datahub ingest run -c ingestion/manual/AEMET.yaml
python3 -m datahub ingest run -c ingestion/manual/DataEU.yaml

# Levantar Airflow para ingestión periódica (cada 30 min)
cd ingestion/airflow && docker compose up -d
# UI: http://localhost:8086

# Federación manual (si el forwarder automático no está activo)
cd ../..
python3 -m datahub ingest run -c ingestion/manual/AEMET_FEDE.yaml
python3 -m datahub ingest run -c ingestion/manual/EU_FEDE.yaml
```
