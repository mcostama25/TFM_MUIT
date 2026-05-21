#!/usr/bin/env bash
# Deploy all three DataHub instances and run ingestion.
# Run from the DOCKER directory: bash deploy.sh
# Requires: docker, docker compose v2, datahub CLI in PATH.
# Set SKIP_INGESTION=true to skip datahub ingest steps.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

wait_for_gms() {
    local port=$1 label=$2
    printf 'Waiting for %s GMS on port %s' "$label" "$port"
    until curl -sf "http://localhost:${port}/health" >/dev/null 2>&1; do
        printf '.'
        sleep 5
    done
    echo " ready."
}

echo "=== Starting Instance 1 ==="
docker compose --env-file .env.1 --project-name datahub1 up -d

echo "=== Starting Instance 2 ==="
docker compose --env-file .env.2 --project-name datahub2 up -d

wait_for_gms 8080 "Instance 1"
wait_for_gms 8082 "Instance 2"

if [ "${SKIP_INGESTION:-false}" != "true" ]; then
    echo "=== Running source ingestion ==="
    datahub ingest -c ingestion/AEMET.yaml
    datahub ingest -c ingestion/DataEU.yaml
fi

echo "=== Starting Federation instance ==="
docker compose --env-file .env.fede --project-name datahubfede up -d

wait_for_gms 8084 "Federation"

if [ "${SKIP_INGESTION:-false}" != "true" ]; then
    echo "=== Running federation ingestion ==="
    datahub ingest -c ingestion/AEMET_FEDE.yaml
    datahub ingest -c ingestion/EU_FEDE.yaml
fi

cat <<'EOF'

=== Deployment complete ===
  Instance 1 :  http://localhost:9002  (GMS: http://localhost:8080)
  Instance 2 :  http://localhost:9003  (GMS: http://localhost:8082)
  Federation :  http://localhost:9004  (GMS: http://localhost:8084)

To tear down (preserving data volumes):
  docker compose --env-file .env.1    --project-name datahub1    down
  docker compose --env-file .env.2    --project-name datahub2    down
  docker compose --env-file .env.fede --project-name datahubfede down

To tear down and delete all data:
  docker compose --env-file .env.1    --project-name datahub1    down -v
  docker compose --env-file .env.2    --project-name datahub2    down -v
  docker compose --env-file .env.fede --project-name datahubfede down -v
EOF
