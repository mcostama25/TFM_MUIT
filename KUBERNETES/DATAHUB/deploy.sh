#!/bin/bash
set -euo pipefail

#######################################
# Defaults
#######################################
CPUS=6
MEMORY=16g
RELEASE_NAME="datahub"
HELM_REPO_NAME="datahub"
HELM_REPO_URL="https://helm.datahubproject.io/"
HELM_CHART="datahub/datahub"

#######################################
# Usage
#######################################
usage() {
    echo "Usage: $0 -n <number_of_clusters>"
    exit 1
}

#######################################
# Parse arguments
#######################################
while getopts "n:" opt; do
    case "$opt" in
        n) NUM_CLUSTERS="$OPTARG" ;;
        *) usage ;;
    esac
    done

if [[ -z "${NUM_CLUSTERS:-}" ]]; then
    usage
fi

if ! [[ "$NUM_CLUSTERS" =~ ^[0-9]+$ ]] || [[ "$NUM_CLUSTERS" -lt 1 ]]; then
    echo "Error: -n must be a positive integer"
    exit 1
fi

#######################################
# Pre-flight checks
#######################################
command -v minikube >/dev/null 2>&1 || { echo "minikube not found"; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo "kubectl not found"; exit 1; }
command -v helm >/dev/null 2>&1 || { echo "helm not found"; exit 1; }

#######################################
# Helm repo setup
#######################################
if ! helm repo list | grep -q "^${HELM_REPO_NAME}"; then
    helm repo add "$HELM_REPO_NAME" "$HELM_REPO_URL"
fi
helm repo update

#######################################
# Clean environment
#######################################
minikube delete --all --purge 2>/dev/null || true

#######################################
# Deploy cluster/profile
#######################################
PROFILE="datahub"

echo "========================================"
echo " Deploying cluster: ${PROFILE}"
echo "========================================"

if ! minikube profile list | grep -q "^| ${PROFILE} "; then
minikube start \
    -p "$PROFILE" \
    --cpus="$CPUS" \
    --memory="$MEMORY"
else
echo "Minikube profile ${PROFILE} already exists"
fi

kubectl config use-context "$PROFILE"

#######################################
# Deploy Instances/namespaces
#######################################
for i in $(seq 1 "$NUM_CLUSTERS"); do

    NAMESPACE="datahub${i}"
    
    if ! kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
    kubectl create namespace "$NAMESPACE"
    fi

    kubectl create secret generic mysql-secrets --from-literal=mysql-root-password=datahub --from-literal=mysql-password=datahub -n "$NAMESPACE"
    kubectl create secret generic neo4j-secrets --from-literal=neo4j-password=datahub --from-literal=NEO4J_AUTH=neo4j/datahub -n "$NAMESPACE"

    if helm status "$RELEASE_NAME${i}" -n "$NAMESPACE" >/dev/null 2>&1; then
        echo "Helm release already installed in ${PROFILE}"
    else
        helm install prerequisites "$HELM_CHART-prerequisites" \
            --namespace "$NAMESPACE" \
            --values helm/prerequisites/values.yaml \
            --create-namespace \
            --timeout 20m \
            --wait=true

        echo "[+] Prerequisites deployed"

        helm install "$RELEASE_NAME" "$HELM_CHART" \
            --namespace "$NAMESPACE" \
            --values helm/values.yaml \
            --create-namespace \
            --timeout 20m \
            --wait=true
    fi

    echo "[+]DataHub${i} deployed in ${PROFILE}"
done
