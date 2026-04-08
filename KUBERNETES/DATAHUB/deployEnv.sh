#!/bin/bash
set -euo pipefail

#######################################
# Defaults
#######################################
CPUS=6
MEMORY=12g
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
command -v kubectl  >/dev/null 2>&1 || { echo "kubectl not found";  exit 1; }
command -v helm     >/dev/null 2>&1 || { echo "helm not found";     exit 1; }

#######################################
# Helm repo setup
#######################################
if ! helm repo list | grep -q "^${HELM_REPO_NAME}"; then
    helm repo add "$HELM_REPO_NAME" "$HELM_REPO_URL"
fi
# helm repo update

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
# Helper: crea un recurs compatible amb Helm
#######################################
create_helm_managed_secret() {
    local name=$1
    local namespace=$2
    local release=$3
    shift 3
    # $@ són els --from-literal

    kubectl create secret generic "$name" \
        "$@" \
        -n "$namespace" \
        --dry-run=client -o yaml \
    | kubectl annotate --local -f - \
        "meta.helm.sh/release-name=${release}" \
        "meta.helm.sh/release-namespace=${namespace}" \
        --overwrite -o yaml \
    | kubectl label --local -f - \
        app.kubernetes.io/managed-by=Helm \
        --overwrite -o yaml \
    | kubectl apply -f -
}

create_helm_managed_sa() {
    local name=$1
    local namespace=$2
    local release=$3

    kubectl create serviceaccount "$name" \
        -n "$namespace" \
        --dry-run=client -o yaml \
    | kubectl annotate --local -f - \
        "meta.helm.sh/release-name=${release}" \
        "meta.helm.sh/release-namespace=${namespace}" \
        --overwrite -o yaml \
    | kubectl label --local -f - \
        app.kubernetes.io/managed-by=Helm \
        --overwrite -o yaml \
    | kubectl apply -f -
}


#######################################
# Deploy Instances/namespaces
#######################################
for i in $(seq 1 "$NUM_CLUSTERS"); do

    NAMESPACE="datahub${i}"

    if ! kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
        kubectl create namespace "$NAMESPACE"
    fi

    # Secrets estàndards (ja existien)
    kubectl create secret generic mysql-secrets \
        --from-literal=mysql-root-password=datahub \
        --from-literal=mysql-password=datahub \
        -n "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

    kubectl create secret generic neo4j-secrets \
        --from-literal=neo4j-password=datahub \
        --from-literal=NEO4J_AUTH=neo4j/datahub \
        -n "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

    # Nous recursos compatibles amb Helm
    create_helm_managed_sa "datahub-operator-sa" "$NAMESPACE" "$RELEASE_NAME"

    create_helm_managed_secret "datahub-auth-secrets" "$NAMESPACE" "$RELEASE_NAME" \
        --from-literal=token_service_signing_key=$(openssl rand -base64 32) \
        --from-literal=token_service_salt=$(openssl rand -base64 32) \
        --from-literal=system_client_id="datahub" \
        --from-literal=system_client_secret=$(openssl rand -base64 32)

    # -------------------------------------------------------
    # NOU: Pre-crear el ServiceAccount que necessita el hook
    # datahub-system-update abans que Helm intenti el deploy
    # -------------------------------------------------------
    echo "[+] Pre-creating datahub-operator-sa in ${NAMESPACE}..."
    kubectl create serviceaccount datahub-operator-sa \
        -n "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

    kubectl create clusterrolebinding "datahub-operator-crb-${NAMESPACE}" \
        --clusterrole=cluster-admin \
        --serviceaccount="${NAMESPACE}:datahub-operator-sa" \
        --dry-run=client -o yaml | kubectl apply -f -

    echo "[+] ServiceAccount datahub-operator-sa ready in ${NAMESPACE}"
    # -------------------------------------------------------

    if helm status "$RELEASE_NAME${i}" -n "$NAMESPACE" >/dev/null 2>&1; then
        echo "Helm release already installed in ${PROFILE}"
    else
        helm install prerequisites "$HELM_CHART-prerequisites" \
            --namespace "$NAMESPACE" \
            --values helm/prerequisites/values.yaml \
            --create-namespace \
            --timeout 30m \
            --wait=true

        echo "[+] Prerequisites deployed"

        helm install "$RELEASE_NAME" "$HELM_CHART" \
            --namespace "$NAMESPACE" \
            --values helm/values.yaml \
            --create-namespace \
            --timeout 30m \
            --wait=true
    fi

    echo "[+] DataHub${i} deployed in ${PROFILE}"
done