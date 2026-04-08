#!/bin/bash

kubectl delete job/iceberg-ingestion-job  -n datahub1 --ignore-not-found
kubectl delete configmap/iceberg-recipe  -n datahub1 --ignore-not-found
kubectl delete secret/datahub-token  -n datahub1 --ignore-not-found

kubectl apply -f secret-datahub-token.yaml
kubectl apply -f iceberg_ingestion.yaml
kubectl apply -f iceberg_ingestion_job.yaml

echo "[!] Job running"

sleep 10

kubectl logs job/iceberg-ingestion-job -n datahub1 --follow
