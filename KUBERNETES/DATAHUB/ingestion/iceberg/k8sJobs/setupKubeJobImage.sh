#!/bin/bash

image="datahub-ingestion-iceberg:latest"
docker build -t "$image" .
minikube image load "$image" -p datahub
echo "[+] Waiting for the image to be loaded into minikube..."
