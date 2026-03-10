#!/bin/bash

image="datahub-ingestion-iceberg:latest"

docker build -t "$image" .

minikube image load "$image" -p datahub


echo "[+] Waiting for the image to be loaded into minikube..."
sleep 2

echo "[+] Deploying Nessie service..."
kubectl apply -f nessieService.yaml

# sleep 3
# echo "[+] Port forwarding to Nessie service on port 19120..."
# kubectl port-forward service/nessie-service 19120:19120 -n datahub1 &

echo "[!]Setup completed"

