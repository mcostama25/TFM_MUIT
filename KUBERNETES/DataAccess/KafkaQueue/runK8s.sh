#!/bin/bash
set -e

docker build --no-cache -t datahub-kafka-consumer:latest .
# for minikube:
minikube image load datahub-kafka-consumer:latest -p datahub

kubectl delete configmap/datahub-kafka-consumer-config  -n datahub1 --ignore-not-found
kubectl delete secret/datahub-kafka-consumer-secret  -n datahub1 --ignore-not-found
kubectl delete deployment/datahub-kafka-consumer  -n datahub1 --ignore-not-found


kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/deployment.yaml