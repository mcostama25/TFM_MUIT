#!/bin/bash

kubectl port-forward svc/datahub-datahub-frontend -n datahub1 9005:9002 &

kubectl port-forward svc/datahub-datahub-gms -n datahub1 8080:8080 &

kubectl port-forward svc/nessie-service -n datahub1 19120:19120 &

kubectl port-forward svc/minio-service -n datahub1 9001:9001 &

kubectl port-forward svc/minio-service -n datahub1 9000:9000 &
