#!/bin/bash
# Genera el fichero de configuración de DataHub Actions en /tmp/action.yml
# usando las variables de entorno inyectadas por docker-compose,
# y luego arranca el proceso datahub-actions.
set -e

python3 - <<'EOF'
import os, yaml

cfg = {
    "name": "Federation Forward Action",
    "source": {
        # Fuente de eventos: Kafka interno de la instancia (broker:29092)
        "type": "kafka",
        "config": {
            "connection": {
                "bootstrap": os.environ.get("KAFKA_BOOTSTRAP_SERVER", "broker:29092"),
                "schema_registry_url": os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
            },
            # Leer solo los mensajes nuevos (no reprocesar histórico al arrancar)
            "consumer_config": {"auto.offset.reset": "latest"},
            # Escuchar el topic MetadataChangeLog_Versioned_v1: se dispara cuando cambia cualquier entidad
            "topic_routes": {"ece": "MetadataChangeLog_Versioned_v1"},
        },
    },
    "action": {
        # Tipo de acción registrado en setup.py como entry point
        "type": "federation_forwarder",
        "config": {},  # La configuración se lee directamente de variables de entorno en create()
    },
}

with open("/tmp/action.yml", "w") as f:
    yaml.dump(cfg, f)
EOF

exec datahub-actions run -c /tmp/action.yml
