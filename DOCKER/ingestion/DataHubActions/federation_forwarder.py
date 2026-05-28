# Acción personalizada de DataHub Actions que escucha eventos EntityChangeEvent_v1
# y dispara una ingestión completa hacia la instancia de federación.
import logging
import os
import subprocess
import tempfile
import threading
import time

import yaml
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext

logger = logging.getLogger(__name__)

# Patrones de URN que se deben ignorar (metadatos internos de DataHub)
DENY_PATTERNS = [
    "^urn:li:dataHubPageModule.*",
    "^urn:li:dataHubIngestionSource.*",
    "^urn:li:dataHubExecutionRequest.*",
    "^urn:li:dataHubSecret.*",
    "^urn:li:dataHubAccessToken.*",
    "^urn:li:inviteToken.*",
    "^urn:li:globalSettings.*",
    "^urn:li:dataHubStepState.*",
]


class FederationForwardAction(Action):
    def __init__(self, recipe: dict, debounce_seconds: int):
        self._recipe = recipe
        self._debounce_s = debounce_seconds
        # Lock para acceso seguro entre el hilo principal (act) y el worker
        self._lock = threading.Lock()
        self._last_event_time = 0.0
        # _pending indica que se ha recibido un evento y hay que ejecutar ingestión
        self._pending = False
        # Hilo worker en segundo plano que espera el debounce y lanza la ingestión
        threading.Thread(target=self._worker_loop, daemon=True).start()

    @classmethod
    def create(cls, config: dict, ctx: PipelineContext) -> "FederationForwardAction":
        # Construye la receta de ingestión equivalente a AEMET_FEDE.yaml usando variables de entorno.
        # Se accede a los servicios de la instancia origen a través de los puertos expuestos al host
        # (host.docker.internal) para mantener el aislamiento entre instancias Docker.
        instance = os.environ.get("INSTANCE", "unknown")
        recipe = {
            "pipeline_name": f"event_fede_{instance}",
            # API de DataHub origen (instancia 1 o 2) para gestión de estado de ingestión
            "datahub_api": {
                "server": os.environ["SOURCE_GMS_URL"],
                "token": os.environ.get("DATAHUB_TOKEN", ""),
            },
            "source": {
                "type": "datahub",  # Lee entidades desde otra instancia DataHub
                "config": {
                    "include_all_versions": False,
                    # Conexión a MySQL de la instancia origen para ingestión con estado
                    "database_connection": {
                        "scheme": "mysql+pymysql",
                        "host_port": os.environ["SOURCE_MYSQL_HOST_PORT"],
                        "username": os.environ.get("MYSQL_USER", "datahub"),
                        "password": os.environ.get("MYSQL_PASSWORD", "datahub"),
                        "database": "datahub",
                    },
                    # Conexión a Kafka de la instancia origen para leer el MCL
                    "kafka_connection": {
                        "bootstrap": os.environ["SOURCE_KAFKA_BOOTSTRAP"],
                        "schema_registry_url": os.environ["SOURCE_SCHEMA_REGISTRY_URL"],
                    },
                    # Ingestión con estado: permite ingerir solo los cambios desde la última ejecución
                    "stateful_ingestion": {"enabled": True, "ignore_old_state": False},
                    "urn_pattern": {"deny": DENY_PATTERNS},
                },
            },
            "flags": {"set_system_metadata": False},
            # Destino: instancia de federación (instancia 3)
            "sink": {
                "type": "datahub-rest",
                "config": {
                    "server": os.environ["FEDERATION_GMS_URL"],
                    "token": os.environ.get("DATAHUB_TOKEN", ""),
                },
            },
        }
        debounce = int(os.environ.get("DEBOUNCE_SECONDS", "30"))
        return cls(recipe=recipe, debounce_seconds=debounce)

    def act(self, event: EventEnvelope) -> None:
        # Se llama por cada evento EntityChangeEvent_v1 recibido.
        # Solo actualiza el timestamp y marca que hay una ingestión pendiente.
        # El worker hilo decide cuándo ejecutar la ingestión real.
        with self._lock:
            self._last_event_time = time.monotonic()
            self._pending = True

    def _worker_loop(self) -> None:
        # Bucle que revisa cada segundo si hay ingestión pendiente y si el período
        # de debounce ha expirado (no se han recibido eventos en los últimos DEBOUNCE_SECONDS).
        while True:
            time.sleep(1)
            with self._lock:
                if not self._pending:
                    continue
                # Esperar a que no lleguen más eventos antes de lanzar ingestión
                if time.monotonic() - self._last_event_time < self._debounce_s:
                    continue
                self._pending = False
            # Ejecutar ingestión fuera del lock para no bloquear act()
            self._run_ingestion()
            # Si llegaron más eventos durante la ingestión, _pending será True
            # y el bucle lanzará otra ingestión en la siguiente iteración (comportamiento de cola)

    def _run_ingestion(self) -> None:
        # Escribe la receta a un archivo temporal y ejecuta `datahub ingest`
        logger.info("Disparando ingestión hacia la federación...")
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(self._recipe, f)
            path = f.name
        try:
            result = subprocess.run(
                ["datahub", "ingest", "-c", path],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                logger.error("Ingestión fallida:\n%s", result.stderr)
            else:
                logger.info("Ingestión completada correctamente.")
        finally:
            os.unlink(path)  # Eliminar el archivo temporal

    def close(self) -> None:
        pass
