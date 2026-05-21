import json
import logging
import re
import urllib.parse
from datetime import datetime, timezone
from typing import Dict, Optional

import requests
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext

logger = logging.getLogger(__name__)

_SCHEMA_FIELD_RE = re.compile(r"\.\[type=\w+\]\.(\w+)\)$")
_DATASET_PLATFORM_RE = re.compile(r"urn:li:dataset:\(urn:li:dataPlatform:([^,]+),")


def _encode_id(urn: str) -> str:
    return urllib.parse.quote(urn, safe="")


def _iso_from_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).date().isoformat()


def _extract_platform(dataset_urn: str) -> Optional[str]:
    m = _DATASET_PLATFORM_RE.match(dataset_urn)
    return m.group(1) if m else None


def _extract_field_name(modifier: str) -> str:
    m = _SCHEMA_FIELD_RE.search(modifier)
    return m.group(1) if m else modifier.split(".")[-1].rstrip(")")


class DcatForwardAction(Action):
    """
    DataHub Action that translates EntityChangeEvent_v1 events into DCAT3 REST API calls.

    Mapping:
      container LIFECYCLE CREATE/REINSTATE  → POST /catalogs           (409 → GET+PUT)
      container LIFECYCLE REMOVE            → DELETE /catalogs/{id}
      dataset   LIFECYCLE CREATE/REINSTATE  → POST /catalogs/{platform}/datasets
                                              (404 fallback → /datasets, 409 → GET+PUT)
      dataset   LIFECYCLE REMOVE            → DELETE /datasets/{id}
      dataset   OWNER ADD/MODIFY            → GET /datasets/{id} + PUT (update publisher)
      dataset   TECHNICAL_SCHEMA ADD        → GET /datasets/{id} + PUT (append field to description)
    """

    def __init__(self, server_url: str) -> None:
        self.server_url = server_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "DcatForwardAction":
        # server_url = config_dict.get("server_url", "http://localhost:8080") # Runs in a pod, can not assume localhost is the DCAT server. 
        server_url = config_dict.get("server_url", "http://10.0.2.15:8080")
        return cls(server_url)

    def act(self, event: EventEnvelope) -> None:
        if event.event_type != "EntityChangeEvent_v1":
            return
        try:
            raw = event.event.to_obj() if hasattr(event.event, "to_obj") else event.event
            if isinstance(raw, str):
                raw = json.loads(raw)
            self._dispatch(raw)
        except Exception:
            logger.exception("Failed to forward event to DCAT server")

    def _dispatch(self, event: dict) -> None:
        entity_type = event.get("entityType", "")
        urn = event.get("entityUrn", "")
        category = event.get("category", "")
        operation = event.get("operation", "")
        audit = event.get("auditStamp", {})
        ts_ms = audit.get("time", 0)
        actor = audit.get("actor", "")
        modifier = event.get("modifier", "")

        if entity_type == "dataset":
            self._handle_dataset(urn, category, operation, ts_ms, actor, modifier)
        elif entity_type == "container":
            self._handle_container(urn, category, operation, ts_ms, actor)

    # --- Dataset (→ DCAT Dataset) ---

    def _handle_dataset(
        self,
        urn: str,
        category: str,
        operation: str,
        ts_ms: int,
        actor: str,
        modifier: str,
    ) -> None:
        encoded_id = _encode_id(urn)

        if category == "LIFECYCLE":
            if operation in ("CREATE", "REINSTATE"):
                self._dataset_upsert(urn, encoded_id, ts_ms)
            elif operation == "REMOVE":
                self._req("DELETE", "/datasets/{}".format(encoded_id), None)

        elif category == "OWNER" and operation in ("ADD", "MODIFY"):
            self._dataset_patch(encoded_id, {"dcterms_publisher": actor})

        elif category == "TECHNICAL_SCHEMA" and operation == "ADD":
            field_name = _extract_field_name(modifier)
            self._dataset_append_description(encoded_id, field_name)

    def _dataset_upsert(self, urn: str, encoded_id: str, ts_ms: int) -> None:
        date_str = _iso_from_ms(ts_ms)
        payload: Dict = {
            "dcterms_identifier": urn,
            "dcterms_title": urn,
            "dcterms_issued": date_str,
        }

        platform = _extract_platform(urn)
        resp = None
        if platform:
            catalog_id = _encode_id("urn:li:dataPlatform:{}".format(platform))
            resp = self._req("POST", "/catalogs/{}/datasets".format(catalog_id), payload)
            if resp is not None and resp.status_code == 404:
                resp = self._req("POST", "/datasets", payload)
        else:
            resp = self._req("POST", "/datasets", payload)

        if resp is not None and resp.status_code == 409:
            existing = self._get_json("/datasets/{}".format(encoded_id)) or {}
            existing.update(payload)
            self._req("PUT", "/datasets/{}".format(encoded_id), existing)

    def _dataset_patch(self, encoded_id: str, updates: dict) -> None:
        existing = self._get_json("/datasets/{}".format(encoded_id))
        if existing is None:
            logger.warning("Dataset %s not found for patch", encoded_id)
            return
        existing.update(updates)
        self._req("PUT", "/datasets/{}".format(encoded_id), existing)

    def _dataset_append_description(self, encoded_id: str, field_name: str) -> None:
        existing = self._get_json("/datasets/{}".format(encoded_id))
        if existing is None:
            logger.warning("Dataset %s not found for schema update", encoded_id)
            return
        current = existing.get("dcterms_description") or ""
        existing["dcterms_description"] = (current + "; " + field_name).lstrip("; ")
        self._req("PUT", "/datasets/{}".format(encoded_id), existing)

    # --- Container (→ DCAT Catalog) ---

    def _handle_container(
        self, urn: str, category: str, operation: str, ts_ms: int, actor: str
    ) -> None:
        encoded_id = _encode_id(urn)
        date_str = _iso_from_ms(ts_ms)
        payload: Dict = {
            "dcterms_identifier": urn,
            "dcterms_title": "Container {}".format(urn[-8:]),
            "dcterms_issued": date_str,
            "dcterms_creator": actor,
        }

        if category != "LIFECYCLE":
            return

        if operation in ("CREATE", "REINSTATE"):
            resp = self._req("POST", "/catalogs", payload)
            if resp is not None and resp.status_code == 409:
                existing = self._get_json("/catalogs/{}".format(encoded_id)) or {}
                existing.update(payload)
                self._req("PUT", "/catalogs/{}".format(encoded_id), existing)
        elif operation == "REMOVE":
            self._req("DELETE", "/catalogs/{}".format(encoded_id), None)

    # --- HTTP helpers ---

    def _req(self, method: str, path: str, payload: Optional[dict]) -> Optional[requests.Response]:
        url = "{}{}".format(self.server_url, path)
        try:
            if method == "DELETE":
                resp = self.session.delete(url, timeout=10)
            elif method == "GET":
                resp = self.session.get(url, timeout=10)
            else:
                resp = self.session.request(method, url, json=payload, timeout=10)

            if resp.status_code >= 500:
                logger.error("%s %s → %d: %s", method, path, resp.status_code, resp.text[:200])
            elif resp.status_code >= 400 and resp.status_code not in (404, 409):
                logger.warning("%s %s → %d: %s", method, path, resp.status_code, resp.text[:200])
            else:
                logger.debug("%s %s → %d", method, path, resp.status_code)
            return resp
        except requests.RequestException:
            logger.exception("%s %s failed", method, path)
            return None

    def _get_json(self, path: str) -> Optional[dict]:
        resp = self._req("GET", path, None)
        if resp is not None and resp.status_code == 200:
            try:
                return resp.json()
            except ValueError:
                logger.warning("GET %s returned non-JSON body", path)
        return None

    def close(self) -> None:
        self.session.close()
