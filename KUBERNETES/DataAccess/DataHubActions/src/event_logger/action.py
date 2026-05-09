import json
import logging
import os

from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext

logger = logging.getLogger(__name__)


class EventLoggerAction(Action):
    def __init__(self, output_path: str) -> None:
        self.output_path = output_path
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "EventLoggerAction":
        output_path = config_dict.get("output_path", "/data/eventLogs.mdjson")
        return cls(output_path)

    def act(self, event: EventEnvelope) -> None:
        try:
            raw = event.event.to_obj() if hasattr(event.event, "to_obj") else str(event.event)
            record = {"event_type": event.event_type, "event": raw}
            with open(self.output_path, "a") as f:
                f.write(json.dumps(record) + "\n")
        except Exception:
            logger.exception("Failed to log event")

    def close(self) -> None:
        pass
