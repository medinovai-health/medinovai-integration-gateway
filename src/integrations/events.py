"""CloudEvents-style publishing (Kafka-compatible payload; in-memory default)."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog

E_EVENT_INGESTED = "medinovai.integration.record.ingested"
E_EVENT_FAILED = "medinovai.integration.record.failed"
E_EVENT_BATCH_DONE = "medinovai.integration.batch.completed"
E_KAFKA_BROKERS_ENV = "MOS_KAFKA_BOOTSTRAP_SERVERS"

mos_logger = structlog.get_logger()


class CloudEventPublisher:
    """Emit structured events; logs JSON when Kafka not configured."""

    def __init__(self) -> None:
        self._mos_memory: List[Dict[str, Any]] = []
        self._mos_kafka = os.environ.get(E_KAFKA_BROKERS_ENV, "").strip()

    def drain_memory(self) -> List[Dict[str, Any]]:
        mos_out = list(self._mos_memory)
        self._mos_memory.clear()
        return mos_out

    async def publish(
        self,
        mos_event_type: str,
        mos_source: str,
        mos_data: Dict[str, Any],
        mos_correlation_id: str,
        mos_tenant_id: str,
    ) -> str:
        mos_event_id = str(uuid.uuid4())
        mos_ce = {
            "specversion": "1.0",
            "type": mos_event_type,
            "source": mos_source,
            "id": mos_event_id,
            "time": datetime.now(timezone.utc).isoformat(),
            "datacontenttype": "application/json",
            "data": {
                **mos_data,
                "correlation_id": mos_correlation_id,
                "tenant_id": mos_tenant_id,
            },
        }
        if self._mos_kafka:
            # Kafka producer wiring belongs in a dedicated worker; log intent only.
            mos_logger.info(
                "cloudevent_kafka_pending",
                event_type=mos_event_type,
                correlation_id=mos_correlation_id,
                tenant_id=mos_tenant_id,
                category="business",
                phi_safe=True,
                payload_digest=len(json.dumps(mos_ce)),
            )
        else:
            mos_logger.info(
                "cloudevent_emitted",
                event_type=mos_event_type,
                correlation_id=mos_correlation_id,
                tenant_id=mos_tenant_id,
                category="business",
                phi_safe=True,
            )
            self._mos_memory.append(mos_ce)
        return mos_event_id
