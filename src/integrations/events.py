"""CloudEvents-style publishing (Kafka-compatible payload; structured logs only)."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

import structlog

E_EVENT_INGESTED = "medinovai.integration.record.ingested"
E_EVENT_FAILED = "medinovai.integration.record.failed"
E_EVENT_BATCH_DONE = "medinovai.integration.batch.completed"
E_KAFKA_BROKERS_ENV = "MOS_KAFKA_BOOTSTRAP_SERVERS"

mos_logger = structlog.get_logger()


class CloudEventPublisher:
    """Emit structured events via logs (or Kafka intent when brokers are set)."""

    def __init__(self) -> None:
        self._mos_kafka = os.environ.get(E_KAFKA_BROKERS_ENV, "").strip()

    def drain_memory(self) -> List[Dict[str, Any]]:
        """Backward-compatible no-op; events are not buffered in process memory.

        Returns:
            Empty list. Use log aggregation or Kafka for durable event history.
        """
        return []

    async def publish(
        self,
        mos_event_type: str,
        mos_source: str,
        mos_data: Dict[str, Any],
        mos_correlation_id: str,
        mos_tenant_id: str,
    ) -> str:
        """Build a CloudEvent envelope and record it (log or Kafka-pending).

        Args:
            mos_event_type: CloudEvents ``type``.
            mos_source: Provenance URI or logical source id.
            mos_data: Event payload (phi_safe fields only).
            mos_correlation_id: Request correlation id.
            mos_tenant_id: Tenant scope.

        Returns:
            Generated event id (UUID string).
        """
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
        return mos_event_id
