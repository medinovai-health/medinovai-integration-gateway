"""Orchestrate parse → PHI scan → EPG → OMOP → CloudEvents with DLQ on failure."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import structlog

from integrations.dlq import DeadLetterQueue
from integrations.epg_client import EpgMtlsClient
from integrations.events import CloudEventPublisher, E_EVENT_BATCH_DONE, E_EVENT_FAILED, E_EVENT_INGESTED
from integrations.omop_lakehouse import OmopLakehouseClient
from jobs import IngestJobState, IngestJobStore
from mappers.fhir_to_omop import map_fhir_bundle_entries_to_omop
from mappers.hl7_to_omop import map_parsed_hl7_to_omop
from parsers.fhir_parser import FhirParseError, parse_fhir_bundle_bytes
from parsers.hl7_parser import Hl7ParseError, parse_hl7_message
from phi_guard import scan_payload_for_phi_patterns

mos_logger = structlog.get_logger()


def _omop_rows_to_dict(mos_rows: Any) -> Dict[str, Any]:
    return {
        "person": list(mos_rows.mos_person),
        "measurement": list(mos_rows.mos_measurement),
        "observation": list(mos_rows.mos_observation),
        "condition_occurrence": list(mos_rows.mos_condition_occurrence),
        "drug_exposure": list(mos_rows.mos_drug_exposure),
        "visit_occurrence": list(mos_rows.mos_visit_occurrence),
    }


class IngestOrchestrator:
    """Wires parsers, mappers, and outbound clients."""

    def __init__(self) -> None:
        self._mos_epg = EpgMtlsClient()
        self._mos_omop = OmopLakehouseClient()
        self._mos_bus = CloudEventPublisher()
        self._mos_dlq = DeadLetterQueue()
        self._mos_jobs = IngestJobStore()

    @property
    def jobs(self) -> IngestJobStore:
        return self._mos_jobs

    @property
    def dlq(self) -> DeadLetterQueue:
        return self._mos_dlq

    @property
    def events(self) -> CloudEventPublisher:
        return self._mos_bus

    async def ingest_fhir_bundle(
        self,
        mos_body: bytes,
        mos_content_type: Optional[str],
        mos_tenant_id: str,
        mos_correlation_id: str,
    ) -> Dict[str, Any]:
        mos_text = mos_body.decode("utf-8", errors="replace")
        mos_phi = scan_payload_for_phi_patterns(mos_text)
        if mos_phi.mos_any_hit:
            mos_logger.warning(
                "phi_pattern_hits",
                correlation_id=mos_correlation_id,
                tenant_id=mos_tenant_id,
                category="SECURITY",
                phi_safe=True,
                ssn_like=mos_phi.mos_ssn_like_count,
                email_like=mos_phi.mos_email_like_count,
            )
        try:
            mos_parsed = parse_fhir_bundle_bytes(mos_body, mos_content_type)
        except FhirParseError as mos_exc:
            await self._mos_bus.publish(
                E_EVENT_FAILED,
                "integration-gateway",
                {"error": "fhir_parse", "code": str(mos_exc)},
                mos_correlation_id,
                mos_tenant_id,
            )
            self._mos_dlq.push(
                {
                    "kind": "fhir",
                    "error": str(mos_exc),
                    "correlation_id": mos_correlation_id,
                    "tenant_id": mos_tenant_id,
                }
            )
            raise
        mos_rows = map_fhir_bundle_entries_to_omop(mos_parsed.mos_entries, mos_tenant_id)
        mos_summary = {
            "bundle_type": mos_parsed.mos_bundle_type,
            "resource_counts": mos_parsed.mos_resource_type_counts,
            "batch_semantics": mos_parsed.mos_is_batch_semantics,
        }
        await self._mos_epg.forward_for_privacy_review(mos_summary, mos_correlation_id, mos_tenant_id)
        await self._mos_omop.write_omop_batch(_omop_rows_to_dict(mos_rows), mos_correlation_id, mos_tenant_id)
        await self._mos_bus.publish(
            E_EVENT_INGESTED,
            "integration-gateway",
            {"format": "fhir", "summary": mos_summary},
            mos_correlation_id,
            mos_tenant_id,
        )
        return {
            "accepted": True,
            "format": "fhir",
            "summary": mos_summary,
            "omop_counts": {k: len(v) for k, v in _omop_rows_to_dict(mos_rows).items()},
        }

    async def ingest_hl7(
        self,
        mos_body: bytes,
        mos_tenant_id: str,
        mos_correlation_id: str,
    ) -> Dict[str, Any]:
        mos_text = mos_body.decode("utf-8", errors="replace")
        mos_phi = scan_payload_for_phi_patterns(mos_text)
        if mos_phi.mos_any_hit:
            mos_logger.warning(
                "phi_pattern_hits",
                correlation_id=mos_correlation_id,
                tenant_id=mos_tenant_id,
                category="SECURITY",
                phi_safe=True,
                ssn_like=mos_phi.mos_ssn_like_count,
                email_like=mos_phi.mos_email_like_count,
            )
        try:
            mos_msg = parse_hl7_message(mos_text)
        except Hl7ParseError as mos_exc:
            await self._mos_bus.publish(
                E_EVENT_FAILED,
                "integration-gateway",
                {"error": "hl7_parse", "code": str(mos_exc)},
                mos_correlation_id,
                mos_tenant_id,
            )
            self._mos_dlq.push(
                {
                    "kind": "hl7",
                    "error": str(mos_exc),
                    "correlation_id": mos_correlation_id,
                    "tenant_id": mos_tenant_id,
                }
            )
            raise
        mos_rows = map_parsed_hl7_to_omop(mos_msg, mos_tenant_id)
        mos_summary = {
            "message_type": mos_msg.mos_message_type,
            "trigger": mos_msg.mos_trigger_event,
            "segments": list(mos_msg.mos_segments.keys()),
        }
        await self._mos_epg.forward_for_privacy_review(mos_summary, mos_correlation_id, mos_tenant_id)
        await self._mos_omop.write_omop_batch(_omop_rows_to_dict(mos_rows), mos_correlation_id, mos_tenant_id)
        await self._mos_bus.publish(
            E_EVENT_INGESTED,
            "integration-gateway",
            {"format": "hl7v2", "summary": mos_summary},
            mos_correlation_id,
            mos_tenant_id,
        )
        return {
            "accepted": True,
            "format": "hl7v2",
            "summary": mos_summary,
            "omop_counts": {k: len(v) for k, v in _omop_rows_to_dict(mos_rows).items()},
        }

    async def ingest_batch(
        self,
        mos_items: List[Dict[str, Any]],
        mos_tenant_id: str,
        mos_correlation_id: str,
    ) -> Dict[str, Any]:
        mos_job_id = await self._mos_jobs.create_job({"item_count": len(mos_items)})
        await self._mos_jobs.update_job(mos_job_id, IngestJobState.PROCESSING)
        mos_ok = 0
        mos_fail = 0
        for mos_it in mos_items:
            mos_kind = str(mos_it.get("type") or "").lower()
            try:
                if mos_kind == "fhir":
                    mos_ct = str(mos_it.get("contentType") or "application/fhir+json")
                    mos_raw = mos_it.get("body")
                    mos_bytes = (
                        mos_raw.encode("utf-8")
                        if isinstance(mos_raw, str)
                        else bytes(mos_raw)
                    )
                    await self.ingest_fhir_bundle(mos_bytes, mos_ct, mos_tenant_id, mos_correlation_id)
                elif mos_kind == "hl7":
                    mos_raw = mos_it.get("body", "")
                    mos_bytes = mos_raw.encode("utf-8") if isinstance(mos_raw, str) else bytes(mos_raw)
                    await self.ingest_hl7(mos_bytes, mos_tenant_id, mos_correlation_id)
                else:
                    raise ValueError("unknown_item_type")
                mos_ok += 1
            except Exception:  # noqa: BLE001
                mos_fail += 1
        await self._mos_jobs.update_job(
            mos_job_id,
            IngestJobState.COMPLETED,
            {"accepted": mos_ok, "failed": mos_fail},
        )
        await self._mos_bus.publish(
            E_EVENT_BATCH_DONE,
            "integration-gateway",
            {"job_id": mos_job_id, "accepted": mos_ok, "failed": mos_fail},
            mos_correlation_id,
            mos_tenant_id,
        )
        return {"job_id": mos_job_id, "accepted": mos_ok, "failed": mos_fail}
