"""Tests for FHIR/HL7 parsers and ingest HTTP API."""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

pytest.importorskip("httpx")

from parsers.fhir_parser import FhirParseError, parse_fhir_bundle_bytes
from parsers.hl7_parser import Hl7ParseError, parse_hl7_message


def _minimal_fhir_bundle() -> bytes:
    mos_bundle = {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [
            {
                "resource": {
                    "resourceType": "Patient",
                    "id": "p1",
                    "gender": "male",
                    "birthDate": "1990-01-01",
                }
            },
            {
                "resource": {
                    "resourceType": "Observation",
                    "id": "o1",
                    "status": "final",
                    "code": {
                        "coding": [{"system": "http://loinc.org", "code": "8867-4"}],
                    },
                    "subject": {"reference": "Patient/p1"},
                    "valueQuantity": {"value": 72, "unit": "bpm"},
                },
            },
        ],
    }
    return json.dumps(mos_bundle).encode("utf-8")


def test_parse_fhir_bundle_counts_resources() -> None:
    mos_parsed = parse_fhir_bundle_bytes(_minimal_fhir_bundle(), "application/fhir+json")
    assert mos_parsed.mos_resource_type_counts["Patient"] == 1
    assert mos_parsed.mos_resource_type_counts["Observation"] == 1


def test_parse_fhir_invalid_raises() -> None:
    with pytest.raises(FhirParseError):
        parse_fhir_bundle_bytes(b"{}", "application/json")


def test_parse_hl7_adt() -> None:
    mos_msg = (
        "MSH|^~\\&|SAPP|SFAC|RAPP|RFAC|202401011200||ADT^A01|MSG001|P|2.5\r"
        "PID|1||12345^^^MRN||Doe^John||19800101|M\r"
        "PV1|1|O|WARD^101^01|||||||||||||||||SELF|||||||||||||||||||||||||202401011200"
    )
    mos_parsed = parse_hl7_message(mos_msg)
    assert mos_parsed.mos_message_type == "ADT"
    assert mos_parsed.mos_trigger_event == "A01"


def test_parse_hl7_empty_raises() -> None:
    with pytest.raises(Hl7ParseError):
        parse_hl7_message("")


def test_post_ingest_fhir_accepted(mos_client: TestClient) -> None:
    mos_resp = mos_client.post(
        "/api/v1/ingest/fhir",
        content=_minimal_fhir_bundle(),
        headers={
            "Content-Type": "application/fhir+json",
            "X-Tenant-ID": "t-demo",
            "X-Correlation-ID": "corr-test-1",
        },
    )
    assert mos_resp.status_code == 202
    mos_body = mos_resp.json()
    assert mos_body["accepted"] is True
    assert mos_body["correlation_id"] == "corr-test-1"
    assert mos_body["format"] == "fhir"


def test_post_ingest_hl7_accepted(mos_client: TestClient) -> None:
    mos_msg = (
        "MSH|^~\\&|SAPP|SFAC|RAPP|RFAC|202401011200||ORU^R01|MSG002|P|2.5\r"
        "PID|1||999^^^MRN||Test^User||19900101|F\r"
        "OBR|1|||1234-5^Panel^LN|||202401011200\r"
        "OBX|1|NM|8867-4^Heart rate^LN||72|bpm|||||F|||202401011200\r"
    )
    mos_resp = mos_client.post(
        "/api/v1/ingest/hl7",
        content=mos_msg.encode("utf-8"),
        headers={"Content-Type": "text/plain", "X-Tenant-ID": "t-demo"},
    )
    assert mos_resp.status_code == 202
    assert mos_resp.json()["format"] == "hl7v2"


def test_post_ingest_batch_returns_job(mos_client: TestClient) -> None:
    mos_payload = {
        "items": [
            {
                "type": "fhir",
                "body": _minimal_fhir_bundle().decode("utf-8"),
                "contentType": "application/fhir+json",
            }
        ]
    }
    mos_resp = mos_client.post("/api/v1/ingest/batch", json=mos_payload)
    assert mos_resp.status_code == 202
    mos_body = mos_resp.json()
    assert "job_id" in mos_body
    mos_status = mos_client.get(f"/api/v1/ingest/status/{mos_body['job_id']}")
    assert mos_status.status_code == 200
    assert mos_status.json()["state"] in {"pending", "processing", "completed", "failed"}


def test_ingest_status_unknown_404(mos_client: TestClient) -> None:
    mos_resp = mos_client.get("/api/v1/ingest/status/00000000-0000-0000-0000-000000000099")
    assert mos_resp.status_code == 404
