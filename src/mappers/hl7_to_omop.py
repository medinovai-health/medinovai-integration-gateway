"""Map parsed HL7v2 messages to OMOP-shaped rows (ADT/ORU/ORM/MDM)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from mappers.fhir_to_omop import E_OMOP_MAPPING_VERSION, OmopRows, _stable_int_id

from parsers.hl7_parser import ParsedHl7Message


def _field(mos_seg: Optional[Any], mos_idx: int) -> Optional[str]:
    if mos_seg is None:
        return None
    mos_fields = getattr(mos_seg, "mos_fields", []) or []
    if mos_idx < 0 or mos_idx >= len(mos_fields):
        return None
    return mos_fields[mos_idx]


def map_parsed_hl7_to_omop(mos_msg: ParsedHl7Message, mos_tenant_id: str) -> OmopRows:
    """Build OMOP preview rows from PID/PV1/OBR/OBX segments (no PHI in audit logs)."""
    mos_rows = OmopRows()
    mos_pid = mos_msg.mos_segments.get("PID", [None])[0]
    mos_pv1 = mos_msg.mos_segments.get("PV1", [None])[0]
    mos_patient_key = _field(mos_pid, 2) or "unknown"
    mos_person_id = _stable_int_id(f"{mos_tenant_id}|hl7|{mos_patient_key}")
    mos_dob = _field(mos_pid, 6)
    mos_sex = _field(mos_pid, 7)
    mos_rows.mos_person.append(
        {
            "person_id": mos_person_id,
            "gender_concept_id": 8551,
            "year_of_birth": int(str(mos_dob)[:4]) if mos_dob and len(str(mos_dob)) >= 4 else None,
            "person_source_value": str(mos_patient_key)[:128],
            "gender_source_value": mos_sex,
            "_mapping_version": E_OMOP_MAPPING_VERSION,
        }
    )
    if mos_pv1:
        mos_admit = _field(mos_pv1, 44)
        mos_rows.mos_visit_occurrence.append(
            {
                "visit_occurrence_id": _stable_int_id(f"{mos_person_id}|pv1"),
                "person_id": mos_person_id,
                "visit_concept_id": 9201,
                "visit_start_datetime": mos_admit,
                "visit_source_value": _field(mos_pv1, 2),
                "_mapping_version": E_OMOP_MAPPING_VERSION,
            }
        )
    for mos_obr in mos_msg.mos_segments.get("OBR", []) or []:
        mos_rows.mos_measurement.append(
            {
                "measurement_id": _stable_int_id(f"{mos_person_id}|obr|{_field(mos_obr, 3) or ''}"),
                "person_id": mos_person_id,
                "measurement_concept_id": 0,
                "measurement_source_value": _field(mos_obr, 4),
                "measurement_datetime": _field(mos_obr, 7),
                "_target_table": "MEASUREMENT",
                "_mapping_version": E_OMOP_MAPPING_VERSION,
            }
        )
    for mos_obx in mos_msg.mos_segments.get("OBX", []) or []:
        mos_val = _field(mos_obx, 5)
        mos_rows.mos_observation.append(
            {
                "observation_id": _stable_int_id(f"{mos_person_id}|obx|{_field(mos_obx, 3) or ''}"),
                "person_id": mos_person_id,
                "observation_concept_id": 0,
                "observation_source_value": _field(mos_obx, 3),
                "value_as_string": mos_val,
                "observation_datetime": _field(mos_obx, 14),
                "_target_table": "OBSERVATION",
                "_mapping_version": E_OMOP_MAPPING_VERSION,
            }
        )
    return mos_rows
