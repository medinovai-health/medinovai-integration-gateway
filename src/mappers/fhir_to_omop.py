"""Map FHIR R4 resources to OMOP CDM v5.4-shaped rows (synthetic keys, no PHI in logs)."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


E_OMOP_MAPPING_VERSION = "2026.03.30-sprint5"


def _stable_int_id(mos_key: str, mos_mod: int = 2_147_483_647) -> int:
    mos_digest = hashlib.sha256(mos_key.encode("utf-8")).hexdigest()[:12]
    return int(mos_digest, 16) % mos_mod


def _first_coding_code(mos_codeable: Any) -> Optional[str]:
    if not isinstance(mos_codeable, dict):
        return None
    mos_codings = mos_codeable.get("coding")
    if isinstance(mos_codings, list) and mos_codings:
        mos_c = mos_codings[0]
        if isinstance(mos_c, dict):
            return str(mos_c.get("code") or mos_c.get("display") or "") or None
    return None


def _gender_concept(mos_g: Optional[str]) -> int:
    mos_map = {"male": 8507, "female": 8532, "unknown": 8551, "other": 8521}
    return mos_map.get((mos_g or "").lower(), 8551)


@dataclass
class OmopRows:
    """Container for OMOP table rows produced from one ingest unit."""

    mos_person: List[Dict[str, Any]] = field(default_factory=list)
    mos_measurement: List[Dict[str, Any]] = field(default_factory=list)
    mos_observation: List[Dict[str, Any]] = field(default_factory=list)
    mos_condition_occurrence: List[Dict[str, Any]] = field(default_factory=list)
    mos_drug_exposure: List[Dict[str, Any]] = field(default_factory=list)
    mos_visit_occurrence: List[Dict[str, Any]] = field(default_factory=list)


def map_patient_to_person(mos_res: Dict[str, Any], mos_tenant_id: str) -> Dict[str, Any]:
    """FHIR Patient → PERSON."""
    mos_pid = str(mos_res.get("id") or "")
    mos_key = f"{mos_tenant_id}|{mos_pid}"
    mos_birth = mos_res.get("birthDate")
    return {
        "person_id": _stable_int_id(mos_key),
        "gender_concept_id": _gender_concept(str(mos_res.get("gender") or "")),
        "year_of_birth": int(str(mos_birth)[:4]) if mos_birth and len(str(mos_birth)) >= 4 else None,
        "month_of_birth": int(str(mos_birth)[5:7]) if mos_birth and len(str(mos_birth)) >= 7 else None,
        "day_of_birth": int(str(mos_birth)[8:10]) if mos_birth and len(str(mos_birth)) >= 10 else None,
        "person_source_value": mos_pid or None,
        "gender_source_value": mos_res.get("gender"),
        "_mapping_version": E_OMOP_MAPPING_VERSION,
    }


def map_observation_to_measurement_or_observation(
    mos_res: Dict[str, Any],
    mos_person_id: int,
) -> Dict[str, Any]:
    """FHIR Observation → MEASUREMENT-oriented row (labs/vitals)."""
    mos_code = _first_coding_code(mos_res.get("code"))
    mos_val_qty = mos_res.get("valueQuantity") or {}
    mos_value = mos_val_qty.get("value") if isinstance(mos_val_qty, dict) else None
    mos_unit = mos_val_qty.get("unit") if isinstance(mos_val_qty, dict) else None
    mos_eff = mos_res.get("effectiveDateTime") or mos_res.get("issued")
    return {
        "measurement_id": _stable_int_id(f"m|{mos_res.get('id')}|{mos_person_id}"),
        "person_id": mos_person_id,
        "measurement_concept_id": 0,
        "measurement_source_value": mos_code,
        "value_as_number": float(mos_value) if mos_value is not None else None,
        "unit_source_value": mos_unit,
        "measurement_date": mos_eff,
        "measurement_datetime": mos_eff,
        "_target_table": "MEASUREMENT",
        "_mapping_version": E_OMOP_MAPPING_VERSION,
    }


def map_condition_to_condition_occurrence(
    mos_res: Dict[str, Any],
    mos_person_id: int,
) -> Dict[str, Any]:
    """FHIR Condition → CONDITION_OCCURRENCE."""
    mos_code = _first_coding_code(mos_res.get("code"))
    mos_onset = mos_res.get("onsetDateTime") or mos_res.get("recordedDate")
    return {
        "condition_occurrence_id": _stable_int_id(f"c|{mos_res.get('id')}|{mos_person_id}"),
        "person_id": mos_person_id,
        "condition_concept_id": 0,
        "condition_start_date": mos_onset,
        "condition_start_datetime": mos_onset,
        "condition_source_value": mos_code,
        "_mapping_version": E_OMOP_MAPPING_VERSION,
    }


def map_medication_to_drug_exposure(
    mos_res: Dict[str, Any],
    mos_person_id: int,
) -> Dict[str, Any]:
    """FHIR MedicationStatement/MedicationRequest → DRUG_EXPOSURE (simplified)."""
    mos_med = mos_res.get("medicationCodeableConcept") or mos_res.get("medicationReference")
    mos_code = _first_coding_code(mos_med) if isinstance(mos_med, dict) else None
    mos_start = mos_res.get("effectiveDateTime") or mos_res.get("authoredOn")
    return {
        "drug_exposure_id": _stable_int_id(f"d|{mos_res.get('id')}|{mos_person_id}"),
        "person_id": mos_person_id,
        "drug_concept_id": 0,
        "drug_exposure_start_date": mos_start,
        "drug_exposure_start_datetime": mos_start,
        "drug_source_value": mos_code,
        "_mapping_version": E_OMOP_MAPPING_VERSION,
    }


def map_encounter_to_visit_occurrence(
    mos_res: Dict[str, Any],
    mos_person_id: int,
) -> Dict[str, Any]:
    """FHIR Encounter → VISIT_OCCURRENCE."""
    mos_type_code = _first_coding_code(mos_res.get("type"))
    mos_start = mos_res.get("period", {}).get("start") if isinstance(mos_res.get("period"), dict) else None
    mos_end = mos_res.get("period", {}).get("end") if isinstance(mos_res.get("period"), dict) else None
    return {
        "visit_occurrence_id": _stable_int_id(f"v|{mos_res.get('id')}|{mos_person_id}"),
        "person_id": mos_person_id,
        "visit_concept_id": 9201,
        "visit_start_date": mos_start,
        "visit_start_datetime": mos_start,
        "visit_end_date": mos_end,
        "visit_end_datetime": mos_end,
        "visit_source_value": mos_type_code,
        "_mapping_version": E_OMOP_MAPPING_VERSION,
    }


def map_fhir_resource_to_omop(
    mos_res: Dict[str, Any],
    mos_tenant_id: str,
    mos_default_person_id: Optional[int] = None,
) -> OmopRows:
    """Map a single FHIR resource dict to OMOP rows."""
    mos_rows = OmopRows()
    mos_rt = str(mos_res.get("resourceType") or "")
    mos_person_id: Optional[int] = mos_default_person_id
    if mos_rt == "Patient":
        mos_rows.mos_person.append(map_patient_to_person(mos_res, mos_tenant_id))
        return mos_rows
    if mos_person_id is None:
        mos_person_id = _stable_int_id(f"{mos_tenant_id}|subj|{mos_res.get('subject', {}).get('reference', '')}")
    if mos_rt == "Observation":
        mos_rows.mos_measurement.append(
            map_observation_to_measurement_or_observation(mos_res, mos_person_id)
        )
    elif mos_rt == "Condition":
        mos_rows.mos_condition_occurrence.append(
            map_condition_to_condition_occurrence(mos_res, mos_person_id)
        )
    elif mos_rt in {"MedicationStatement", "MedicationRequest", "MedicationAdministration"}:
        mos_rows.mos_drug_exposure.append(
            map_medication_to_drug_exposure(mos_res, mos_person_id)
        )
    elif mos_rt == "Encounter":
        mos_rows.mos_visit_occurrence.append(
            map_encounter_to_visit_occurrence(mos_res, mos_person_id)
        )
    return mos_rows


def map_fhir_bundle_entries_to_omop(
    mos_entries: List[Dict[str, Any]],
    mos_tenant_id: str,
) -> OmopRows:
    """Map ordered Bundle entries; Patient resources establish person ids for subsequent rows."""
    mos_acc = OmopRows()
    mos_patient_ids: Dict[str, int] = {}
    for mos_res in mos_entries:
        mos_rt = str(mos_res.get("resourceType") or "")
        if mos_rt == "Patient":
            mos_p = map_patient_to_person(mos_res, mos_tenant_id)
            mos_acc.mos_person.append(mos_p)
            mos_key = str(mos_res.get("id") or "")
            if mos_key:
                mos_patient_ids[mos_key] = int(mos_p["person_id"])
    for mos_res in mos_entries:
        mos_rt = str(mos_res.get("resourceType") or "")
        if mos_rt == "Patient":
            continue
        mos_ref = ""
        mos_subj = mos_res.get("subject")
        if isinstance(mos_subj, dict):
            mos_ref = str(mos_subj.get("reference") or "")
        mos_pid_key = mos_ref.split("/")[-1] if "Patient/" in mos_ref else ""
        mos_default = mos_patient_ids.get(mos_pid_key) if mos_pid_key else None
        mos_part = map_fhir_resource_to_omop(mos_res, mos_tenant_id, mos_default_person_id=mos_default)
        mos_acc.mos_measurement.extend(mos_part.mos_measurement)
        mos_acc.mos_observation.extend(mos_part.mos_observation)
        mos_acc.mos_condition_occurrence.extend(mos_part.mos_condition_occurrence)
        mos_acc.mos_drug_exposure.extend(mos_part.mos_drug_exposure)
        mos_acc.mos_visit_occurrence.extend(mos_part.mos_visit_occurrence)
    return mos_acc
