"""FHIR R4 Bundle parsing (JSON and limited XML) with structural validation."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import xmltodict
from fhir.resources.bundle import Bundle
from pydantic import ValidationError

E_FHIR_JSON_MEDIA = "application/fhir+json"
E_FHIR_XML_MEDIA = "application/fhir+xml"
E_BATCH_BUNDLE_TYPES = frozenset({"batch", "transaction", "batch-response", "transaction-response"})


class FhirParseError(ValueError):
    """Raised when a payload cannot be parsed or validated as a FHIR R4 Bundle."""


@dataclass
class ParsedFhirBundle:
    """Validated FHIR R4 Bundle with extracted resource summaries (no raw PHI in logs)."""

    mos_bundle_type: str
    mos_resource_type_counts: Dict[str, int]
    mos_entries: List[Dict[str, Any]] = field(default_factory=list)
    mos_raw_resource_count: int = 0
    mos_is_batch_semantics: bool = False


def _flatten_fhir_primitive(node: Any) -> Any:
    """Normalize FHIR JSON/XML primitive dicts like {'value': x} to x."""
    if isinstance(node, dict) and set(node.keys()) <= {"value", "id", "extension"}:
        if "value" in node:
            return node["value"]
    return node


def _normalize_fhir_dict(mos_obj: Any) -> Any:
    """Recursively normalize common FHIR element shapes for Pydantic validation."""
    if isinstance(mos_obj, list):
        return [_normalize_fhir_dict(mos_x) for mos_x in mos_obj]
    if not isinstance(mos_obj, dict):
        return mos_obj
    mos_out: Dict[str, Any] = {}
    for mos_k, mos_v in mos_obj.items():
        mos_key = mos_k.split(":")[-1]
        if mos_key == "resource" and isinstance(mos_v, dict):
            mos_inner = _normalize_fhir_dict(mos_v)
            mos_out[mos_key] = mos_inner
            continue
        mos_out[mos_key] = _normalize_fhir_dict(mos_v)
    return mos_out


def _xml_bundle_to_jsonish(mos_root: Dict[str, Any]) -> Dict[str, Any]:
    """Best-effort convert xmltodict Bundle subtree to FHIR JSON-like dict."""
    if not mos_root:
        raise FhirParseError("empty_xml_bundle")
    mos_local = {mos_k.split(":")[-1]: mos_v for mos_k, mos_v in mos_root.items()}
    if "Bundle" in mos_root:
        return _xml_bundle_to_jsonish(mos_root["Bundle"])
    mos_bundle: Dict[str, Any] = {"resourceType": "Bundle"}
    mos_type_node = mos_local.get("type")
    if isinstance(mos_type_node, dict):
        mos_bundle["type"] = _flatten_fhir_primitive(mos_type_node.get("value", mos_type_node))
    elif mos_type_node is not None:
        mos_bundle["type"] = mos_type_node
    mos_entries = mos_local.get("entry")
    if mos_entries is None:
        mos_bundle["entry"] = []
    elif not isinstance(mos_entries, list):
        mos_entries = [mos_entries]
    mos_json_entries: List[Dict[str, Any]] = []
    for mos_ent in mos_entries:
        if not isinstance(mos_ent, dict):
            continue
        mos_ent_local = {mos_k.split(":")[-1]: mos_v for mos_k, mos_v in mos_ent.items()}
        mos_res = mos_ent_local.get("resource")
        if isinstance(mos_res, dict):
            mos_json_entries.append({"resource": _normalize_fhir_dict(mos_res)})
    mos_bundle["entry"] = mos_json_entries
    return mos_bundle


def parse_fhir_bundle_bytes(
    mos_payload: bytes,
    mos_content_type: Optional[str] = None,
) -> ParsedFhirBundle:
    """Parse and validate a FHIR R4 Bundle from JSON or XML bytes.

    Args:
        mos_payload: Raw HTTP body.
        mos_content_type: Optional Content-Type hint (fhir+json or fhir+xml).

    Returns:
        ParsedFhirBundle with entry resource dicts suitable for mapping.

    Raises:
        FhirParseError: If validation fails or resourceType is not Bundle.
    """
    mos_ct = (mos_content_type or "").lower()
    mos_text = mos_payload.decode("utf-8", errors="replace").strip()
    if not mos_text:
        raise FhirParseError("empty_body")
    try:
        if "xml" in mos_ct or mos_text.startswith("<"):
            mos_parsed = xmltodict.parse(mos_payload)
            mos_data = _normalize_fhir_dict(_xml_bundle_to_jsonish(mos_parsed))
            if mos_data.get("resourceType") != "Bundle":
                raise FhirParseError("not_a_bundle")
            try:
                mos_bundle = Bundle.model_validate(mos_data)
            except ValidationError as mos_exc:
                raise FhirParseError(f"bundle_validation:{mos_exc}") from mos_exc
        else:
            mos_loaded = json.loads(mos_text)
            if not isinstance(mos_loaded, dict):
                raise FhirParseError("json_not_object")
            try:
                mos_bundle = Bundle.model_validate(mos_loaded)
            except ValidationError:
                mos_normalized = _normalize_fhir_dict(mos_loaded)
                if mos_normalized.get("resourceType") != "Bundle":
                    raise FhirParseError("not_a_bundle")
                try:
                    mos_bundle = Bundle.model_validate(mos_normalized)
                except ValidationError as mos_exc:
                    raise FhirParseError(f"bundle_validation:{mos_exc}") from mos_exc
    except json.JSONDecodeError as mos_exc:
        raise FhirParseError(f"parse_error:{mos_exc}") from mos_exc
    except FhirParseError:
        raise
    except Exception as mos_exc:  # noqa: BLE001 — surface as parse failure
        raise FhirParseError(f"parse_error:{mos_exc}") from mos_exc
    mos_type = str(mos_bundle.type or "collection")
    mos_counts: Dict[str, int] = {}
    mos_entries_out: List[Dict[str, Any]] = []
    mos_entry_list = mos_bundle.entry or []
    for mos_entry in mos_entry_list:
        mos_res = mos_entry.resource
        if mos_res is None:
            continue
        mos_rt = getattr(mos_res, "resource_type", None) or mos_res.__class__.__name__
        mos_counts[mos_rt] = mos_counts.get(mos_rt, 0) + 1
        try:
            mos_dump = mos_res.model_dump(mode="json", exclude_none=True)
        except Exception:  # noqa: BLE001
            mos_dump = {"resourceType": mos_rt}
        mos_entries_out.append(mos_dump)
    return ParsedFhirBundle(
        mos_bundle_type=mos_type,
        mos_resource_type_counts=mos_counts,
        mos_entries=mos_entries_out,
        mos_raw_resource_count=len(mos_entries_out),
        mos_is_batch_semantics=mos_type in E_BATCH_BUNDLE_TYPES,
    )


def extract_bundle_resources(mos_parsed: ParsedFhirBundle) -> List[Dict[str, Any]]:
    """Return resource dicts from a parsed bundle (batch order preserved)."""
    return list(mos_parsed.mos_entries)
