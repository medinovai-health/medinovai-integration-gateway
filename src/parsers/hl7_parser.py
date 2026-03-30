"""HL7v2 message parsing for ADT, ORM, ORU, MDM (common segments)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from hl7apy.consts import VALIDATION_LEVEL
from hl7apy.core import Segment
from hl7apy.parser import ParserError, parse_message

E_SUPPORTED_MESSAGE_TYPES = frozenset(
    {"ADT", "ORM", "ORU", "MDM", "ACK", "BAR", "DFT"}
)


class Hl7ParseError(ValueError):
    """Raised when an HL7v2 message cannot be parsed."""


@dataclass
class ParsedHl7SegmentView:
    """PHI-safe segment summary: field count and segment name only in logs."""

    mos_name: str
    mos_fields: List[Optional[str]]


@dataclass
class ParsedHl7Message:
    """Structured HL7v2 view with segment maps for mapping layers."""

    mos_message_type: str
    mos_trigger_event: str
    mos_version: str
    mos_segments: Dict[str, List[ParsedHl7SegmentView]] = field(default_factory=dict)


def _segment_to_view(mos_seg: Segment) -> ParsedHl7SegmentView:
    """Map hl7apy Segment children (MSH_9, PID_3, …) to 1-based field index list."""
    mos_by_index: Dict[int, str] = {}
    for mos_child in mos_seg.children:
        mos_nm = getattr(mos_child, "name", "") or ""
        if "_" in mos_nm:
            _, mos_suffix = mos_nm.rsplit("_", 1)
            if mos_suffix.isdigit():
                mos_by_index[int(mos_suffix)] = str(mos_child.to_er7())
    mos_max = max(mos_by_index, default=0)
    mos_fields: List[Optional[str]] = [mos_by_index.get(mos_i) for mos_i in range(1, mos_max + 1)]
    return ParsedHl7SegmentView(mos_name=mos_seg.name, mos_fields=mos_fields)


def _append_segment(mos_store: Dict[str, List[ParsedHl7SegmentView]], mos_view: ParsedHl7SegmentView) -> None:
    mos_store.setdefault(mos_view.mos_name, []).append(mos_view)


def parse_hl7_message(mos_raw: str, mos_encoding: str = "utf-8") -> ParsedHl7Message:
    """Parse an HL7v2 pipe-delimited message using hl7apy (handles delimiters).

    Args:
        mos_raw: ER7-encoded message string or bytes-decoded text.
        mos_encoding: Source encoding hint (message decoded upstream).

    Returns:
        ParsedHl7Message with MSH-derived type and segment field arrays.

    Raises:
        Hl7ParseError: If parsing fails or MSH is missing.
    """
    mos_text = mos_raw.strip()
    if not mos_text:
        raise Hl7ParseError("empty_message")
    try:
        mos_msg = parse_message(mos_text, validation_level=VALIDATION_LEVEL.TOLERANT)
    except ParserError as mos_exc:
        raise Hl7ParseError(f"parser:{mos_exc}") from mos_exc
    except Exception as mos_exc:  # noqa: BLE001
        raise Hl7ParseError(f"parser:{mos_exc}") from mos_exc
    mos_segments: Dict[str, List[ParsedHl7SegmentView]] = {}
    for mos_child in mos_msg.children:
        if isinstance(mos_child, Segment):
            _append_segment(mos_segments, _segment_to_view(mos_child))
    mos_msh = mos_segments.get("MSH", [None])[0]
    if mos_msh is None or len(mos_msh.mos_fields) < 9:
        raise Hl7ParseError("missing_msh")
    mos_mtype = mos_msh.mos_fields[8] or ""
    mos_parts = mos_mtype.split("^")
    mos_mt = mos_parts[0] if mos_parts else ""
    mos_tr = mos_parts[1] if len(mos_parts) > 1 else ""
    mos_ver = mos_msh.mos_fields[11] if len(mos_msh.mos_fields) > 11 else ""
    if mos_mt and mos_mt not in E_SUPPORTED_MESSAGE_TYPES:
        # Still parse; downstream may reject — allow extensibility.
        pass
    return ParsedHl7Message(
        mos_message_type=mos_mt,
        mos_trigger_event=mos_tr,
        mos_version=str(mos_ver or ""),
        mos_segments=mos_segments,
    )


def hl7_message_to_serializable(mos_msg: ParsedHl7Message) -> Dict[str, Any]:
    """Convert parsed message to JSON-serializable dict for batch/API responses."""
    mos_seg_out: Dict[str, Any] = {}
    for mos_name, mos_list in mos_msg.mos_segments.items():
        mos_seg_out[mos_name] = [{"fields": mos_v.mos_fields} for mos_v in mos_list]
    return {
        "messageType": mos_msg.mos_message_type,
        "triggerEvent": mos_msg.mos_trigger_event,
        "version": mos_msg.mos_version,
        "segments": mos_seg_out,
    }
