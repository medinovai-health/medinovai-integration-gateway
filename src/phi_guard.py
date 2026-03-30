"""Heuristic PHI pattern detection — counts only; never logs matched content."""

from __future__ import annotations

import re
from dataclasses import dataclass
E_PHI_SSN_LIKE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
E_PHI_EMAIL_LIKE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")


@dataclass
class PhiScanResult:
    """Aggregate PHI heuristic hits for audit metrics."""

    mos_ssn_like_count: int
    mos_email_like_count: int
    mos_any_hit: bool


def scan_payload_for_phi_patterns(mos_text: str) -> PhiScanResult:
    """Return counts of suspicious patterns (no substring logging)."""
    mos_ssn = len(E_PHI_SSN_LIKE.findall(mos_text))
    mos_email = len(E_PHI_EMAIL_LIKE.findall(mos_text))
    return PhiScanResult(
        mos_ssn_like_count=mos_ssn,
        mos_email_like_count=mos_email,
        mos_any_hit=mos_ssn + mos_email > 0,
    )
