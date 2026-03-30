"""Dead-letter queue for failed transforms (in-process deque cap)."""

from __future__ import annotations

import os
from collections import deque
from typing import Any, Deque, Dict, List

E_DLQ_MAX_ENV = "MOS_DLQ_MAX_ITEMS"
E_DLQ_DEFAULT_MAX = 5000


class DeadLetterQueue:
    """Bounded deque storing failure metadata only (no raw PHI payloads)."""

    def __init__(self) -> None:
        mos_max = int(os.environ.get(E_DLQ_MAX_ENV, str(E_DLQ_DEFAULT_MAX)))
        self._mos_items: Deque[Dict[str, Any]] = deque(maxlen=mos_max)

    def push(self, mos_entry: Dict[str, Any]) -> None:
        self._mos_items.append(mos_entry)

    def snapshot(self) -> List[Dict[str, Any]]:
        return list(self._mos_items)
