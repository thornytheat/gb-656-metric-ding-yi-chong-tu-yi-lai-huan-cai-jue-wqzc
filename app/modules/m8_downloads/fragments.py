"""分片与租约的小工具：字节切片、摘要、租约凭证、统一时间格式。

时间一律 UTC ISO（微秒位固定），字符串比较即时间比较。
"""

from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta

from app import config


def iso(moment: datetime) -> str:
    return moment.astimezone(config.UTC).isoformat(timespec="microseconds")


def now_iso() -> str:
    return iso(config.now_utc())


def lease_expiry(moment: datetime, lease_seconds: int) -> str:
    return iso(moment + timedelta(seconds=lease_seconds))


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def new_lease_token() -> str:
    return secrets.token_hex(16)


def slice_bounds(start: int, end: int, total: int) -> tuple[int, int]:
    """[start, end) 截到 [0, total] 内，返回实际落盘区间。"""
    return max(0, start), min(end, total)


def fragment_count(total_bytes: int, fragment_bytes: int) -> int:
    if total_bytes <= 0:
        return 0
    return (total_bytes + fragment_bytes - 1) // fragment_bytes
