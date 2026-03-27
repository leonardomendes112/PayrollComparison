from __future__ import annotations

import csv
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional, Union

COL_DRIVER = "DriverID"
COL_DATE = "Date"
COL_CODE = "Code"
COL_AMOUNT = "Amount"
COL_UNIT = "Time Unit"

DIFF_COLS = [COL_DRIVER, COL_DATE, COL_CODE, COL_UNIT, "Change", "Pre-changes", "Post-changes"]

_TIME_RE = re.compile(r"^[+-]?\d{1,3}:\d{2}$")


def mask_api_key(key: str, keep: int = 6) -> str:
    """Mask an API key for safe display in logs/UI."""
    key = (key or "").strip()
    if not key:
        return ""
    if len(key) <= keep:
        return "*" * len(key)
    return ("*" * (len(key) - keep)) + key[-keep:]


def iso_date(value: date) -> str:
    """Return an ISO-8601 date string."""
    return value.isoformat()


def parse_iso_date(value: str) -> date:
    """Parse a YYYY-MM-DD string."""
    return datetime.strptime(value, "%Y-%m-%d").date()


def date_batches(start: date, end: date, batch_days: int) -> list[tuple[date, date]]:
    """Split an inclusive date range into smaller inclusive batches."""
    if end < start:
        raise ValueError("end-date must be greater than or equal to start-date")
    out: list[tuple[date, date]] = []
    current = start
    while current <= end:
        batch_end = min(end, current + timedelta(days=batch_days - 1))
        out.append((current, batch_end))
        current = batch_end + timedelta(days=1)
    return out


def auto_tune_chunking(driver_count: int, total_days: int) -> tuple[int, int]:
    """Choose practical defaults to reduce 413 errors from the payroll endpoint."""
    total_days = max(1, int(total_days or 1))

    if driver_count >= 800:
        batch_days = 3
    elif driver_count >= 500:
        batch_days = 5
    elif driver_count >= 250:
        batch_days = 7
    else:
        batch_days = 10
    batch_days = min(batch_days, total_days)

    target_driver_days = 100
    driver_chunk = max(5, min(50, target_driver_days // max(1, batch_days)))
    if driver_count >= 600:
        driver_chunk = min(driver_chunk, 10)
    elif driver_count >= 300:
        driver_chunk = min(driver_chunk, 15)
    elif driver_count >= 150:
        driver_chunk = min(driver_chunk, 20)
    driver_chunk = min(driver_chunk, max(1, driver_count))
    return batch_days, driver_chunk


def excel_date_col(value: date) -> str:
    """Use ISO date columns so Excel opens the CSV safely."""
    return value.isoformat()


def minutes_to_hhmm(total_minutes: Union[float, int]) -> str:
    """Convert minutes to HH:MM."""
    try:
        value = int(round(float(total_minutes)))
    except Exception:
        return str(total_minutes)
    sign = "-" if value < 0 else ""
    value = abs(value)
    hours = value // 60
    minutes = value % 60
    return f"{sign}{hours:02d}:{minutes:02d}"


def safe_str(value: Any) -> str:
    """Convert None to an empty string, otherwise cast to str."""
    return "" if value is None else str(value)


def excel_sanitize_cell(value: Any) -> str:
    """Prevent Excel from treating values as formulas or coercing negative HH:MM."""
    if value is None:
        return ""
    text = str(value)
    if not text:
        return ""
    if text[0] in ("=", "+", "@"):
        return "'" + text
    if text[0] == "-" and _TIME_RE.match(text):
        return "'" + text
    return text


def excel_unsanitize_cell(value: Any) -> str:
    """Undo the leading apostrophe added for Excel safety."""
    if value is None:
        return ""
    text = str(value)
    return text[1:] if text.startswith("'") and len(text) > 1 else text


def is_zero_amount(amount: Any, unit: Any = "") -> bool:
    """Treat numeric and time-like zero values as empty for diff classification."""
    amount_text = excel_unsanitize_cell(amount).strip()
    if amount_text == "":
        return True
    cleaned = amount_text.replace(",", "").strip()
    if ":" in cleaned:
        time_text = cleaned.lstrip("+")
        negative = time_text.startswith("-")
        time_text = time_text[1:] if negative else time_text
        return time_text in ("0:00", "00:00", "00:00:00")
    try:
        return float(cleaned) == 0.0
    except Exception:
        return cleaned in ("0", "0.0", "0.00")


def sniff_dialect(path: str, encoding: str) -> csv.Dialect:
    """Best-effort CSV dialect detection."""
    with open(path, "r", encoding=encoding, newline="") as handle:
        sample = handle.read(50_000)
    try:
        return csv.Sniffer().sniff(sample)
    except Exception:
        return csv.get_dialect("excel")


def normalize_str(value: Any) -> str:
    """Normalize a value for diffing."""
    if value is None:
        return ""
    return str(value).strip()


def try_float(value: str) -> Optional[float]:
    """Convert a numeric-like string into a float when possible."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "")
    try:
        return float(text)
    except Exception:
        return None


def amounts_equal(left: str, right: str, tolerance: Optional[float]) -> bool:
    """Compare two amount strings exactly or with numeric tolerance."""
    left_n = normalize_str(left)
    right_n = normalize_str(right)
    if left_n == right_n:
        return True

    left_f = try_float(left_n)
    right_f = try_float(right_n)
    if left_f is not None and right_f is not None:
        if tolerance is None:
            return left_f == right_f
        return abs(left_f - right_f) <= tolerance

    return False


def ensure_directory(path: Path) -> Path:
    """Create a directory if needed and return it."""
    path.mkdir(parents=True, exist_ok=True)
    return path
