from __future__ import annotations

import csv
import re
import zipfile
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from .api import build_allocation_maps_from_operational_plan, ensure_list_payload, fetch_operational_plan_v2
from .models import DriverInfo
from .utils import (
    COL_AMOUNT,
    COL_CODE,
    COL_DATE,
    COL_DRIVER,
    COL_UNIT,
    DIFF_COLS,
    amounts_equal,
    date_batches,
    excel_date_col,
    excel_sanitize_cell,
    excel_unsanitize_cell,
    is_zero_amount,
    minutes_to_hhmm,
    normalize_str,
    safe_str,
    sniff_dialect,
)


def format_amount_and_unit(value: Any, unit: str) -> tuple[str, str]:
    """Format Optibus result values into the CSV shape expected by downstream users."""
    normalized_unit = (unit or "").strip()
    if isinstance(value, bool) or normalized_unit.lower() == "boolean":
        return ("1" if bool(value) else "0", "Boolean")

    if isinstance(value, str):
        stripped = value.strip()
        if normalized_unit:
            return (stripped, normalized_unit)
        return (stripped, "Number")

    if normalized_unit.lower() in {"minutes", "minute"}:
        return (minutes_to_hhmm(0 if value is None else value), "Hours")

    if normalized_unit.lower() in {"hours", "hour"}:
        try:
            hours = float(value)
            return (minutes_to_hhmm(hours * 60.0), "Hours")
        except Exception:
            return (safe_str(value), "Hours")

    if normalized_unit.lower() in {"days", "day"}:
        return (safe_str(value), "Days")

    return (safe_str(value), normalized_unit if normalized_unit else "Number")


def to_payroll_rows_from_payroll_api(records: list[dict]) -> list[dict]:
    """Transform payroll API records into flat CSV rows."""
    rows: list[dict] = []
    for record in records:
        working_driver = record.get("workingDriver") or {}
        driver_id = safe_str(
            working_driver.get("driverExternalId")
            or working_driver.get("driverId")
            or working_driver.get("driverUuid")
        )
        occurrence_dates = record.get("occurrenceDates") or {}
        date_text = safe_str(occurrence_dates.get("startDate") or occurrence_dates.get("date") or "")
        code = safe_str(record.get("codeId") or record.get("workEntityIdReference") or record.get("entityId") or "")
        result = record.get("result") or {}
        unit = safe_str(result.get("unit") or "")
        value = result.get("value")
        amount_text, unit_text = format_amount_and_unit(value=value, unit=unit)

        if driver_id and date_text and code:
            rows.append(
                {
                    COL_DRIVER: driver_id,
                    COL_DATE: date_text,
                    COL_CODE: code,
                    COL_AMOUNT: amount_text,
                    COL_UNIT: unit_text,
                }
            )
    return rows


def save_payroll_csv(rows: list[dict], out_path: Path) -> int:
    """Save payroll rows and return the number of records written."""
    dataframe = pd.DataFrame(rows, columns=[COL_DRIVER, COL_DATE, COL_CODE, COL_AMOUNT, COL_UNIT])
    dataframe[COL_AMOUNT] = dataframe[COL_AMOUNT].apply(excel_sanitize_cell)
    dataframe[COL_DRIVER] = pd.to_numeric(dataframe[COL_DRIVER], errors="ignore")
    dataframe = dataframe.sort_values([COL_DRIVER, COL_DATE, COL_CODE, COL_UNIT], kind="stable")
    dataframe.to_csv(out_path, index=False, encoding="utf-8-sig")
    return len(dataframe)


def minutes_to_time_str(value: Any) -> str:
    """Keep times as raw minute counts to match the existing absence export."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    try:
        return str(int(value))
    except Exception:
        return safe_str(value)


def save_absences_csv(absences: list[dict], by_external_id: dict[str, DriverInfo], out_path: Path) -> int:
    """Save absences in the same column layout used by the original script."""
    out_rows: list[dict] = []
    for absence in absences:
        driver_id = safe_str(absence.get("driverId"))
        info = by_external_id.get(driver_id)
        driver_name = f"{info.first_name} {info.last_name}".strip() if info else ""
        depot_name = info.depot_name if info else ""

        out_rows.append(
            {
                "Driver Id": driver_id,
                "Driver Name": driver_name,
                "Depot Name": depot_name,
                "Absence code": safe_str(absence.get("absenceCode")),
                "Start date": safe_str(absence.get("startDate")),
                "Start time": minutes_to_time_str(absence.get("startTime")),
                "End date": safe_str(absence.get("endDate") or absence.get("startDate")),
                "End time": minutes_to_time_str(absence.get("endTime")),
                "Note": safe_str(absence.get("note")),
            }
        )

    dataframe = pd.DataFrame(
        out_rows,
        columns=[
            "Driver Id",
            "Driver Name",
            "Depot Name",
            "Absence code",
            "Start date",
            "Start time",
            "End date",
            "End time",
            "Note",
        ],
    )
    dataframe["Driver Id"] = pd.to_numeric(dataframe["Driver Id"], errors="ignore")
    dataframe = dataframe.sort_values(["Driver Id", "Start date", "Absence code"], kind="stable")
    dataframe.to_csv(out_path, index=False, encoding="utf-8-sig")
    return len(dataframe)


def _save_allocation_matrix(
    allocation_map: dict[tuple[str, str], list[str]],
    driver_ids: list[str],
    start: date,
    end: date,
    out_path: Path,
) -> int:
    """Write a wide allocation matrix keyed by Driver ID and date columns."""
    dates: list[date] = []
    current = start
    while current <= end:
        dates.append(current)
        current = current.fromordinal(current.toordinal() + 1)

    columns = ["Driver ID"] + [excel_date_col(value) for value in dates]
    rows_out: list[dict] = []

    def sort_key(value: str) -> tuple[int, Any]:
        try:
            return (0, int(value))
        except Exception:
            return (1, value)

    for driver_id in sorted(list(driver_ids), key=sort_key):
        row = {"Driver ID": safe_str(driver_id)}
        for current_date in dates:
            key = (safe_str(driver_id), current_date.isoformat())
            values = allocation_map.get(key, [])
            if values:
                seen: set[str] = set()
                ordered: list[str] = []
                for value in values:
                    value_text = str(value).strip()
                    if value_text and value_text not in seen:
                        seen.add(value_text)
                        ordered.append(value_text)
                row[excel_date_col(current_date)] = ", ".join(ordered)
            else:
                row[excel_date_col(current_date)] = ""
        rows_out.append(row)

    dataframe = pd.DataFrame(rows_out, columns=columns)
    dataframe["Driver ID"] = pd.to_numeric(dataframe["Driver ID"], errors="ignore")
    dataframe.to_csv(out_path, index=False, encoding="utf-8-sig")
    return len(dataframe)


def save_allocation_csvs(
    client,
    by_uuid: dict[str, DriverInfo],
    driver_external_ids: list[str],
    start: date,
    end: date,
    batch_days: int,
    out_actual_path: Path,
    out_planned_path: Path,
) -> tuple[int, int]:
    """Fetch actual/planned allocations and save two wide CSVs."""
    actual_map: dict[tuple[str, str], list[str]] = defaultdict(list)
    planned_map: dict[tuple[str, str], list[str]] = defaultdict(list)

    for batch_start, batch_end in date_batches(start, end, batch_days):
        payload = fetch_operational_plan_v2(client, start=batch_start, end=batch_end, depot_uuids=None)
        depot_plans = ensure_list_payload(payload)

        for depot_plan in depot_plans:
            if not isinstance(depot_plan, dict):
                continue
            batch_actual_map, batch_planned_map = build_allocation_maps_from_operational_plan(depot_plan, by_uuid)
            for key, values in batch_actual_map.items():
                if values:
                    actual_map[key].extend(values)
            for key, values in batch_planned_map.items():
                if values:
                    planned_map[key].extend(values)

    actual_rows = _save_allocation_matrix(actual_map, driver_external_ids, start, end, out_actual_path)
    planned_rows = _save_allocation_matrix(planned_map, driver_external_ids, start, end, out_planned_path)
    return actual_rows, planned_rows


Key = tuple[str, str, str, str]


def load_amount_lists(path: str, encoding: str, delimiter: str = "") -> dict[Key, list[str]]:
    """Load CSV amounts keyed by the comparison dimensions."""
    dialect = sniff_dialect(path, encoding)
    if delimiter:
        dialect.delimiter = delimiter  # type: ignore[attr-defined]

    out: dict[Key, list[str]] = defaultdict(list)
    with open(path, "r", encoding=encoding, newline="") as handle:
        reader = csv.DictReader(handle, dialect=dialect)
        for row in reader:
            key: Key = (
                normalize_str(row.get(COL_DRIVER)),
                normalize_str(row.get(COL_DATE)),
                normalize_str(row.get(COL_CODE)),
                normalize_str(row.get(COL_UNIT)),
            )
            amount_raw = excel_unsanitize_cell(row.get(COL_AMOUNT))
            out[key].append(normalize_str(amount_raw))
    return out


def compute_diffs(
    file1: str,
    file2: str,
    out_csv: str,
    tolerance: float | None = None,
    encoding: str = "utf-8-sig",
    delimiter: str = "",
) -> int:
    """Compare two payroll CSVs and write the differences file."""
    left_map = load_amount_lists(file1, encoding=encoding, delimiter=delimiter)
    right_map = load_amount_lists(file2, encoding=encoding, delimiter=delimiter)

    all_keys = set(left_map.keys()) | set(right_map.keys())
    differences: list[dict[str, Any]] = []

    for key in all_keys:
        left_list = list(left_map.get(key, []))
        right_list = list(right_map.get(key, []))

        left_counts = Counter(left_list)
        right_counts = Counter(right_list)

        common = left_counts & right_counts
        for value, count in common.items():
            left_counts[value] -= count
            right_counts[value] -= count
            if left_counts[value] <= 0:
                del left_counts[value]
            if right_counts[value] <= 0:
                del right_counts[value]

        left_remaining = list(left_counts.elements())
        right_remaining = list(right_counts.elements())

        if tolerance is not None:
            right_used = [False] * len(right_remaining)
            new_left: list[str] = []
            for left_value in left_remaining:
                matched = False
                for index, right_value in enumerate(right_remaining):
                    if right_used[index]:
                        continue
                    if amounts_equal(left_value, right_value, tolerance):
                        right_used[index] = True
                        matched = True
                        break
                if not matched:
                    new_left.append(left_value)
            new_right = [value for index, value in enumerate(right_remaining) if not right_used[index]]
            left_remaining, right_remaining = new_left, new_right

        driver_id, date_text, code, unit = key
        if len(left_remaining) == 0 and len(right_remaining) == 0:
            continue

        if len(left_remaining) == len(right_remaining) and len(left_remaining) > 0:
            for left_value, right_value in zip(left_remaining, right_remaining):
                left_is_zero = is_zero_amount(left_value, unit)
                right_is_zero = is_zero_amount(right_value, unit)

                if left_is_zero and right_is_zero:
                    continue
                if left_is_zero and not right_is_zero:
                    differences.append(
                        {
                            COL_DRIVER: driver_id,
                            COL_DATE: date_text,
                            COL_CODE: code,
                            COL_UNIT: unit,
                            "Change": "addition",
                            "Pre-changes": "",
                            "Post-changes": right_value,
                        }
                    )
                    continue
                if right_is_zero and not left_is_zero:
                    differences.append(
                        {
                            COL_DRIVER: driver_id,
                            COL_DATE: date_text,
                            COL_CODE: code,
                            COL_UNIT: unit,
                            "Change": "removed",
                            "Pre-changes": left_value,
                            "Post-changes": "",
                        }
                    )
                    continue
                differences.append(
                    {
                        COL_DRIVER: driver_id,
                        COL_DATE: date_text,
                        COL_CODE: code,
                        COL_UNIT: unit,
                        "Change": "modified",
                        "Pre-changes": left_value,
                        "Post-changes": right_value,
                    }
                )
        else:
            for left_value in left_remaining:
                differences.append(
                    {
                        COL_DRIVER: driver_id,
                        COL_DATE: date_text,
                        COL_CODE: code,
                        COL_UNIT: unit,
                        "Change": "removed",
                        "Pre-changes": left_value,
                        "Post-changes": "",
                    }
                )
            for right_value in right_remaining:
                differences.append(
                    {
                        COL_DRIVER: driver_id,
                        COL_DATE: date_text,
                        COL_CODE: code,
                        COL_UNIT: unit,
                        "Change": "addition",
                        "Pre-changes": "",
                        "Post-changes": right_value,
                    }
                )

    dataframe = pd.DataFrame(differences, columns=DIFF_COLS)
    dataframe[COL_DRIVER] = pd.to_numeric(dataframe[COL_DRIVER], errors="ignore")
    dataframe = dataframe.sort_values([COL_DRIVER, COL_DATE, COL_CODE, COL_UNIT, "Change"], kind="stable")
    if "Pre-changes" in dataframe.columns:
        dataframe["Pre-changes"] = dataframe["Pre-changes"].apply(
            lambda value: excel_sanitize_cell(excel_unsanitize_cell(value))
        )
    if "Post-changes" in dataframe.columns:
        dataframe["Post-changes"] = dataframe["Post-changes"].apply(
            lambda value: excel_sanitize_cell(excel_unsanitize_cell(value))
        )
    dataframe.to_csv(out_csv, index=False, encoding="utf-8-sig")
    return len(dataframe)


def enrich_differences(
    amount_path: Path,
    absences_path: Path,
    allocation_actual_path: Path,
    allocation_planned_path: Path,
    out_path: Path,
) -> int:
    """Enrich differences with absences, actual allocation, and planned allocation."""
    amount = pd.read_csv(amount_path, encoding="utf-8-sig")
    for column in [COL_DRIVER, COL_DATE]:
        if column not in amount.columns:
            raise ValueError(f"differences CSV missing required column: {column}")

    absences_df = pd.read_csv(absences_path, encoding="utf-8-sig")
    required_absence_columns = {"Driver Id", "Start date", "End date", "Absence code"}
    if not required_absence_columns.issubset(absences_df.columns):
        raise ValueError(
            "absences CSV missing required columns: Driver Id, Start date, End date, Absence code"
        )

    def parse_excel_date(value: Any) -> pd.Timestamp | None:
        if pd.isna(value):
            return None
        text = str(value).strip().replace('="', "").replace('"', "")
        try:
            return pd.to_datetime(text, format="%Y-%m-%d")
        except Exception:
            try:
                return pd.to_datetime(text, dayfirst=False)
            except Exception:
                return None

    absences_df["_start"] = absences_df["Start date"].apply(parse_excel_date)
    absences_df["_end"] = absences_df["End date"].apply(parse_excel_date)
    absences_df["_driver"] = absences_df["Driver Id"].astype(str).str.strip()

    absences_map: dict[tuple[str, pd.Timestamp], list[str]] = defaultdict(list)
    for _, row in absences_df.iterrows():
        driver_id = row["_driver"]
        start_date = row["_start"]
        end_date = row["_end"]
        absence_code = str(row["Absence code"]).strip()
        if not driver_id or driver_id in {"nan", "None"} or start_date is None or end_date is None or not absence_code:
            continue
        current = start_date.normalize()
        end_normalized = end_date.normalize()
        while current <= end_normalized:
            absences_map[(str(driver_id), current)].append(absence_code)
            current += pd.Timedelta(days=1)

    def absences_for(driver_id: str, timestamp: pd.Timestamp) -> str:
        codes = absences_map.get((driver_id, timestamp.normalize()), [])
        if not codes:
            return ""
        seen: set[str] = set()
        ordered: list[str] = []
        for code in codes:
            if code not in seen:
                ordered.append(code)
                seen.add(code)
        return ", ".join(ordered)

    actual_allocation = pd.read_csv(allocation_actual_path, encoding="utf-8-sig")
    planned_allocation = pd.read_csv(allocation_planned_path, encoding="utf-8-sig")
    for dataframe, name in [(actual_allocation, "allocation actual"), (planned_allocation, "allocation planned")]:
        if "Driver ID" not in dataframe.columns:
            raise ValueError(f"{name} CSV missing required column: Driver ID")

    def allocation_map_from_matrix(dataframe: pd.DataFrame) -> dict[tuple[str, pd.Timestamp], str]:
        allocation_lists: dict[tuple[str, pd.Timestamp], list[str]] = defaultdict(list)
        date_columns = [column for column in dataframe.columns if re.match(r"^\d{4}-\d{2}-\d{2}$", str(column))]
        frame = dataframe.copy()
        frame["Driver ID"] = frame["Driver ID"].astype(str).str.strip()

        for _, row in frame.iterrows():
            driver_id = row["Driver ID"]
            if not driver_id or driver_id in {"nan", "None"}:
                continue
            for column in date_columns:
                value = row.get(column)
                if pd.isna(value):
                    continue
                text = str(value).strip()
                if not text:
                    continue
                timestamp = parse_excel_date(column)
                if timestamp is None:
                    continue
                allocation_lists[(str(driver_id), timestamp.normalize())].append(text)

        out: dict[tuple[str, pd.Timestamp], str] = {}
        for key, values in allocation_lists.items():
            seen: set[str] = set()
            ordered: list[str] = []
            for value in values:
                if value and value not in seen:
                    seen.add(value)
                    ordered.append(value)
            out[key] = ", ".join(ordered)
        return out

    actual_map = allocation_map_from_matrix(actual_allocation)
    planned_map = allocation_map_from_matrix(planned_allocation)

    def allocation_for(driver_id: str, timestamp: pd.Timestamp, which: str) -> str:
        key = (driver_id, timestamp.normalize())
        if which == "actual":
            return actual_map.get(key, "")
        return planned_map.get(key, "")

    amount[COL_DRIVER] = amount[COL_DRIVER].astype(str).str.strip()
    amount[COL_DATE] = pd.to_datetime(amount[COL_DATE], errors="coerce")
    amount["Absences"] = [
        absences_for(str(driver_id), timestamp)
        if (str(driver_id) not in {"", "nan", "None"} and not pd.isna(timestamp))
        else ""
        for driver_id, timestamp in zip(amount[COL_DRIVER], amount[COL_DATE])
    ]
    amount["Actual Allocation"] = [
        allocation_for(str(driver_id), timestamp, "actual")
        if (str(driver_id) not in {"", "nan", "None"} and not pd.isna(timestamp))
        else ""
        for driver_id, timestamp in zip(amount[COL_DRIVER], amount[COL_DATE])
    ]
    amount["Planned Allocation"] = [
        allocation_for(str(driver_id), timestamp, "planned")
        if (str(driver_id) not in {"", "nan", "None"} and not pd.isna(timestamp))
        else ""
        for driver_id, timestamp in zip(amount[COL_DRIVER], amount[COL_DATE])
    ]

    for column in ["Pre-changes", "Post-changes"]:
        if column in amount.columns:
            amount[column] = amount[column].apply(
                lambda value: "" if is_zero_amount(value, "") else excel_sanitize_cell(excel_unsanitize_cell(value))
            )

    amount.to_csv(out_path, index=False, encoding="utf-8-sig")
    return len(amount)


def create_zip_archive(file_paths: list[Path], zip_path: Path) -> Path:
    """Bundle output files into a single zip archive."""
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for file_path in file_paths:
            archive.write(file_path, arcname=file_path.name)
    return zip_path
