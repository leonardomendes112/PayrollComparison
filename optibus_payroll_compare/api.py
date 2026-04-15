from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Optional

import requests

from .models import DriverInfo
from .utils import date_batches, iso_date, safe_str


class OptibusError(RuntimeError):
    """Raised when the Optibus API returns an unexpected error."""


def _is_retryable_payroll_error(exc: Exception) -> bool:
    """Return True for payroll engine/server errors that are worth isolating by splitting."""
    message = str(exc)
    if "HTTP 500" not in message:
        return False
    if "/v2/payroll" not in message:
        return False
    lowered = message.lower()
    return (
        "engine-error" in lowered
        or "type mismatch" in lowered
        or "could not find a type converters" in lowered
        or '\"name\":\"engine-error\"' in lowered
    )


def _summarize_exception(exc: Exception, limit: int = 240) -> str:
    """Return a shortened single-line error summary for logs."""
    text = " ".join(str(exc).split())
    return text[:limit]


class OptibusClient:
    """Small HTTP client for the Optibus external API."""

    def __init__(self, base_url: str, api_key: str, api_client: str, timeout_s: int = 120) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": api_key,
                "X-Optibus-Api-Client": api_client,
            }
        )

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    def get_json(self, path: str, params: Optional[dict[str, Any]] = None, allow_413: bool = False) -> Any:
        """Run a GET request and parse JSON when available."""
        url = self._url(path)
        response = self.session.get(url, params=params, timeout=self.timeout_s, allow_redirects=True)
        if response.status_code in (413, 414) and allow_413:
            return {
                "__HTTP_413__": True,
                "__status__": response.status_code,
                "__text__": response.text,
                "__json__": self._maybe_json(response),
            }
        if response.status_code >= 400:
            raise OptibusError(
                f"HTTP {response.status_code} for GET {url} params={params} body={response.text[:800]}"
            )
        return self._maybe_json(response)

    @staticmethod
    def _maybe_json(response: requests.Response) -> Any:
        try:
            return response.json()
        except Exception:
            return response.text


def fetch_regions(client: OptibusClient) -> list[dict]:
    """Fetch regions or wrapped region payloads."""
    data = client.get_json("/v1/regions")
    if isinstance(data, dict):
        for key in ("regions", "data", "items"):
            value = data.get(key)
            if isinstance(value, list):
                return value
        return []
    return data if isinstance(data, list) else []


def fetch_all_drivers(client: OptibusClient, on_date: str) -> list[dict]:
    """Fetch all drivers using the paginated /v2/drivers endpoint."""
    page = 1
    all_rows: list[dict] = []
    while True:
        payload = client.get_json("/v2/drivers", params={"page": page, "onDate": on_date})
        drivers = payload.get("drivers", []) if isinstance(payload, dict) else []
        all_rows.extend(drivers)
        pagination = payload.get("pagination", {}) if isinstance(payload, dict) else {}
        current_page = pagination.get("currentPage", page)
        total_pages = pagination.get("totalPages", current_page)
        if current_page >= total_pages:
            break
        page += 1
    return all_rows


def build_driver_maps(drivers_payload: list[dict]) -> tuple[dict[str, DriverInfo], dict[str, DriverInfo]]:
    """Return lookup dictionaries by UUID and by external driver ID."""
    by_uuid: dict[str, DriverInfo] = {}
    by_external_id: dict[str, DriverInfo] = {}

    for driver in drivers_payload:
        uuid = safe_str(driver.get("uuid") or driver.get("driverUuid") or driver.get("id"))
        external_id = safe_str(driver.get("id") or driver.get("externalId") or driver.get("driverExternalId"))
        first_name = safe_str(driver.get("firstName"))
        last_name = safe_str(driver.get("lastName"))
        main_region_period = driver.get("mainRegionPeriod") or {}
        depot_name = safe_str(main_region_period.get("depotName") or driver.get("depotName"))
        region_name = safe_str(main_region_period.get("regionName") or driver.get("regionName"))

        if not uuid or not external_id:
            continue

        info = DriverInfo(
            external_id=external_id,
            uuid=uuid,
            first_name=first_name,
            last_name=last_name,
            depot_name=depot_name,
            region_name=region_name,
        )
        by_uuid[uuid] = info
        by_external_id[external_id] = info

    return by_uuid, by_external_id


def _chunk_list(items: list[str], chunk_size: int) -> list[list[str]]:
    if chunk_size <= 0:
        return [items]
    return [items[index : index + chunk_size] for index in range(0, len(items), chunk_size)]


def fetch_payroll_chunked(
    client: OptibusClient,
    start: date,
    end: date,
    driver_ids: list[str],
    batch_days: int,
    should_use_cache: bool,
    depot_id: Optional[str] = None,
    driver_chunk_size: int = 50,
    paycodes: Optional[list[str]] = None,
    sleep_seconds: float = 0.0,
    log=print,
) -> list[dict]:
    """Fetch payroll in date and driver chunks to reduce 413 errors."""
    all_rows: list[dict] = []
    batches = date_batches(start, end, batch_days)
    driver_chunks = _chunk_list(driver_ids, driver_chunk_size) if driver_ids else [[]]

    for batch_start, batch_end in batches:
        log(f"Payroll batch {iso_date(batch_start)} -> {iso_date(batch_end)}")
        for index, driver_chunk in enumerate(driver_chunks, start=1):
            if driver_ids:
                log(f"  Drivers chunk {index}/{len(driver_chunks)} (drivers={len(driver_chunk)})")
            rows = _fetch_payroll_range_resilient(
                client=client,
                start=batch_start,
                end=batch_end,
                driver_ids=driver_chunk,
                should_use_cache=should_use_cache,
                depot_id=depot_id,
                paycodes=paycodes,
                log=log,
            )
            all_rows.extend(rows)
            if sleep_seconds:
                import time

                time.sleep(sleep_seconds)

    return all_rows


def _fetch_payroll_range_resilient(
    client: OptibusClient,
    start: date,
    end: date,
    driver_ids: list[str],
    should_use_cache: bool,
    depot_id: Optional[str] = None,
    paycodes: Optional[list[str]] = None,
    log=print,
) -> list[dict]:
    """Fetch one date range and recursively split drivers/dates when the API rejects or cannot calculate the slice."""
    params: dict[str, Any] = {
        "startDate": iso_date(start),
        "endDate": iso_date(end),
        "shouldUseCache": "true" if should_use_cache else "false",
    }

    if paycodes:
        params["paycodes"] = ",".join(str(code) for code in paycodes)

    if driver_ids:
        params["driverIds"] = ",".join(str(driver_id) for driver_id in driver_ids)
    else:
        if not depot_id:
            raise OptibusError("depot_id is required when driver_ids is empty for /v2/payroll.")
        params["depotId"] = depot_id

    try:
        payload = client.get_json("/v2/payroll", params=params, allow_413=True)
    except OptibusError as exc:
        if _is_retryable_payroll_error(exc):
            if driver_ids and len(driver_ids) > 1:
                mid = len(driver_ids) // 2
                left = driver_ids[:mid]
                right = driver_ids[mid:]
                log(
                    "  500 payroll engine error. Splitting drivers: "
                    f"{len(driver_ids)} -> {len(left)} + {len(right)}"
                )
                return _fetch_payroll_range_resilient(
                    client, start, end, left, should_use_cache, depot_id, paycodes, log
                ) + _fetch_payroll_range_resilient(
                    client, start, end, right, should_use_cache, depot_id, paycodes, log
                )

            if start < end:
                total_days = (end - start).days + 1
                left_days = max(1, total_days // 2)
                left_end = start + timedelta(days=left_days - 1)
                right_start = left_end + timedelta(days=1)
                log(
                    "  500 payroll engine error. Splitting dates: "
                    f"{iso_date(start)}..{iso_date(end)} -> "
                    f"{iso_date(start)}..{iso_date(left_end)} + {iso_date(right_start)}..{iso_date(end)}"
                )
                return _fetch_payroll_range_resilient(
                    client, start, left_end, driver_ids, should_use_cache, depot_id, paycodes, log
                ) + _fetch_payroll_range_resilient(
                    client, right_start, end, driver_ids, should_use_cache, depot_id, paycodes, log
                )

            log(
                "  Skipping failing payroll slice after isolating API engine error: "
                f"date={iso_date(start)}, drivers={','.join(driver_ids) if driver_ids else '(depot mode)'}, "
                f"details={_summarize_exception(exc)}"
            )
            return []

        raise

    if isinstance(payload, dict) and payload.get("__HTTP_413__"):
        if driver_ids and len(driver_ids) > 1:
            mid = len(driver_ids) // 2
            left = driver_ids[:mid]
            right = driver_ids[mid:]
            log(f"  413 too large. Splitting drivers: {len(driver_ids)} -> {len(left)} + {len(right)}")
            return _fetch_payroll_range_resilient(
                client, start, end, left, should_use_cache, depot_id, paycodes, log
            ) + _fetch_payroll_range_resilient(
                client, start, end, right, should_use_cache, depot_id, paycodes, log
            )

        if start >= end:
            raise OptibusError(
                f"Payroll request too large even for minimal range ({iso_date(start)}): "
                f"{payload.get('__text__', '')[:800]}"
            )

        total_days = (end - start).days + 1
        left_days = max(1, total_days // 2)
        left_end = start + timedelta(days=left_days - 1)
        right_start = left_end + timedelta(days=1)
        log(
            "  413 too large. Splitting dates: "
            f"{iso_date(start)}..{iso_date(end)} -> "
            f"{iso_date(start)}..{iso_date(left_end)} + {iso_date(right_start)}..{iso_date(end)}"
        )
        return _fetch_payroll_range_resilient(
            client, start, left_end, driver_ids, should_use_cache, depot_id, paycodes, log
        ) + _fetch_payroll_range_resilient(
            client, right_start, end, driver_ids, should_use_cache, depot_id, paycodes, log
        )

    if isinstance(payload, list):
        return payload

    raise OptibusError(f"Unexpected payroll response type: {type(payload)}")


def fetch_absences(client: OptibusClient, start: date, end: date) -> list[dict]:
    """Fetch paginated driver absences for the date range."""
    page = 1
    all_rows: list[dict] = []
    while True:
        payload = client.get_json(
            "/v2/drivers/absences",
            params={"fromDate": iso_date(start), "toDate": iso_date(end), "page": page},
        )
        absences = payload.get("absences", []) if isinstance(payload, dict) else []
        all_rows.extend(absences)
        pagination = payload.get("pagination", {}) if isinstance(payload, dict) else {}
        current_page = pagination.get("currentPage", page)
        total_pages = pagination.get("totalPages", current_page)
        if current_page >= total_pages:
            break
        page += 1
    return all_rows


def fetch_operational_plan_v2(
    client: OptibusClient,
    start: date,
    end: date,
    depot_uuids: Optional[str | list[str]] = None,
) -> Any:
    """Fetch operational plan data including actual and planned assignments."""
    params: dict[str, Any] = {
        "fromDate": iso_date(start),
        "toDate": iso_date(end),
        "includeStops": "false",
        "includeUnassigned": "false",
    }
    if depot_uuids:
        if isinstance(depot_uuids, (list, tuple, set)):
            params["depotUuids"] = ",".join([str(value) for value in depot_uuids if str(value)])
        else:
            params["depotUuids"] = str(depot_uuids)

    return client.get_json("/v2/operational-plan", params=params)


def ensure_list_payload(payload: Any) -> list[dict]:
    """Normalize an operational-plan response into a list of depot plans."""
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return payload
    return []


def _task_display(task: dict) -> str:
    """Choose the most readable task identifier for allocation exports."""
    display_id = safe_str(task.get("displayId") or "")
    if display_id:
        return display_id
    description = safe_str(task.get("description") or "")
    if description:
        return description
    task_type = safe_str(task.get("type") or task.get("dutyType") or "task")
    task_id = safe_str(task.get("id") or "")
    return f"{task_type}:{task_id}" if task_id else task_type


def build_allocation_maps_from_operational_plan(
    depot_plan: dict,
    by_uuid: dict[str, DriverInfo],
) -> tuple[dict[tuple[str, str], list[str]], dict[tuple[str, str], list[str]]]:
    """Return actual and planned allocation maps keyed by (external_driver_id, date)."""

    def normalize_driver_id(raw: Any) -> str:
        value = safe_str(raw).strip()
        if not value:
            return ""
        if value.isdigit():
            return value
        info = by_uuid.get(value)
        return info.external_id if info else value

    tasks = depot_plan.get("tasks", []) or []
    tasks_by_id: dict[str, str] = {}
    for task in tasks:
        task_id = safe_str(task.get("id"))
        if task_id:
            tasks_by_id[task_id] = _task_display(task)

    actual_map: dict[tuple[str, str], list[str]] = defaultdict(list)
    planned_map: dict[tuple[str, str], list[str]] = defaultdict(list)

    for assignment in depot_plan.get("assignments", []) or []:
        date_text = safe_str(assignment.get("date"))
        if not date_text:
            continue

        for actual in assignment.get("driverAssignments", []) or []:
            driver_id = normalize_driver_id(actual.get("driver") or actual.get("driverId") or "")
            if not driver_id:
                continue
            for task_id in actual.get("assignments", []) or []:
                task_key = safe_str(task_id)
                if task_key:
                    actual_map[(driver_id, date_text)].append(tasks_by_id.get(task_key, task_key))

        for planned in assignment.get("plannedAssignments", []) or []:
            driver_id = normalize_driver_id(planned.get("driver") or planned.get("driverId") or "")
            if not driver_id:
                continue
            for task_id in planned.get("assignments", []) or []:
                task_key = safe_str(task_id)
                if task_key:
                    planned_map[(driver_id, date_text)].append(tasks_by_id.get(task_key, task_key))

    return actual_map, planned_map
