# Proposed architecture

This refactor separates the Streamlit UI from the Optibus API and CSV-processing logic. The app keeps the original PRE/POST workflow, but replaces the CLI `input()` pause with two explicit Streamlit steps.

## Repository structure

```text
.
├── .env.example
├── .gitignore
├── README.md
├── optibus_payroll_compare/__init__.py
├── optibus_payroll_compare/api.py
├── optibus_payroll_compare/models.py
├── optibus_payroll_compare/pipeline.py
├── optibus_payroll_compare/processing.py
├── optibus_payroll_compare/utils.py
├── requirements.txt
├── streamlit_app.py
```

## .env.example

```
OPTIBUS_BASE_URL=https://YOUR-ACCOUNT.api.ops.optibus.co
OPTIBUS_API_CLIENT=YOUR_ACCOUNT_NAME
OPTIBUS_API_KEY=YOUR_API_KEY
```

## .gitignore

```
# Python
__pycache__/
*.py[cod]
*.pyo
*.pyd
*.so
.Python
.pytest_cache/
.mypy_cache/
.ruff_cache/

# Virtual environments
.venv/
venv/
env/

# Environment and secrets
.env
.streamlit/secrets.toml

# Streamlit and local outputs
outputs/
tmp/
*.zip

# OS / editor
.DS_Store
.vscode/
.idea/
```

## README.md

```
# Optibus Payroll Compare Streamlit App

## What this project does

This project refactors the uploaded local script into a GitHub-ready Streamlit app for comparing Optibus payroll outputs before and after Work Entity changes.

The workflow stays aligned with the original script:

1. Fetch **PRE** payroll data for a date range
2. Fetch **PRE** absences
3. Fetch **PRE** actual and planned allocations
4. Pause while you update Work Entities in the Optibus web UI
5. Fetch **POST** payroll data
6. Generate:
   - PRE payroll CSV
   - POST payroll CSV
   - payroll differences CSV
   - enriched payroll differences CSV
   - PRE absences CSV
   - PRE actual allocation CSV
   - PRE planned allocation CSV
   - a ZIP containing all outputs

The CSV column shapes are preserved so downstream processes should continue to work.

## Proposed architecture

The refactor separates the project into three layers:

- `streamlit_app.py`: Streamlit UI only
- `optibus_payroll_compare/api.py`: API client and data-fetching logic
- `optibus_payroll_compare/processing.py`: CSV shaping, diffing, enrichment, and ZIP creation
- `optibus_payroll_compare/pipeline.py`: PRE/POST orchestration
- `optibus_payroll_compare/models.py` and `utils.py`: shared data structures and helpers

This removes local-only UI assumptions such as AppleScript prompts, `input()`, and macOS Keychain storage from the core logic.

## Repo structure

```text
.
├── .env.example
├── .gitignore
├── README.md
├── requirements.txt
├── streamlit_app.py
└── optibus_payroll_compare
    ├── __init__.py
    ├── api.py
    ├── models.py
    ├── pipeline.py
    ├── processing.py
    └── utils.py
```

## Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Environment variables

You can set these locally in a `.env` file or in your shell:

```bash
OPTIBUS_BASE_URL=https://YOUR-ACCOUNT.api.ops.optibus.co
OPTIBUS_API_CLIENT=YOUR_ACCOUNT_NAME
OPTIBUS_API_KEY=YOUR_API_KEY
```

A sample template is included in `.env.example`.

## How to run locally

```bash
streamlit run streamlit_app.py
```

Then:

1. Enter your connection details if they are not already in environment variables
2. Choose the start and end date
3. Optionally provide paycodes, batch overrides, or a diff tolerance
4. Click **Run PRE fetch**
5. Make your Work Entity changes in Optibus
6. Return to the app and click **Run POST fetch + compare**
7. Download the CSVs or the ZIP bundle

## Example usage

Typical local usage:

```bash
export OPTIBUS_BASE_URL="https://YOUR-ACCOUNT.api.ops.optibus.co"
export OPTIBUS_API_CLIENT="ADO"
export OPTIBUS_API_KEY="YOUR_API_KEY"
streamlit run streamlit_app.py
```

## Streamlit deployment notes

### Streamlit Community Cloud

Set the following in the app settings or secrets:

- `OPTIBUS_BASE_URL`
- `OPTIBUS_API_CLIENT`
- `OPTIBUS_API_KEY`

This app writes outputs to a temporary directory for the current session. Keep the same browser session open between the PRE and POST steps.

### Paths and secrets

- No hardcoded local paths are used
- No macOS-only AppleScript or Keychain features remain
- Credentials are read from Streamlit inputs, environment variables, or Streamlit secrets/environment settings

## What changed from the original script

### Preserved

- The same core Optibus API workflow
- Driver and date chunking to reduce 413 errors
- Pre/post payroll comparison logic
- Enriched differences with absences and allocation
- CSV shapes and naming style

### Changed for maintainability and Streamlit readiness

- Removed local-only AppleScript dialogs and CLI `input()` pause
- Replaced the pause with a two-step Streamlit workflow
- Split API access, processing, and orchestration into separate modules
- Added validation and clearer error handling
- Added downloadable outputs in the UI
- Added a ZIP bundle for all generated files
- Removed unused local-only configuration persistence behavior

## Notes

This refactor intentionally follows the behavior of the current uploaded script, which runs across **all regions/depots in the account** rather than prompting for a single depot.
```

## optibus_payroll_compare/__init__.py

```python
"""Optibus payroll compare package."""

from .api import OptibusClient, OptibusError
from .models import PostRunResult, PreRunResult, RunParameters
from .pipeline import run_post_compare, run_pre_fetch

__all__ = [
    "OptibusClient",
    "OptibusError",
    "PostRunResult",
    "PreRunResult",
    "RunParameters",
    "run_pre_fetch",
    "run_post_compare",
]
```

## optibus_payroll_compare/api.py

```python
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
    """Fetch one date range and recursively split drivers/dates if the API rejects the request size."""
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

    payload = client.get_json("/v2/payroll", params=params, allow_413=True)

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
```

## optibus_payroll_compare/models.py

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class DriverInfo:
    """Basic driver metadata used to map IDs and names across endpoints."""

    external_id: str
    uuid: str
    first_name: str
    last_name: str
    depot_name: str
    region_name: str


@dataclass(frozen=True)
class RunParameters:
    """User-provided runtime inputs for a comparison run."""

    base_url: str
    api_key: str
    api_client: str
    start_date: str
    end_date: str
    batch_days: Optional[int] = None
    driver_chunk_size: Optional[int] = None
    paycodes_csv: str = ""
    tolerance: Optional[float] = None
    should_use_cache: bool = False

    @property
    def paycodes(self) -> list[str]:
        """Return paycodes as a cleaned list."""
        return [item.strip() for item in self.paycodes_csv.split(",") if item.strip()]


@dataclass
class PreRunResult:
    """Artifacts and metadata created during the PRE fetch step."""

    output_dir: Path
    run_id: str
    start_date: str
    end_date: str
    batch_days: int
    driver_chunk_size: int
    region_count: int
    driver_count: int
    pre_tag: str
    pre_payroll_path: Path
    pre_absences_path: Path
    pre_allocation_actual_path: Path
    pre_allocation_planned_path: Path
    payroll_rows: int
    absences_rows: int

    def files(self) -> list[Path]:
        return [
            self.pre_payroll_path,
            self.pre_absences_path,
            self.pre_allocation_actual_path,
            self.pre_allocation_planned_path,
        ]


@dataclass
class PostRunResult:
    """Artifacts created during the POST fetch, diff, and enrichment step."""

    post_payroll_path: Path
    differences_path: Path
    enriched_differences_path: Path
    zip_path: Path
    payroll_rows: int
    differences_rows: int
    enriched_rows: int

    def files(self, pre_result: PreRunResult) -> list[Path]:
        return pre_result.files() + [
            self.post_payroll_path,
            self.differences_path,
            self.enriched_differences_path,
            self.zip_path,
        ]
```

## optibus_payroll_compare/pipeline.py

```python
from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from typing import Callable

from .api import OptibusClient, build_driver_maps, fetch_absences, fetch_all_drivers, fetch_payroll_chunked, fetch_regions
from .models import PostRunResult, PreRunResult, RunParameters
from .processing import (
    compute_diffs,
    create_zip_archive,
    enrich_differences,
    save_absences_csv,
    save_allocation_csvs,
    save_payroll_csv,
    to_payroll_rows_from_payroll_api,
)
from .utils import auto_tune_chunking, ensure_directory, parse_iso_date

LogFn = Callable[[str], None]


def validate_parameters(params: RunParameters) -> tuple:
    """Validate required fields and parse dates."""
    missing = []
    if not params.base_url.strip():
        missing.append("base_url")
    if not params.api_key.strip():
        missing.append("api_key")
    if not params.api_client.strip():
        missing.append("api_client")
    if not params.start_date.strip():
        missing.append("start_date")
    if not params.end_date.strip():
        missing.append("end_date")

    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")

    start = parse_iso_date(params.start_date)
    end = parse_iso_date(params.end_date)
    if end < start:
        raise ValueError("end_date must be greater than or equal to start_date")

    return start, end


def _resolve_runtime_settings(driver_count: int, total_days: int, params: RunParameters) -> tuple[int, int]:
    """Resolve batch sizes, using auto-tuning when values are not provided."""
    tuned_batch_days, tuned_driver_chunk = auto_tune_chunking(driver_count, total_days)
    batch_days = params.batch_days if params.batch_days and params.batch_days > 0 else tuned_batch_days
    driver_chunk_size = (
        params.driver_chunk_size if params.driver_chunk_size and params.driver_chunk_size > 0 else tuned_driver_chunk
    )
    return int(batch_days), int(driver_chunk_size)


def run_pre_fetch(params: RunParameters, output_dir: Path, log: LogFn = print) -> PreRunResult:
    """Run the PRE stage and save payroll, absences, and allocation files."""
    start, end = validate_parameters(params)
    output_dir = ensure_directory(output_dir)

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    pre_tag = f"pre_{params.start_date}_to_{params.end_date}_{run_id}"

    client = OptibusClient(params.base_url, params.api_key, params.api_client)

    log("Fetching regions/depots...")
    regions = fetch_regions(client)
    try:
        region_count = len(regions)
    except Exception:
        region_count = 0

    log("Fetching drivers for ID/name mapping...")
    drivers_payload = fetch_all_drivers(client, on_date=params.start_date)
    by_uuid, by_external_id = build_driver_maps(drivers_payload)

    driver_ids = list(by_external_id.keys())
    if not driver_ids:
        raise ValueError("No drivers were returned by /v2/drivers for the selected start date.")

    total_days = (end - start).days + 1
    batch_days, driver_chunk_size = _resolve_runtime_settings(len(driver_ids), total_days, params)

    log(f"Running across all regions/depots in this account: {region_count}")
    log(f"Drivers considered: {len(driver_ids):,}")
    log(f"Using batch_days={batch_days} and driver_chunk_size={driver_chunk_size}")

    log("PRE: Fetching payroll...")
    payroll_pre_records = fetch_payroll_chunked(
        client=client,
        start=start,
        end=end,
        driver_ids=driver_ids,
        batch_days=batch_days,
        should_use_cache=params.should_use_cache,
        driver_chunk_size=driver_chunk_size,
        paycodes=params.paycodes or None,
        log=log,
    )
    payroll_pre_rows = to_payroll_rows_from_payroll_api(payroll_pre_records)
    pre_payroll_path = output_dir / f"{pre_tag}_payroll.csv"
    payroll_rows = save_payroll_csv(payroll_pre_rows, pre_payroll_path)

    log("PRE: Fetching absences...")
    absences_records = fetch_absences(client, start=start, end=end)
    pre_absences_path = output_dir / f"{pre_tag}_driver_absences.csv"
    absences_rows = save_absences_csv(absences_records, by_external_id=by_external_id, out_path=pre_absences_path)

    log("PRE: Fetching actual and planned allocations...")
    pre_allocation_actual_path = output_dir / f"{pre_tag}_driver_allocation_actual.csv"
    pre_allocation_planned_path = output_dir / f"{pre_tag}_driver_allocation_planned.csv"
    save_allocation_csvs(
        client=client,
        by_uuid=by_uuid,
        driver_external_ids=driver_ids,
        start=start,
        end=end,
        batch_days=batch_days,
        out_actual_path=pre_allocation_actual_path,
        out_planned_path=pre_allocation_planned_path,
    )

    return PreRunResult(
        output_dir=output_dir,
        run_id=run_id,
        start_date=params.start_date,
        end_date=params.end_date,
        batch_days=batch_days,
        driver_chunk_size=driver_chunk_size,
        region_count=region_count,
        driver_count=len(driver_ids),
        pre_tag=pre_tag,
        pre_payroll_path=pre_payroll_path,
        pre_absences_path=pre_absences_path,
        pre_allocation_actual_path=pre_allocation_actual_path,
        pre_allocation_planned_path=pre_allocation_planned_path,
        payroll_rows=payroll_rows,
        absences_rows=absences_rows,
    )


def run_post_compare(
    params: RunParameters,
    pre_result: PreRunResult,
    log: LogFn = print,
    propagation_wait_seconds: int = 5,
) -> PostRunResult:
    """Run the POST stage, then compute and enrich differences."""
    start, end = validate_parameters(params)

    client = OptibusClient(params.base_url, params.api_key, params.api_client)

    if propagation_wait_seconds > 0:
        log(f"Waiting {propagation_wait_seconds} seconds before POST fetch...")
        time.sleep(propagation_wait_seconds)

    log("POST: Fetching payroll...")
    drivers_payload = fetch_all_drivers(client, on_date=params.start_date)
    _, by_external_id = build_driver_maps(drivers_payload)
    driver_ids = list(by_external_id.keys())

    payroll_post_records = fetch_payroll_chunked(
        client=client,
        start=start,
        end=end,
        driver_ids=driver_ids,
        batch_days=pre_result.batch_days,
        should_use_cache=False,
        driver_chunk_size=pre_result.driver_chunk_size,
        paycodes=params.paycodes or None,
        log=log,
    )
    payroll_post_rows = to_payroll_rows_from_payroll_api(payroll_post_records)
    post_tag = f"post_{params.start_date}_to_{params.end_date}_{pre_result.run_id}"
    post_payroll_path = pre_result.output_dir / f"{post_tag}_payroll.csv"
    post_payroll_rows = save_payroll_csv(payroll_post_rows, post_payroll_path)

    log("Computing differences...")
    differences_path = (
        pre_result.output_dir
        / f"payroll_differences_{params.start_date}_to_{params.end_date}_{pre_result.run_id}.csv"
    )
    differences_rows = compute_diffs(
        file1=str(pre_result.pre_payroll_path),
        file2=str(post_payroll_path),
        out_csv=str(differences_path),
        tolerance=params.tolerance,
    )

    log("Enriching differences...")
    enriched_differences_path = (
        pre_result.output_dir
        / f"payroll_differences_enriched_{params.start_date}_to_{params.end_date}_{pre_result.run_id}.csv"
    )
    enriched_rows = enrich_differences(
        amount_path=differences_path,
        absences_path=pre_result.pre_absences_path,
        allocation_actual_path=pre_result.pre_allocation_actual_path,
        allocation_planned_path=pre_result.pre_allocation_planned_path,
        out_path=enriched_differences_path,
    )

    zip_path = pre_result.output_dir / f"optibus_payroll_compare_{pre_result.run_id}.zip"
    create_zip_archive(
        file_paths=[
            pre_result.pre_payroll_path,
            pre_result.pre_absences_path,
            pre_result.pre_allocation_actual_path,
            pre_result.pre_allocation_planned_path,
            post_payroll_path,
            differences_path,
            enriched_differences_path,
        ],
        zip_path=zip_path,
    )

    return PostRunResult(
        post_payroll_path=post_payroll_path,
        differences_path=differences_path,
        enriched_differences_path=enriched_differences_path,
        zip_path=zip_path,
        payroll_rows=post_payroll_rows,
        differences_rows=differences_rows,
        enriched_rows=enriched_rows,
    )
```

## optibus_payroll_compare/processing.py

```python
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
```

## optibus_payroll_compare/utils.py

```python
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
```

## requirements.txt

```
pandas>=2.2.0
requests>=2.32.0
streamlit>=1.44.0
python-dotenv>=1.0.1
```

## streamlit_app.py

```python
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from optibus_payroll_compare.models import RunParameters
from optibus_payroll_compare.pipeline import run_post_compare, run_pre_fetch
from optibus_payroll_compare.utils import mask_api_key

load_dotenv()

st.set_page_config(page_title="Optibus Payroll Compare", layout="wide")
st.title("Optibus Payroll Compare")
st.caption(
    "Fetch PRE payroll data, make your Work Entity changes in Optibus, then fetch POST payroll and generate "
    "differences plus enriched outputs."
)

if "pre_result" not in st.session_state:
    st.session_state.pre_result = None
if "post_result" not in st.session_state:
    st.session_state.post_result = None
if "output_dir" not in st.session_state:
    st.session_state.output_dir = tempfile.mkdtemp(prefix="optibus_payroll_compare_")


def build_params() -> RunParameters:
    """Read user inputs into a RunParameters object."""
    return RunParameters(
        base_url=st.session_state.base_url,
        api_key=st.session_state.api_key,
        api_client=st.session_state.api_client,
        start_date=st.session_state.start_date.isoformat() if st.session_state.start_date else "",
        end_date=st.session_state.end_date.isoformat() if st.session_state.end_date else "",
        batch_days=int(st.session_state.batch_days) if st.session_state.batch_days else None,
        driver_chunk_size=int(st.session_state.driver_chunk_size) if st.session_state.driver_chunk_size else None,
        paycodes_csv=st.session_state.paycodes_csv.strip(),
        tolerance=float(st.session_state.tolerance) if st.session_state.tolerance else None,
        should_use_cache=st.session_state.should_use_cache,
    )


def make_logger(container):
    """Create a logger that appends messages to a Streamlit code block."""
    messages: list[str] = []

    def log(message: str) -> None:
        messages.append(message)
        container.code("\n".join(messages))

    return log


with st.sidebar:
    st.subheader("Connection")
    st.text_input(
        "Base URL",
        key="base_url",
        value=os.getenv("OPTIBUS_BASE_URL", ""),
        help="Example: https://YOUR-ACCOUNT.api.ops.optibus.co",
    )
    st.text_input(
        "API Client",
        key="api_client",
        value=os.getenv("OPTIBUS_API_CLIENT", ""),
        help='Value for the X-Optibus-Api-Client header.',
    )
    st.text_input(
        "API Key",
        key="api_key",
        value=os.getenv("OPTIBUS_API_KEY", ""),
        type="password",
        help="Stored only in this session unless you use Streamlit secrets or environment variables.",
    )

    st.subheader("Run options")
    st.date_input("Start date", key="start_date", value=None, format="YYYY-MM-DD")
    st.date_input("End date", key="end_date", value=None, format="YYYY-MM-DD")
    st.text_input(
        "Paycodes (optional)",
        key="paycodes_csv",
        value="",
        help="Comma-separated paycodes. Leave blank to fetch all paycodes.",
    )
    st.number_input(
        "Batch days (optional override)",
        key="batch_days",
        min_value=0,
        value=0,
        step=1,
        help="Use 0 to let the app auto-tune this value.",
    )
    st.number_input(
        "Driver chunk size (optional override)",
        key="driver_chunk_size",
        min_value=0,
        value=0,
        step=1,
        help="Use 0 to let the app auto-tune this value.",
    )
    st.number_input(
        "Numeric diff tolerance (optional)",
        key="tolerance",
        min_value=0.0,
        value=0.0,
        step=0.01,
        help="Used only for numeric comparisons. Leave at 0 if you do not need tolerance.",
    )
    st.checkbox(
        "Use cached payroll results during PRE",
        key="should_use_cache",
        value=False,
        help="POST always forces recalculation to capture the latest Work Entity changes.",
    )

    if st.button("Clear session state", use_container_width=True):
        st.session_state.pre_result = None
        st.session_state.post_result = None
        st.rerun()

left, right = st.columns([1, 1])

with left:
    st.subheader("Step 1: PRE fetch")
    st.write("Run this first to capture baseline payroll, absences, and allocation files.")
    if st.session_state.api_key:
        st.info(f"Using API key: {mask_api_key(st.session_state.api_key)}")
    if st.button("Run PRE fetch", type="primary", use_container_width=True):
        params = build_params()
        log_box = st.empty()
        logger = make_logger(log_box)
        try:
            output_dir = Path(st.session_state.output_dir)
            pre_result = run_pre_fetch(params=params, output_dir=output_dir, log=logger)
            st.session_state.pre_result = pre_result
            st.session_state.post_result = None
            st.success("PRE fetch complete.")
        except Exception as exc:
            st.exception(exc)

with right:
    st.subheader("Step 2: POST fetch and compare")
    st.write(
        "After you update Work Entities in the Optibus web UI, run this step to fetch POST payroll and create the "
        "differences files."
    )
    if st.session_state.pre_result is None:
        st.warning("Run the PRE fetch first.")
    else:
        st.success("PRE artifacts are ready. Keep this browser session open until you finish POST.")
        if st.button("Run POST fetch + compare", use_container_width=True):
            params = build_params()
            log_box = st.empty()
            logger = make_logger(log_box)
            try:
                post_result = run_post_compare(params=params, pre_result=st.session_state.pre_result, log=logger)
                st.session_state.post_result = post_result
                st.success("POST fetch and comparison complete.")
            except Exception as exc:
                st.exception(exc)

if st.session_state.pre_result is not None:
    pre_result = st.session_state.pre_result
    st.divider()
    st.subheader("PRE artifacts")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Regions", pre_result.region_count)
    col2.metric("Drivers", pre_result.driver_count)
    col3.metric("Payroll rows", pre_result.payroll_rows)
    col4.metric("Absence rows", pre_result.absences_rows)

    for file_path in pre_result.files():
        with open(file_path, "rb") as handle:
            st.download_button(
                label=f"Download {file_path.name}",
                data=handle.read(),
                file_name=file_path.name,
                mime="text/csv",
            )

if st.session_state.post_result is not None:
    post_result = st.session_state.post_result
    st.divider()
    st.subheader("POST artifacts")
    col1, col2, col3 = st.columns(3)
    col1.metric("POST payroll rows", post_result.payroll_rows)
    col2.metric("Difference rows", post_result.differences_rows)
    col3.metric("Enriched rows", post_result.enriched_rows)

    for file_path in [
        post_result.post_payroll_path,
        post_result.differences_path,
        post_result.enriched_differences_path,
    ]:
        mime = "text/csv"
        with open(file_path, "rb") as handle:
            st.download_button(
                label=f"Download {file_path.name}",
                data=handle.read(),
                file_name=file_path.name,
                mime=mime,
            )

    with open(post_result.zip_path, "rb") as handle:
        st.download_button(
            label="Download all outputs (.zip)",
            data=handle.read(),
            file_name=post_result.zip_path.name,
            mime="application/zip",
            type="primary",
        )

    st.subheader("Enriched differences preview")
    preview_df = pd.read_csv(post_result.enriched_differences_path, encoding="utf-8-sig").head(100)
    st.dataframe(preview_df, use_container_width=True)

st.divider()
st.markdown(
    """
**Notes**

- This app intentionally follows the current script behavior and runs across all regions/depots in the account.
- The original Mac-only AppleScript dialogs and Keychain persistence were removed because they are not suitable for Streamlit deployment.
- For Streamlit Community Cloud, put credentials in the app secrets or environment variables rather than typing them every time.
"""
)
```

## Summary of changes from the original script

- Separated UI, orchestration, API access, and processing logic.
- Replaced AppleScript/CLI interaction with a Streamlit two-step PRE/POST flow.
- Removed macOS-specific settings persistence and Keychain handling from the app runtime.
- Preserved the core output workflow and CSV shapes used by the original script.
