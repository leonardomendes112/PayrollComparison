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
