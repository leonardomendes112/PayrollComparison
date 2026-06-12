from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from typing import Callable

from .api import (
    OptibusClient,
    build_driver_maps,
    fetch_absences,
    fetch_all_drivers,
    fetch_driver_day_labels,
    fetch_payroll_chunked,
    fetch_regions,
    fetch_work_entities_chunked,
)
from .models import PostRunResult, PreRunResult, RunParameters, WorkEntitiesExportResult
from .processing import (
    create_duty_branch_mismatch_report,
    compute_diffs,
    create_zip_archive,
    enrich_differences,
    load_difference_driver_days,
    save_absences_csv,
    save_allocation_csvs,
    save_driver_day_labels_csv,
    save_payroll_csv,
    save_work_entities_csv,
    to_payroll_rows_from_payroll_api,
)
from .utils import ensure_directory, parse_iso_date

LogFn = Callable[[str], None]
DEFAULT_BATCH_DAYS = 31
DEFAULT_DRIVER_CHUNK_SIZE = 7
DEFAULT_MAX_PARALLEL_REQUESTS = 20


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


def _resolve_runtime_settings(driver_count: int, total_days: int, params: RunParameters) -> tuple[int, int, int]:
    """Resolve batch sizes and concurrency with capped defaults."""
    requested_batch_days = params.batch_days if params.batch_days and params.batch_days > 0 else DEFAULT_BATCH_DAYS
    requested_driver_chunk_size = (
        params.driver_chunk_size if params.driver_chunk_size and params.driver_chunk_size > 0 else DEFAULT_DRIVER_CHUNK_SIZE
    )
    requested_max_parallel_requests = (
        params.max_parallel_requests
        if params.max_parallel_requests and params.max_parallel_requests > 0
        else DEFAULT_MAX_PARALLEL_REQUESTS
    )

    batch_days = min(int(requested_batch_days), max(1, total_days))
    driver_chunk_size = min(int(requested_driver_chunk_size), max(1, driver_count))
    estimated_batch_count = max(1, ((total_days - 1) // batch_days) + 1)
    estimated_chunk_count = max(1, ((driver_count - 1) // driver_chunk_size) + 1)
    max_parallel_requests = min(int(requested_max_parallel_requests), estimated_batch_count * estimated_chunk_count)
    return batch_days, driver_chunk_size, max_parallel_requests


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
    batch_days, driver_chunk_size, max_parallel_requests = _resolve_runtime_settings(len(driver_ids), total_days, params)

    log(f"Running across all regions/depots in this account: {region_count}")
    log(f"Drivers considered: {len(driver_ids):,}")
    log(
        f"Using batch_days={batch_days}, driver_chunk_size={driver_chunk_size}, "
        f"max_parallel_requests={max_parallel_requests}"
    )

    log("PRE: Fetching payroll...")
    payroll_pre_records = fetch_payroll_chunked(
        client=client,
        start=start,
        end=end,
        driver_ids=driver_ids,
        batch_days=batch_days,
        should_use_cache=params.should_use_cache,
        driver_chunk_size=driver_chunk_size,
        max_workers=max_parallel_requests,
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

    log("PRE: Fetching driver day labels...")
    driver_day_labels_records = fetch_driver_day_labels(client, start=start, end=end)
    pre_driver_day_labels_path = output_dir / f"{pre_tag}_driver_day_labels.csv"
    driver_day_label_rows = save_driver_day_labels_csv(
        driver_day_labels_records,
        by_uuid=by_uuid,
        out_path=pre_driver_day_labels_path,
    )

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
        max_parallel_requests=max_parallel_requests,
        region_count=region_count,
        driver_count=len(driver_ids),
        pre_tag=pre_tag,
        pre_payroll_path=pre_payroll_path,
        pre_absences_path=pre_absences_path,
        pre_driver_day_labels_path=pre_driver_day_labels_path,
        pre_allocation_actual_path=pre_allocation_actual_path,
        pre_allocation_planned_path=pre_allocation_planned_path,
        payroll_rows=payroll_rows,
        absences_rows=absences_rows,
        driver_day_label_rows=driver_day_label_rows,
    )


def run_post_compare(
    params: RunParameters,
    pre_result: PreRunResult,
    log: LogFn = print,
    propagation_wait_seconds: int = 45,
) -> PostRunResult:
    """Run the POST stage, then compute and enrich differences."""
    start, end = validate_parameters(params)

    client = OptibusClient(params.base_url, params.api_key, params.api_client)

    if propagation_wait_seconds > 0:
        log(f"Waiting {propagation_wait_seconds} seconds before POST fetch...")
        time.sleep(propagation_wait_seconds)

    log("POST: Fetching payroll...")
    drivers_payload = fetch_all_drivers(client, on_date=params.start_date)
    by_uuid, by_external_id = build_driver_maps(drivers_payload)
    driver_ids = list(by_external_id.keys())
    total_days = (end - start).days + 1
    _, _, max_parallel_requests = _resolve_runtime_settings(len(driver_ids), total_days, params)

    payroll_post_records = fetch_payroll_chunked(
        client=client,
        start=start,
        end=end,
        driver_ids=driver_ids,
        batch_days=pre_result.batch_days,
        should_use_cache=False,
        driver_chunk_size=pre_result.driver_chunk_size,
        max_workers=max_parallel_requests,
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
        driver_day_labels_path=pre_result.pre_driver_day_labels_path,
        allocation_actual_path=pre_result.pre_allocation_actual_path,
        allocation_planned_path=pre_result.pre_allocation_planned_path,
        out_path=enriched_differences_path,
    )

    duty_branch_report_path = None
    duty_branch_report_rows = 0
    if params.check_duty_branch_mismatches:
        log("POST: Fetching driver day labels for duty payroll comparison...")
        post_driver_day_labels_path = pre_result.output_dir / f"{post_tag}_driver_day_labels.csv"
        post_driver_day_labels_records = fetch_driver_day_labels(client, start=start, end=end)
        save_driver_day_labels_csv(
            post_driver_day_labels_records,
            by_uuid=by_uuid,
            out_path=post_driver_day_labels_path,
        )

        log("POST: Fetching actual and planned allocations for duty payroll comparison...")
        post_allocation_actual_path = pre_result.output_dir / f"{post_tag}_driver_allocation_actual.csv"
        post_allocation_planned_path = pre_result.output_dir / f"{post_tag}_driver_allocation_planned.csv"
        save_allocation_csvs(
            client=client,
            by_uuid=by_uuid,
            driver_external_ids=driver_ids,
            start=start,
            end=end,
            batch_days=pre_result.batch_days,
            out_actual_path=post_allocation_actual_path,
            out_planned_path=post_allocation_planned_path,
        )

        log("Checking planned versus actual duty payroll results...")
        duty_branch_report_path = (
            pre_result.output_dir
            / f"duty_branch_mismatches_{params.start_date}_to_{params.end_date}_{pre_result.run_id}.csv"
        )
        duty_branch_report_rows = create_duty_branch_mismatch_report(
            payroll_path=post_payroll_path,
            driver_day_labels_path=post_driver_day_labels_path,
            allocation_actual_path=post_allocation_actual_path,
            allocation_planned_path=post_allocation_planned_path,
            out_path=duty_branch_report_path,
        )

    zip_path = pre_result.output_dir / f"optibus_payroll_compare_{pre_result.run_id}.zip"
    file_paths = [
        pre_result.pre_payroll_path,
        pre_result.pre_absences_path,
        pre_result.pre_driver_day_labels_path,
        pre_result.pre_allocation_actual_path,
        pre_result.pre_allocation_planned_path,
        post_payroll_path,
        differences_path,
        enriched_differences_path,
    ]
    if duty_branch_report_path is not None:
        file_paths.append(duty_branch_report_path)
    create_zip_archive(
        file_paths=file_paths,
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
        max_parallel_requests=max_parallel_requests,
        duty_branch_report_path=duty_branch_report_path,
        duty_branch_report_rows=duty_branch_report_rows,
    )


def export_difference_work_entities(
    params: RunParameters,
    pre_result: PreRunResult,
    post_result: PostRunResult,
    log: LogFn = print,
) -> WorkEntitiesExportResult:
    """Export work entities for the driver-days that appear in the payroll differences file."""
    start, end = validate_parameters(params)
    client = OptibusClient(params.base_url, params.api_key, params.api_client)

    diff_driver_days = load_difference_driver_days(post_result.differences_path)
    if not diff_driver_days:
        out_path = (
            pre_result.output_dir
            / f"work_entities_for_payroll_differences_{params.start_date}_to_{params.end_date}_{pre_result.run_id}.csv"
        )
        save_work_entities_csv(records=[], by_uuid={}, target_driver_days=set(), out_path=out_path)
        return WorkEntitiesExportResult(
            work_entities_path=out_path,
            work_entities_rows=0,
            driver_day_count=0,
        )

    drivers_payload = fetch_all_drivers(client, on_date=params.start_date)
    by_uuid, by_external_id = build_driver_maps(drivers_payload)

    grouped_dates: dict[str, list[str]] = {}
    for driver_id, date_text in diff_driver_days:
        grouped_dates.setdefault(date_text, []).append(driver_id)

    work_entity_records: list[dict] = []
    for date_text in sorted(grouped_dates):
        missing_driver_ids = sorted({driver_id for driver_id in grouped_dates[date_text] if driver_id not in by_external_id})
        if missing_driver_ids:
            log(
                f"Skipping {len(missing_driver_ids)} driver IDs on {date_text} because they could not be mapped back "
                "to current driver UUIDs."
            )
        driver_uuids = sorted(
            {
                info.uuid
                for driver_id in grouped_dates[date_text]
                for info in [by_external_id.get(driver_id)]
                if info and info.uuid
            }
        )
        if not driver_uuids:
            continue
        day = parse_iso_date(date_text)
        log(f"Fetching work entities for {date_text} across {len(driver_uuids)} drivers with differences...")
        work_entity_records.extend(
            fetch_work_entities_chunked(
                client=client,
                start=day,
                end=day,
                driver_uuids=driver_uuids,
                batch_days=1,
                should_use_cache=False,
                max_workers=min(post_result.max_parallel_requests, max(1, len(driver_uuids))),
                export_all_entities=True,
                log=log,
            )
        )

    out_path = (
        pre_result.output_dir
        / f"work_entities_for_payroll_differences_{params.start_date}_to_{params.end_date}_{pre_result.run_id}.csv"
    )
    target_driver_days = set(diff_driver_days)
    work_entities_rows = save_work_entities_csv(
        records=work_entity_records,
        by_uuid=by_uuid,
        target_driver_days=target_driver_days,
        out_path=out_path,
    )
    return WorkEntitiesExportResult(
        work_entities_path=out_path,
        work_entities_rows=work_entities_rows,
        driver_day_count=len(target_driver_days),
    )
