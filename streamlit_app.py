from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from optibus_payroll_compare.models import RunParameters
from optibus_payroll_compare.pipeline import export_difference_work_entities, run_post_compare, run_pre_fetch
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
if "work_entities_export_result" not in st.session_state:
    st.session_state.work_entities_export_result = None
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
        max_parallel_requests=int(st.session_state.max_parallel_requests) if st.session_state.max_parallel_requests else None,
        paycodes_csv=st.session_state.paycodes_csv.strip(),
        tolerance=float(st.session_state.tolerance) if st.session_state.tolerance else None,
        should_use_cache=st.session_state.should_use_cache,
        check_duty_branch_mismatches=st.session_state.check_duty_branch_mismatches,
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
        min_value=1,
        value=31,
        step=1,
        help="Default is 31. The app caps this automatically when the selected period is shorter.",
    )
    st.number_input(
        "Driver chunk size (optional override)",
        key="driver_chunk_size",
        min_value=1,
        value=7,
        step=1,
        help="Default is 7. The app caps this automatically when fewer drivers are in scope.",
    )
    st.number_input(
        "Parallel payroll calls",
        key="max_parallel_requests",
        min_value=1,
        max_value=50,
        value=20,
        step=1,
        help="Default is 20. The app caps this automatically when fewer payroll chunks exist.",
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
    st.checkbox(
        "Check planned vs actual duty payroll",
        key="check_duty_branch_mismatches",
        value=False,
        help=(
            "Creates an extra CSV that compares POST payroll results for each duty/date across planned and actual "
            "allocations, with driver-day labels included as context."
        ),
    )

    if st.button("Clear session state", use_container_width=True):
        st.session_state.pre_result = None
        st.session_state.post_result = None
        st.session_state.work_entities_export_result = None
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
            st.session_state.work_entities_export_result = None
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
                st.session_state.work_entities_export_result = None
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
    st.caption(
        f"Run settings used: batch_days={pre_result.batch_days}, "
        f"driver_chunk_size={pre_result.driver_chunk_size}, "
        f"parallel_calls={pre_result.max_parallel_requests}"
    )

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
    st.caption(f"POST payroll fetch used parallel_calls={post_result.max_parallel_requests}")
    if post_result.duty_branch_report_path is not None:
        st.metric("Duty payroll mismatches", post_result.duty_branch_report_rows)

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

    if post_result.duty_branch_report_path is not None:
        with open(post_result.duty_branch_report_path, "rb") as handle:
            st.download_button(
                label=f"Download {post_result.duty_branch_report_path.name}",
                data=handle.read(),
                file_name=post_result.duty_branch_report_path.name,
                mime="text/csv",
            )

    with open(post_result.zip_path, "rb") as handle:
        st.download_button(
            label="Download all outputs (.zip)",
            data=handle.read(),
            file_name=post_result.zip_path.name,
            mime="application/zip",
            type="primary",
        )

    st.subheader("Troubleshooting export")
    st.write(
        "If needed, extract the work entities for the driver-days where payroll changed between PRE and POST."
    )
    if st.button("Extract work entities for changed driver-days", use_container_width=True):
        params = build_params()
        log_box = st.empty()
        logger = make_logger(log_box)
        try:
            export_result = export_difference_work_entities(
                params=params,
                pre_result=st.session_state.pre_result,
                post_result=post_result,
                log=logger,
            )
            st.session_state.work_entities_export_result = export_result
            st.success("Work entities export complete.")
        except Exception as exc:
            st.exception(exc)

    if st.session_state.work_entities_export_result is not None:
        export_result = st.session_state.work_entities_export_result
        col1, col2 = st.columns(2)
        col1.metric("Driver-days exported", export_result.driver_day_count)
        col2.metric("Work entity rows", export_result.work_entities_rows)
        with open(export_result.work_entities_path, "rb") as handle:
            st.download_button(
                label=f"Download {export_result.work_entities_path.name}",
                data=handle.read(),
                file_name=export_result.work_entities_path.name,
                mime="text/csv",
            )

    st.subheader("Enriched differences preview")
    preview_df = pd.read_csv(
        post_result.enriched_differences_path,
        encoding="utf-8-sig",
        keep_default_na=False,
    ).fillna("").head(100)
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
