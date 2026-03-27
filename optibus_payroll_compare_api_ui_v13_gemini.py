#!/usr/bin/env python3
"""
optibus_payroll_compare_api.py

One-command pipeline to:
  1) Fetch payroll entities + driver absences + driver allocation for a date range (PRE)
  2) Pause for you to update Work Entities in the Optibus web UI
  3) Fetch the same data again (POST)
  4) Output:
       - pre_payroll.csv
       - post_payroll.csv
       - payroll_differences.csv
       - payroll_differences_enriched.csv  (differences + Absences + Allocation)

It is designed to preserve the SAME CSV shapes your existing scripts expect:
  - Payroll CSV columns: DriverID, Date, Code, Amount, Time Unit
  - Differences CSV columns: DriverID, Date, Code, Time Unit, Change, Pre-changes, Post-changes
  - Absences CSV columns: Driver Id, Driver Name, Depot Name, Absence code, Start date, Start time, End date, End time, Note
  - Allocation CSV: wide matrix with "Driver ID" + one column per date formatted as YYYY-MM-DD

Auth headers (per Optibus docs):
  Authorization: <API_KEY>
  X-Optibus-Api-Client: <ACCOUNT_NAME>

Docs (reference):
  - GET /v2/payroll
  - GET /v2/drivers
  - GET /v2/drivers/absences
  - GET /v1/regions
  - GET /v2/operational-plan (for Actual vs Planned allocations)
  - GET /v1/calendar-driver-day-labels (labels like OFF)

Usage (example):
  export OPTIBUS_API_KEY="***"
  python3 optibus_payroll_compare_api.py \
    --base-url "https://YOUR-ACCOUNT.api.ops.optibus.co" \
    --api-client "ADO" \
    --start-date 2026-01-01 --end-date 2026-04-01 \
    --batch-days 20 \
    --depot-name "Lemoa" \
    --out-dir "./out"

Notes:
  - This script never prints your API key.
  - If /v2/payroll returns HTTP 413, the script auto-splits drivers and/or dates.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import re
import getpass
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from collections import defaultdict, Counter

import warnings

# Suppress noisy urllib3 warning on macOS system Python (LibreSSL). 
# This MUST happen before we import requests or urllib3.
warnings.filterwarnings("ignore", message=".*LibreSSL.*")
warnings.filterwarnings("ignore", message=".*NotOpenSSLWarning.*")
warnings.filterwarnings("ignore", category=UserWarning, module="urllib3")

import ssl
import requests
import pandas as pd


def mask_api_key(key: str, keep: int = 6) -> str:
    key = (key or "").strip()
    if not key:
        return ""
    if len(key) <= keep:
        return "*" * len(key)
    return ("*" * (len(key) - keep)) + key[-keep:]


def tls_stack_info() -> str:
    return getattr(ssl, "OPENSSL_VERSION", "unknown")


# -----------------------------
# Persisted settings (Mac-friendly)
# -----------------------------
CONFIG_DIR = Path.home() / ".optibus_payroll_compare_api"
CONFIG_FILE = CONFIG_DIR / "settings.json"
KEYCHAIN_SERVICE = "optibus-payroll-compare-api"


def _load_settings_file() -> Dict[str, Any]:
    try:
        if CONFIG_FILE.exists():
            with CONFIG_FILE.open("r", encoding="utf-8") as f:
                return json.load(f) or {}
    except Exception:
        return {}
    return {}


def _save_settings_file(cfg: Dict[str, Any]) -> None:
    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        # best-effort tighten permissions (esp. if user opts to store token in file on non-mac)
        try:
            os.chmod(CONFIG_DIR, 0o700)
        except Exception:
            pass
        tmp = CONFIG_FILE.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, sort_keys=True)
        tmp.replace(CONFIG_FILE)
        try:
            os.chmod(CONFIG_FILE, 0o600)
        except Exception:
            pass
    except Exception:
        # non-fatal
        return


def _keychain_get(account: str) -> Optional[str]:
    """Return secret from macOS Keychain for (service, account)."""
    if not _is_mac() or not account:
        return None
    try:
        import subprocess
        p = subprocess.run(
            ["security", "find-generic-password", "-s", KEYCHAIN_SERVICE, "-a", account, "-w"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        val = (p.stdout or "").strip()
        return val or None
    except Exception:
        return None


def _keychain_set(account: str, secret: str) -> bool:
    """Store secret in macOS Keychain for (service, account)."""
    if not _is_mac() or not account:
        return False
    try:
        import subprocess
        # -U updates if exists
        subprocess.run(
            ["security", "add-generic-password", "-U", "-s", KEYCHAIN_SERVICE, "-a", account, "-w", secret],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        return True
    except Exception:
        return False


def _keychain_delete(account: str) -> None:
    if not _is_mac() or not account:
        return
    try:
        import subprocess
        subprocess.run(
            ["security", "delete-generic-password", "-s", KEYCHAIN_SERVICE, "-a", account],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except Exception:
        return


def _apply_saved_settings(args: argparse.Namespace) -> argparse.Namespace:
    """Apply saved settings (base URL, api-client, depot, batch_days, out_dir, api_key via Keychain)."""
    cfg = _load_settings_file()
    profiles = cfg.get("profiles", {}) or {}
    # base URL
    if not getattr(args, "base_url", ""):
        args.base_url = (cfg.get("last_base_url") or "").strip()

    prof = profiles.get(args.base_url, {}) if getattr(args, "base_url", "") else {}
    if not getattr(args, "api_client", ""):
        args.api_client = (prof.get("api_client") or "").strip()
    if not getattr(args, "depot_id", ""):
        args.depot_id = (prof.get("depot_id") or "").strip()
    if not getattr(args, "depot_name", ""):
        args.depot_name = (prof.get("depot_name") or "").strip()

    if not getattr(args, "out_dir", "") or getattr(args, "out_dir", "") == ".":
        last_out = (cfg.get("last_out_dir") or "").strip()
        if last_out:
            args.out_dir = last_out

    # API key: env var already applied by argparse defaults; if still missing, try Keychain by base_url
    if not getattr(args, "api_key", "") and getattr(args, "base_url", ""):
        k = _keychain_get(args.base_url.strip())
        if k:
            args.api_key = k

    return args


def _maybe_persist_settings(args: argparse.Namespace, *, force: bool = False) -> None:
    """Persist base_url + api_client (+ depot) to settings file; api_key to Keychain (macOS).
    Only prompts (GUI/CLI) when something new needs saving.
    """
    if getattr(args, "no_save", False):
        return

    base_url = (getattr(args, "base_url", "") or "").strip()
    if not base_url:
        return

    cfg = _load_settings_file()
    profiles = cfg.get("profiles", {}) or {}
    prof = profiles.get(base_url, {}) or {}

    desired_prof = {
        "api_client": (getattr(args, "api_client", "") or "").strip(),
        "depot_id": (getattr(args, "depot_id", "") or "").strip(),
        "depot_name": (getattr(args, "depot_name", "") or "").strip(),
    }

    needs_profile_save = force or (cfg.get("last_base_url") != base_url) or any(
        (desired_prof.get(k) or "") != (prof.get(k) or "") for k in desired_prof.keys()
    )
    needs_batch_save = force or (isinstance(getattr(args, "batch_days", None), int) and cfg.get("batch_days") != args.batch_days)
    needs_outdir_save = force or ((getattr(args, "out_dir", "") or "").strip() and cfg.get("last_out_dir") != (getattr(args, "out_dir", "") or "").strip())

    needs_keychain_save = False
    if (getattr(args, "api_key", "") or "").strip():
        # save to Keychain only if not already present or force
        needs_keychain_save = force or (_keychain_get(base_url) is None)

    if not (needs_profile_save or needs_keychain_save or needs_batch_save or needs_outdir_save):
        return

    use_gui = _is_mac() and (not getattr(args, "no_gui", False))
    prompt_msg = "Save settings for next time?\n\n" + \
                 f"Base URL: {base_url}\n" + \
                 f"API Client: {(getattr(args, 'api_client', '') or '').strip()}\n" + \
                 ("API Key: (store in Keychain)\n" if needs_keychain_save and _is_mac() else "")

    do_save = True
    if use_gui:
        script = f'''
        tell application "Finder"
            activate
            set theDialog to display dialog "{_as_quote(prompt_msg)}" buttons {{"No","Yes"}} default button "Yes"
            return button returned of theDialog
        end tell
        '''
        choice = (_osascript(script) or "").strip()
        do_save = (choice == "Yes")
    else:
        ans = input(f"{prompt_msg}\nSave? [Y/n]: ").strip().lower()
        do_save = (ans in ("", "y", "yes"))

    if not do_save:
        return

    # settings file
    cfg["last_base_url"] = base_url
    cfg["batch_days"] = getattr(args, "batch_days", 20) if isinstance(getattr(args, "batch_days", None), int) else cfg.get("batch_days", 20)
    out_dir = (getattr(args, "out_dir", "") or "").strip()
    if out_dir:
        cfg["last_out_dir"] = out_dir

    profiles[base_url] = desired_prof
    cfg["profiles"] = profiles
    _save_settings_file(cfg)

    # Keychain
    if needs_keychain_save and _is_mac():
        _keychain_set(base_url, (getattr(args, "api_key", "") or "").strip())


def _forget_saved_settings(args: argparse.Namespace) -> None:
    """Remove saved settings (and Keychain token) for the current/last base URL."""
    cfg = _load_settings_file()
    base_url = (getattr(args, "base_url", "") or "").strip() or (cfg.get("last_base_url") or "").strip()
    if base_url:
        _keychain_delete(base_url)
        profiles = cfg.get("profiles", {}) or {}
        if base_url in profiles:
            profiles.pop(base_url, None)
            cfg["profiles"] = profiles
        if cfg.get("last_base_url") == base_url:
            cfg["last_base_url"] = ""
        _save_settings_file(cfg)

# -----------------------------
# macOS-safe UI helpers (AppleScript)
# -----------------------------
def _is_mac() -> bool:
    return sys.platform == "darwin"

def _forget_all_saved_settings() -> None:
    """Remove saved settings file and any Keychain tokens referenced by profiles."""
    cfg = _load_settings_file()
    profiles = cfg.get("profiles", {}) or {}
    for base_url in list(profiles.keys()):
        try:
            _keychain_delete(base_url)
        except Exception:
            pass
    try:
        if CONFIG_FILE.exists():
            CONFIG_FILE.unlink()
    except Exception:
        pass


def _as_quote(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _osascript(script: str) -> Optional[str]:
    """
    Run AppleScript and return stdout stripped.
    Returns None if user cancels.
    """
    try:
        import subprocess
        p = subprocess.run(
            ["osascript", "-e", script],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        out = (p.stdout or "").strip()
        return out or None
    except Exception:
        return None


def mac_prompt_text(prompt: str, default: str = "") -> Optional[str]:
    script = f'''
    tell application "Finder"
        activate
        set theDialog to display dialog "{_as_quote(prompt)}" default answer "{_as_quote(default)}" buttons {{"Cancel","OK"}} default button "OK"
        return text returned of theDialog
    end tell
    '''
    return _osascript(script)


def mac_choose_folder(prompt: str) -> Optional[str]:
    script = f'''
    tell application "Finder"
        activate
        set folderPath to POSIX path of (choose folder with prompt "{_as_quote(prompt)}")
        return folderPath
    end tell
    '''
    return _osascript(script)


def prompt_text_cli(prompt: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    val = input(f"{prompt}{suffix}: ").strip()
    return val or default

def safe_getpass(prompt: str) -> str:
    """Safely prompt for a password, falling back to visible input if the debugger crashes it."""
    # Detect if we are running inside VS Code's debugger
    if "debugpy" in sys.modules or "pydevd" in sys.modules:
        print(f"\n⚠️ [VS Code Debugger Detected] Hidden text input may not work.")
        print("Falling back to visible input to prevent crashing.")
        return input(prompt + " (Visible): ").strip()
    
    try:
        return getpass.getpass(prompt).strip()
    except Exception:
        # Final fallback if getpass fails for any other terminal reason
        print("\n⚠️ Hidden input not supported in this terminal.")
        return input(prompt + " (Visible): ").strip()


def fill_args_interactively(args: argparse.Namespace) -> argparse.Namespace:
    """
    Fill missing args via AppleScript dialogs on macOS (preferred) or CLI prompts.
    This avoids tkinter which can crash on some Mac Python builds.
    """
    use_gui = _is_mac() and (not getattr(args, "no_gui", False))

    def ask(prompt: str, default: str = "") -> str:
        if use_gui:
            out = mac_prompt_text(prompt, default)
            if out is None:
                # user cancelled -> fall back to CLI so they can still proceed
                return prompt_text_cli(prompt, default)
            return out.strip() or default
        return prompt_text_cli(prompt, default)

    # Base URL
    if not getattr(args, "base_url", ""):
        args.base_url = ask("External API base URL (e.g. https://YOUR-ACCOUNT.api.ops.optibus.co)", "")

    # API client header
    if not getattr(args, "api_client", ""):
        args.api_client = ask('X-Optibus-Api-Client value (e.g. "ADO")', "ADO")

    # API key (allow visible entry on request; never printed in full)
    if not getattr(args, "api_key", ""):
        if use_gui:
            # Ask user if they want to enter the key hidden or visible
            mode_script = f'''
            tell application "Finder"
                activate
                set choice to choose from list {{"Hidden (recommended)", "Visible"}} with prompt "{_as_quote("How do you want to enter the API key?")}" default items {{"Hidden (recommended)"}}
                if choice is false then return ""
                return item 1 of choice
            end tell
            '''
            mode = (_osascript(mode_script) or "").strip()
            if not mode:
                # User hit cancel on the prompt, fall back to safe CLI input
                args.api_key = safe_getpass("API key (will not be printed): ")
            else:
                if mode.startswith("Visible"):
                    out = mac_prompt_text("API key (input will be visible; it will still not be printed):", "")
                    if out is None:
                        args.api_key = prompt_text_cli("API key", "")
                    else:
                        args.api_key = out.strip()
                else:
                    script = f'''
                    tell application "Finder"
                        activate
                        set theDialog to display dialog "{_as_quote("API key (will not be printed):")}" default answer "" with hidden answer buttons {{"Cancel","OK"}} default button "OK"
                        return text returned of theDialog
                    end tell
                    '''
                    out = _osascript(script)
                    if out is None:
                        # Fallback to safe CLI entry if AppleScript popup fails in VS Code
                        args.api_key = safe_getpass("API key (will not be printed): ")
                    else:
                        args.api_key = out.strip()
        else:
            args.api_key = safe_getpass("API key (will not be printed): ")

    # Dates
    if not getattr(args, "start_date", ""):
        args.start_date = ask("Start date (YYYY-MM-DD)", "")
    if not getattr(args, "end_date", ""):
        args.end_date = ask("End date (YYYY-MM-DD)", getattr(args, "start_date", "") or "")

    # Output directory (folder picker)
    if not getattr(args, "out_dir", "") or getattr(args, "out_dir", "") == ".":
        if use_gui:
            picked = mac_choose_folder("Choose an output folder for CSVs")
            if picked:
                args.out_dir = picked
            else:
                args.out_dir = prompt_text_cli("Output directory", ".")
        else:
            args.out_dir = prompt_text_cli("Output directory", ".")

    return args


# -----------------------------
# Constants: CSV columns
# -----------------------------
COL_DRIVER = "DriverID"
COL_DATE = "Date"
COL_CODE = "Code"
COL_AMOUNT = "Amount"
COL_UNIT = "Time Unit"

DIFF_COLS = [COL_DRIVER, COL_DATE, COL_CODE, COL_UNIT, "Change", "Pre-changes", "Post-changes"]


# -----------------------------
# Small utilities
# -----------------------------
def iso_date(d: date) -> str:
    return d.isoformat()


def parse_iso_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def date_batches(start: date, end: date, batch_days: int) -> List[Tuple[date, date]]:
    """Inclusive batches of max batch_days days."""
    if end < start:
        raise ValueError("end-date must be >= start-date")
    out = []
    cur = start
    while cur <= end:
        nxt = min(end, cur + timedelta(days=batch_days - 1))
        out.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return out

def auto_tune_chunking(driver_count: int, total_days: int) -> Tuple[int, int]:
    """Heuristic to pick a fast default that avoids 413 errors."""
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


def excel_date_col(d: date) -> str:
    """CSV header for a date column.

    Use ISO format (YYYY-MM-DD) so Excel opens the CSV without formula parsing.
    """
    return d.isoformat()



def minutes_to_hhmm(total_minutes: Union[float, int]) -> str:
    """Convert minutes to HH:MM (zero-padded), e.g. 430 -> 07:10."""
    try:
        m = int(round(float(total_minutes)))
    except Exception:
        return str(total_minutes)
    sign = "-" if m < 0 else ""
    m = abs(m)
    hh = m // 60
    mm = m % 60
    return f"{sign}{hh:02d}:{mm:02d}"


def safe_str(v: Any) -> str:
    return "" if v is None else str(v)


# -----------------------------
# Excel / CSV compatibility helpers
# -----------------------------
_TIME_RE = re.compile(r"^[+-]?\d{1,3}:\d{2}$")

def excel_sanitize_cell(v: Any) -> str:
    """Make a cell safe to open in Excel.

    Excel treats values starting with = + - @ as formulas in some contexts.
    We prefix a leading apostrophe for time-like negatives (e.g. -7:10) and formula-like prefixes.
    Excel will typically hide the apostrophe while keeping the value as text.
    """
    if v is None:
        return ""
    s = str(v)
    if not s:
        return ""
    if s[0] in ("=", "+", "@"):
        return "'" + s
    if s[0] == "-" and _TIME_RE.match(s):
        return "'" + s
    return s

def excel_unsanitize_cell(v: Any) -> str:
    if v is None:
        return ""
    s = str(v)
    return s[1:] if s.startswith("'") and len(s) > 1 else s

def is_zero_amount(amount: Any, unit: Any = "") -> bool:
    """Treat numeric/time zeros as empty for diff classification."""
    a = excel_unsanitize_cell(amount).strip()
    if a == "":
        return True
    a2 = a.replace(",", "").strip()
    if ":" in a2:
        t = a2.lstrip("+")
        neg = t.startswith("-")
        t = t[1:] if neg else t
        return t in ("0:00", "00:00", "00:00:00")
    try:
        return float(a2) == 0.0
    except Exception:
        return a2 in ("0", "0.0", "0.00")


# -----------------------------
# Optibus client
# -----------------------------
class OptibusError(RuntimeError):
    pass


class OptibusClient:
    def __init__(self, base_url: str, api_key: str, api_client: str, timeout_s: int = 120) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self.sess = requests.Session()
        self.sess.headers.update({
            "Authorization": api_key,
            "X-Optibus-Api-Client": api_client,
        })

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    def get_json(self, path: str, params: Optional[Dict[str, Any]] = None, allow_413: bool = False) -> Any:
        url = self._url(path)
        resp = self.sess.get(url, params=params, timeout=self.timeout_s, allow_redirects=True)
        if resp.status_code in (413, 414) and allow_413:
            # Caller handles splitting.
            return {"__HTTP_413__": True, "__status__": resp.status_code, "__text__": resp.text, "__json__": self._maybe_json(resp)}
        if resp.status_code >= 400:
            raise OptibusError(f"HTTP {resp.status_code} for GET {url} params={params} body={resp.text[:800]}")
        return self._maybe_json(resp)

    @staticmethod
    def _maybe_json(resp: requests.Response) -> Any:
        try:
            return resp.json()
        except Exception:
            return resp.text


# -----------------------------
# Fetch: regions / depots
# -----------------------------
def fetch_regions(client: OptibusClient) -> List[dict]:
    data = client.get_json("/v1/regions")
    # The docs specify a JSON array response, but some gateways may wrap it.
    if isinstance(data, dict):
        for k in ("regions", "data", "items"):
            v = data.get(k)
            if isinstance(v, list):
                return v
        # If it's a dict but not a wrapper we recognize, return empty list to force manual depot selection.
        return []
    return data if isinstance(data, list) else []


def pick_depot_id(regions: List[dict], depot_name: Optional[str], depot_id: Optional[str]) -> Tuple[str, str]:
    """
    Returns (depot_uuid, depot_name).

    The Optibus API's `/v1/regions` endpoint returns a list of Regions, each optionally containing `units`.
    In some accounts, Regions exist but `units` is empty (the Region itself functions as the depot identifier
    for other endpoints expecting a `depotId`).

    This helper supports both shapes:
      * regions[].units[].uuid  (preferred)
      * regions[].uuid          (fallback when there are no units)

    If depot_id is given, we try to find its name; otherwise we pick by depot_name or prompt.
    """
    units: List[Tuple[str, str, str]] = []  # (region_name, unit_name, unit_uuid)

    # 1) Preferred: Region -> Units
    for r in regions or []:
        region_name = safe_str(r.get("name"))
        for u in (r.get("units") or r.get("depots") or r.get("opUnits") or []) or []:
            if isinstance(u, dict) and u.get("isArchived"):
                continue
            unit_name = safe_str(getattr(u, "get", lambda k, d=None: d)("name"))
            unit_uuid = safe_str(getattr(u, "get", lambda k, d=None: d)("uuid"))
            if unit_uuid:
                units.append((region_name, unit_name, unit_uuid))

    # 2) Fallback: Region itself acts as depotId (seen in some tenants where units are not configured)
    if len(units) == 0:
        for r in regions or []:
            if r.get("isArchived"):
                continue
            region_name = safe_str(r.get("name"))
            region_uuid = safe_str(r.get("uuid"))
            if region_uuid:
                units.append((region_name, region_name, region_uuid))

    if depot_id:

        for region_name, unit_name, unit_uuid in units:
            if unit_uuid == depot_id:
                return unit_uuid, unit_name
        # Unknown id -> still return, but name is id
        return depot_id, depot_id

    if depot_name:
        matches = [(ru, un, uu) for ru, un, uu in units if un.lower() == depot_name.lower()]
        if len(matches) == 1:
            _, un, uu = matches[0]
            return uu, un
        if len(matches) > 1:
            # Same depot name across regions - fall back to prompt
            units = matches

    if len(units) == 0:
        raise OptibusError("No depots/units found in /v1/regions response.")
    if len(units) == 1:
        _, unit_name, unit_uuid = units[0]
        return unit_uuid, unit_name

    print("\nSelect depot/unit:")
    for i, (region_name, unit_name, unit_uuid) in enumerate(units, start=1):
        print(f"  {i:>2}. {unit_name}  (region: {region_name})  id: {unit_uuid}")
    while True:
        choice = input("Enter number: ").strip()
        if not choice:
            raise SystemExit("Cancelled.")
        if choice.isdigit() and 1 <= int(choice) <= len(units):
            _, unit_name, unit_uuid = units[int(choice) - 1]
            return unit_uuid, unit_name
        print("Invalid choice. Try again.")


# -----------------------------
# Fetch: drivers (for mapping UUID <-> external ID and names)
# -----------------------------
@dataclass(frozen=True)
class DriverInfo:
    external_id: str
    uuid: str
    first_name: str
    last_name: str
    depot_name: str
    region_name: str


def fetch_all_drivers(client: OptibusClient, on_date: str) -> List[dict]:
    """
    Fetch all drivers via GET /v2/drivers (paginated).
    """
    page = 1
    all_rows: List[dict] = []
    while True:
        payload = client.get_json("/v2/drivers", params={"page": page, "onDate": on_date})
        drivers = payload.get("drivers", []) if isinstance(payload, dict) else []
        all_rows.extend(drivers)
        pagination = payload.get("pagination", {}) if isinstance(payload, dict) else {}
        cur = pagination.get("currentPage", page)
        total = pagination.get("totalPages", cur)
        if cur >= total:
            break
        page += 1
    return all_rows


def build_driver_maps(drivers_payload: List[dict]) -> Tuple[Dict[str, DriverInfo], Dict[str, DriverInfo]]:
    """
    Returns (by_uuid, by_external_id).
    """
    by_uuid: Dict[str, DriverInfo] = {}
    by_ext: Dict[str, DriverInfo] = {}

    for d in drivers_payload:
        # Docs show both old and new shapes across time; handle defensively.
        uuid = safe_str(d.get("uuid") or d.get("driverUuid") or d.get("id"))
        ext = safe_str(d.get("id") or d.get("externalId") or d.get("driverExternalId"))
        first = safe_str(d.get("firstName"))
        last = safe_str(d.get("lastName"))
        mrp = d.get("mainRegionPeriod") or {}
        depot = safe_str(mrp.get("depotName") or d.get("depotName"))
        region = safe_str(mrp.get("regionName") or d.get("regionName"))

        if not uuid or not ext:
            continue

        info = DriverInfo(external_id=ext, uuid=uuid, first_name=first, last_name=last, depot_name=depot, region_name=region)
        by_uuid[uuid] = info
        by_ext[ext] = info

    return by_uuid, by_ext


def filter_driver_external_ids_for_depot(by_ext: Dict[str, DriverInfo], depot_name: str) -> List[str]:
    """
    Try to select drivers whose main *depot* OR *region* matches depot_name.
    This is important when /v1/regions returns only regions (no units): in that case depot_name is often the region name,
    and driver.mainRegionPeriod.regionName (not depotName) is what will match.

    If we cannot match anything confidently, we return ALL driver IDs (and rely on driver chunking),
    but we also print a warning.
    """
    target = (depot_name or "").strip().lower()
    if not target:
        return sorted(set(by_ext.keys()))

    matched = [
        info.external_id
        for info in by_ext.values()
        if (info.depot_name and info.depot_name.strip().lower() == target)
        or (info.region_name and info.region_name.strip().lower() == target)
    ]
    if matched:
        return sorted(set(matched))

    # Fallback: nothing matched; return all (the API calls will be chunked anyway).
    print(f"  ⚠️ Could not match drivers by depot/region name '{depot_name}'. Using ALL drivers and relying on chunking.")
    return sorted(set(by_ext.keys()))



# -----------------------------
# Fetch: payroll + work entities (batched by dates + driver chunking)
# -----------------------------
def _chunk_list(items: List[str], chunk_size: int) -> List[List[str]]:
    if chunk_size <= 0:
        return [items]
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def fetch_payroll_chunked(
    client: OptibusClient,
    start: date,
    end: date,
    driver_ids: List[str],
    batch_days: int,
    should_use_cache: bool,
    depot_id: Optional[str] = None,
    driver_chunk_size: int = 50,
    paycodes: Optional[List[str]] = None,
    sleep_s: float = 0.0,
) -> List[dict]:
    """
    Calls GET /v2/payroll in date batches.
    Within each date batch, calls are chunked by driverIds upfront (default 50) to keep requests small and avoid 413/414.
    If the server still returns 413 for a given (drivers, dates) combination, we recursively split drivers and/or dates.

    Notes:
      - Some tenants reject providing both driverIds and depotId. This script uses driverIds mode by default.
      - If driver_ids is empty, depot_id will be used (if provided).

    See docs:
      - GET /v2/payroll (limit of 204k work-entity calculations per request; returns 413 when exceeded).
    """
    all_rows: List[dict] = []
    batches = date_batches(start, end, batch_days)

    driver_chunks: List[List[str]]
    if driver_ids:
        driver_chunks = _chunk_list(driver_ids, driver_chunk_size)
    else:
        driver_chunks = [[]]  # use depotId mode

    for (b_start, b_end) in batches:
        print(f"  Payroll batch {iso_date(b_start)} -> {iso_date(b_end)}")
        for idx, dchunk in enumerate(driver_chunks, start=1):
            if driver_ids:
                print(f"    Drivers chunk {idx}/{len(driver_chunks)} (drivers={len(dchunk)})")
            batch_rows = _fetch_payroll_range_resilient(
                client=client,
                start=b_start,
                end=b_end,
                driver_ids=dchunk,
                should_use_cache=should_use_cache,
                depot_id=depot_id,
                paycodes=paycodes,
            )
            all_rows.extend(batch_rows)
            if sleep_s:
                time.sleep(sleep_s)

    return all_rows


def _fetch_payroll_range_resilient(
    client: OptibusClient,
    start: date,
    end: date,
    driver_ids: List[str],
    should_use_cache: bool,
    depot_id: Optional[str] = None,
    paycodes: Optional[List[str]] = None,
) -> List[dict]:
    """
    Fetch payroll for one inclusive date range, handling 413 by splitting drivers and/or dates.
    """
    params: Dict[str, Any] = {
        "startDate": iso_date(start),
        "endDate": iso_date(end),
        "shouldUseCache": "true" if should_use_cache else "false",
    }
    
    # CRITICAL FIX: Optibus requires a comma-separated string for list parameters, 
    # not a Python list (which creates multiple identical query parameters).
    if paycodes:
        params["paycodes"] = ",".join(str(p) for p in paycodes)

    # IMPORTANT: Some tenants reject providing both driverIds and depotId (HTTP 400).
    # If driverIds are provided, omit depotId.
    if driver_ids:
        params["driverIds"] = ",".join(str(d) for d in driver_ids)
    else:
        if not depot_id:
            raise OptibusError("depot_id is required when driver_ids is empty for /v2/payroll.")
        params["depotId"] = depot_id

    payload = client.get_json("/v2/payroll", params=params, allow_413=True)

    # The client wraps 413 as a dict marker when allow_413=True
    if isinstance(payload, dict) and payload.get("__HTTP_413__"):
        # Prefer splitting drivers first (if we have more than 1)
        if driver_ids and len(driver_ids) > 1:
            mid = len(driver_ids) // 2
            left = driver_ids[:mid]
            right = driver_ids[mid:]
            print(f"      ⚠️ 413 too large. Splitting drivers: {len(driver_ids)} -> {len(left)} + {len(right)}")
            return (
                _fetch_payroll_range_resilient(client, start, end, left, should_use_cache, depot_id, paycodes)
                + _fetch_payroll_range_resilient(client, start, end, right, should_use_cache, depot_id, paycodes)
            )

        # If already down to <=1 driver (or using depotId mode), split the date range.
        if start >= end:
            raise OptibusError(f"Payroll request too large even for minimal range ({iso_date(start)}): {payload.get('__text__','')[:800]}")

        total_days = (end - start).days + 1
        left_days = max(1, total_days // 2)
        left_end = start + timedelta(days=left_days - 1)
        right_start = left_end + timedelta(days=1)

        print(f"      ⚠️ 413 too large. Splitting dates: {iso_date(start)}..{iso_date(end)} -> {iso_date(start)}..{iso_date(left_end)} + {iso_date(right_start)}..{iso_date(end)}")
        return (
            _fetch_payroll_range_resilient(client, start, left_end, driver_ids, should_use_cache, depot_id, paycodes)
            + _fetch_payroll_range_resilient(client, right_start, end, driver_ids, should_use_cache, depot_id, paycodes)
        )

    if isinstance(payload, list):
        return payload

    raise OptibusError(f"Unexpected payroll response type: {type(payload)}")


# -----------------------------
# Transform: payroll JSON -> CSV rows (DriverID, Date, Code, Amount, Time Unit)
# -----------------------------
def to_payroll_rows_from_payroll_api(records: List[dict]) -> List[dict]:
    rows: List[dict] = []
    for r in records:
        wd = r.get("workingDriver") or {}
        driver_id = safe_str(wd.get("driverExternalId") or wd.get("driverId") or wd.get("driverUuid"))
        occ = r.get("occurrenceDates") or {}
        dt = safe_str(occ.get("startDate") or occ.get("date") or "")
        code = safe_str(r.get("codeId") or r.get("workEntityIdReference") or r.get("entityId") or "")
        res = r.get("result") or {}

        unit = safe_str(res.get("unit") or "")
        value = res.get("value")

        amount_str, unit_str = format_amount_and_unit(value=value, unit=unit)

        if driver_id and dt and code:
            rows.append({
                COL_DRIVER: driver_id,
                COL_DATE: dt,
                COL_CODE: code,
                COL_AMOUNT: amount_str,
                COL_UNIT: unit_str,
            })
    return rows




def format_amount_and_unit(value: Any, unit: str) -> Tuple[str, str]:
    """
    Heuristic formatting to resemble Optibus CSV exports:

    - If unit is Minutes -> Amount is HH:MM and Time Unit is Hours.
    - If unit is Boolean -> Amount is 1/0 and Time Unit is Boolean.
    - Else Amount is stringified value and Time Unit is unit or "Number".
    """
    u = (unit or "").strip()
    if isinstance(value, bool) or u.lower() == "boolean":
        return ("1" if bool(value) else "0", "Boolean")

    # Value may already be a string (e.g., "07:10")
    if isinstance(value, str):
        v = value.strip()
        if u:
            # keep unit as-is if API already returns a string time representation
            return (v, u)
        return (v, "Number")

    if u.lower() == "minutes" or u.lower() == "minute":
        return (minutes_to_hhmm(0 if value is None else value), "Hours")

    if u.lower() == "hours" or u.lower() == "hour":
        # If API returns a numeric hours value, convert to HH:MM as well.
        try:
            hours = float(value)
            return (minutes_to_hhmm(hours * 60.0), "Hours")
        except Exception:
            return (safe_str(value), "Hours")

    if u.lower() == "days" or u.lower() == "day":
        return (safe_str(value), "Days")

    # Default
    return (safe_str(value), u if u else "Number")


def save_payroll_csv(rows: List[dict], out_path: Path) -> None:
    df = pd.DataFrame(rows, columns=[COL_DRIVER, COL_DATE, COL_CODE, COL_AMOUNT, COL_UNIT])

    # Excel-safe amounts (avoid parsing negative HH:MM as time)
    df[COL_AMOUNT] = df[COL_AMOUNT].apply(excel_sanitize_cell)

    # Stable sorting like your compare script
    df[COL_DRIVER] = pd.to_numeric(df[COL_DRIVER], errors="ignore")
    df = df.sort_values([COL_DRIVER, COL_DATE, COL_CODE, COL_UNIT], kind="stable")

    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  ✅ Saved payroll CSV: {out_path}  (rows={len(df):,})")



# -----------------------------
# Fetch + Transform: absences -> CSV (same shape as your current export)
# -----------------------------
def fetch_absences(client: OptibusClient, start: date, end: date) -> List[dict]:
    page = 1
    all_rows: List[dict] = []
    while True:
        payload = client.get_json("/v2/drivers/absences", params={
            "fromDate": iso_date(start),
            "toDate": iso_date(end),
            "page": page,
        })
        absences = payload.get("absences", []) if isinstance(payload, dict) else []
        all_rows.extend(absences)
        pagination = payload.get("pagination", {}) if isinstance(payload, dict) else {}
        cur = pagination.get("currentPage", page)
        total = pagination.get("totalPages", cur)
        if cur >= total:
            break
        page += 1
    return all_rows


def minutes_to_time_str(m: Any) -> str:
    if m is None or (isinstance(m, float) and pd.isna(m)):
        return ""
    try:
        mi = int(m)
    except Exception:
        return safe_str(m)
    # Keep empty when the export normally has blank; still useful if present.
    return str(mi)


def save_absences_csv(absences: List[dict], by_ext: Dict[str, DriverInfo], out_path: Path) -> None:
    out_rows: List[dict] = []
    for a in absences:
        driver_id = safe_str(a.get("driverId"))
        code = safe_str(a.get("absenceCode"))
        sdate = safe_str(a.get("startDate"))
        edate = safe_str(a.get("endDate") or a.get("startDate"))
        stime = a.get("startTime")
        etime = a.get("endTime")
        note = safe_str(a.get("note"))

        info = by_ext.get(driver_id)
        dname = (f"{info.first_name} {info.last_name}".strip() if info else "")
        depot = (info.depot_name if info else "")

        out_rows.append({
            "Driver Id": driver_id,
            "Driver Name": dname,
            "Depot Name": depot,
            "Absence code": code,
            "Start date": sdate if sdate else "",
            "Start time": minutes_to_time_str(stime),
            "End date": edate if edate else "",
            "End time": minutes_to_time_str(etime),
            "Note": note,
        })

    df = pd.DataFrame(out_rows, columns=["Driver Id","Driver Name","Depot Name","Absence code","Start date","Start time","End date","End time","Note"])
    df["Driver Id"] = pd.to_numeric(df["Driver Id"], errors="ignore")
    df = df.sort_values(["Driver Id","Start date","Absence code"], kind="stable")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  ✅ Saved absences CSV: {out_path}  (rows={len(df):,})")


# -----------------------------
# Fetch + Transform: allocation (Actual vs Planned) -> wide CSVs like your current driver_allocation export
# -----------------------------
def fetch_operational_plan_v2(
    client: OptibusClient,
    start: date,
    end: date,
    depot_uuids: Optional[Union[str, List[str]]] = None,
) -> Any:
    """
    GET /v2/operational-plan

    We use this endpoint because its response includes BOTH:
      - assignments.driverAssignments   (what is effectively the 'actual' allocation)
      - assignments.plannedAssignments  (the 'planned' allocation)

    Notes:
      - depotUuids is OPTIONAL. When omitted, the endpoint returns all depots/regions.
      - Passing an invalid depot UUID causes the entire request to fail with 404.
    """
    params = {
        "fromDate": iso_date(start),
        "toDate": iso_date(end),
        "includeStops": "false",
        "includeUnassigned": "false",
    }
    if depot_uuids:
        if isinstance(depot_uuids, (list, tuple, set)):
            params["depotUuids"] = ",".join([str(x) for x in depot_uuids if str(x)])
        else:
            params["depotUuids"] = str(depot_uuids)

    payload = client.get_json("/v2/operational-plan", params=params)
    return payload

def _ensure_list_payload(payload: Any) -> List[dict]:
    """Ensure the API payload is a list of depot plans."""
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return payload
    return []

def _pick_depot_plan_from_op_payload(payload: Any, depot_id: str) -> Optional[dict]:
    """Payload is typically a list of depot plans; pick the one matching depotUuid (or first)."""
    if isinstance(payload, dict):
        # Some versions may return a single object (defensive).
        return payload
    if isinstance(payload, list) and payload:
        for it in payload:
            if isinstance(it, dict) and safe_str(it.get("depotUuid")) == safe_str(depot_id):
                return it
        # Fallback to first depot
        return payload[0] if isinstance(payload[0], dict) else None
    return None


def _task_display(task: dict) -> str:
    """Prefer displayId; fall back to description; else type."""
    disp = safe_str(task.get("displayId") or "")
    if disp:
        return disp
    desc = safe_str(task.get("description") or "")
    if desc:
        return desc
    ttype = safe_str(task.get("type") or task.get("dutyType") or "task")
    tid = safe_str(task.get("id") or "")
    return f"{ttype}:{tid}" if tid else ttype


def build_allocation_maps_from_operational_plan(
    depot_plan: dict,
    by_uuid: Dict[str, DriverInfo],
) -> Tuple[Dict[Tuple[str, str], List[str]], Dict[Tuple[str, str], List[str]]]:
    """
    Returns:
      actual_map[(externalDriverId, date)] -> list[taskDisplay]
      planned_map[(externalDriverId, date)] -> list[taskDisplay]

    Note: /v2/operational-plan may return driver UUIDs. We normalize to external driver IDs so it matches payroll DriverID.
    """

    def _norm_driver_id(raw: Any) -> str:
        s = safe_str(raw).strip()
        if not s:
            return ""
        if s.isdigit():
            return s
        info = by_uuid.get(s)
        return info.external_id if info else s

    tasks = depot_plan.get("tasks", []) or []
    tasks_by_id: Dict[str, str] = {}
    for t in tasks:
        tid = safe_str(t.get("id"))
        if tid:
            tasks_by_id[tid] = _task_display(t)

    actual_map: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    planned_map: Dict[Tuple[str, str], List[str]] = defaultdict(list)

    for a in depot_plan.get("assignments", []) or []:
        dt = safe_str(a.get("date"))
        if not dt:
            continue

        for da in a.get("driverAssignments", []) or []:
            driver_id = _norm_driver_id(da.get("driver") or da.get("driverId") or "")
            if not driver_id:
                continue
            for task_id in (da.get("assignments", []) or []):
                tid = safe_str(task_id)
                if tid:
                    actual_map[(driver_id, dt)].append(tasks_by_id.get(tid, tid))

        for pa in a.get("plannedAssignments", []) or []:
            driver_id = _norm_driver_id(pa.get("driver") or pa.get("driverId") or "")
            if not driver_id:
                continue
            for task_id in (pa.get("assignments", []) or []):
                tid = safe_str(task_id)
                if tid:
                    planned_map[(driver_id, dt)].append(tasks_by_id.get(tid, tid))

    return actual_map, planned_map


def fetch_driver_day_labels(client: OptibusClient, start: date, end: date) -> List[dict]:
    """
    GET /v1/calendar-driver-day-labels
    """
    payload = client.get_json("/v1/calendar-driver-day-labels", params={
        "fromDate": iso_date(start),
        "toDate": iso_date(end),
    })
    return payload if isinstance(payload, list) else []


def _normalize_driver_id_for_labels(driver_id_raw: str, by_uuid: Dict[str, DriverInfo]) -> Optional[str]:
    """
    driverId in labels can be either:
      - external driver id (e.g., "871")
      - driver UUID
    We try to normalize it to external id so it matches payroll DriverID.
    """
    s = safe_str(driver_id_raw)
    if not s:
        return None
    # If it looks numeric, assume it's already the external id.
    if s.isdigit():
        return s
    info = by_uuid.get(s)
    if info:
        return info.external_id
    return s  # fallback


def _save_allocation_matrix(
    alloc_map: Dict[Tuple[str, str], List[str]],
    driver_ids: List[str],
    start: date,
    end: date,
    out_path: Path,
) -> None:
    dates: List[date] = []
    cur = start
    while cur <= end:
        dates.append(cur)
        cur += timedelta(days=1)

    cols = ["Driver ID"] + [excel_date_col(d) for d in dates]
    rows_out: List[dict] = []

    driver_set = list(driver_ids)

    def _key(x: str):
        try:
            return (0, int(x))
        except Exception:
            return (1, x)

    driver_set.sort(key=_key)

    for did in driver_set:
        did_s = safe_str(did)
        row = {"Driver ID": did_s}
        for d in dates:
            k = (did_s, d.isoformat())
            vals = alloc_map.get(k, [])
            if vals:
                seen = set()
                outv = []
                for v in vals:
                    sv = str(v).strip()
                    if sv and sv not in seen:
                        seen.add(sv)
                        outv.append(sv)
                row[excel_date_col(d)] = ", ".join(outv)
            else:
                row[excel_date_col(d)] = ""

        rows_out.append(row)

    df = pd.DataFrame(rows_out, columns=cols)
    df["Driver ID"] = pd.to_numeric(df["Driver ID"], errors="ignore")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  ✅ Saved allocation CSV: {out_path}  (drivers={len(df):,}, days={len(dates):,})")


def save_allocation_csvs(
    client: OptibusClient,
    labels_payload: List[dict],
    by_uuid: Dict[str, DriverInfo],
    driver_external_ids: List[str],
    start: date,
    end: date,
    batch_days: int,
    out_actual_path: Path,
    out_planned_path: Path,
) -> None:
    """
    Writes TWO allocation files (wide matrices):
      - Actual Allocation (from assignments.driverAssignments)
      - Planned Allocation (from assignments.plannedAssignments)

    IMPORTANT:
      - We do NOT request a single depot/region. We fetch ALL depot plans returned by the account
        for each date batch and merge them. This avoids 404 failures caused by supplying an
        invalid depotUuid and aligns with the requirement to cover all regions/depots.

      - Multiple duties per day are supported (values are joined and de-duped).
    """
    actual_map: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    planned_map: Dict[Tuple[str, str], List[str]] = defaultdict(list)

    for (b_start, b_end) in date_batches(start, end, batch_days):
        payload = fetch_operational_plan_v2(client, start=b_start, end=b_end, depot_uuids=None)
        depot_plans = _ensure_list_payload(payload)

        for depot_plan in depot_plans:
            if not isinstance(depot_plan, dict):
                continue
            a_map, p_map = build_allocation_maps_from_operational_plan(depot_plan, by_uuid)

            for k, vals in a_map.items():
                if vals:
                    actual_map[k].extend(vals)
            for k, vals in p_map.items():
                if vals:
                    planned_map[k].extend(vals)

    _save_allocation_matrix(actual_map, driver_external_ids, start, end, out_actual_path)
    _save_allocation_matrix(planned_map, driver_external_ids, start, end, out_planned_path)


def sniff_dialect(path: str, encoding: str) -> csv.Dialect:
    with open(path, "r", encoding=encoding, newline="") as f:
        sample = f.read(50_000)
    try:
        return csv.Sniffer().sniff(sample)
    except Exception:
        return csv.get_dialect("excel")


def normalize_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def try_float(s: str) -> Optional[float]:
    if s is None:
        return None
    t = str(s).strip()
    if not t:
        return None
    t = t.replace(",", "")
    try:
        return float(t)
    except Exception:
        return None


def amounts_equal(a: str, b: str, tolerance: Optional[float]) -> bool:
    a_n = normalize_str(a)
    b_n = normalize_str(b)
    if a_n == b_n:
        return True

    fa = try_float(a_n)
    fb = try_float(b_n)
    if fa is not None and fb is not None:
        if tolerance is None:
            return fa == fb
        return abs(fa - fb) <= tolerance

    return False


Key = Tuple[str, str, str, str]  # (DriverID, Date, Code, Time Unit)


def load_amount_lists(path: str, encoding: str, delimiter: str = "") -> Dict[Key, List[str]]:
    dialect = sniff_dialect(path, encoding)
    if delimiter:
        dialect.delimiter = delimiter  # type: ignore

    out: Dict[Key, List[str]] = defaultdict(list)
    with open(path, "r", encoding=encoding, newline="") as f:
        reader = csv.DictReader(f, dialect=dialect)
        for row in reader:
            key: Key = (
                normalize_str(row.get(COL_DRIVER)),
                normalize_str(row.get(COL_DATE)),
                normalize_str(row.get(COL_CODE)),
                normalize_str(row.get(COL_UNIT)),
            )
            amt_raw = excel_unsanitize_cell(row.get(COL_AMOUNT))
            out[key].append(normalize_str(amt_raw))
    return out



def compute_diffs(
    file1: str,
    file2: str,
    out_csv: str,
    tolerance: Optional[float] = None,
    encoding: str = "utf-8-sig",
    delimiter: str = "",
) -> None:
    a_map = load_amount_lists(file1, encoding=encoding, delimiter=delimiter)
    b_map = load_amount_lists(file2, encoding=encoding, delimiter=delimiter)

    all_keys = set(a_map.keys()) | set(b_map.keys())
    diffs: List[Dict[str, Any]] = []

    for key in all_keys:
        a_list = list(a_map.get(key, []))
        b_list = list(b_map.get(key, []))

        a_counts = Counter(a_list)
        b_counts = Counter(b_list)

        # Cancel identical occurrences (exact match or within tolerance)
        # For speed and stability, do exact cancellation first.
        common = a_counts & b_counts
        for val, n in common.items():
            a_counts[val] -= n
            b_counts[val] -= n
            if a_counts[val] <= 0:
                del a_counts[val]
            if b_counts[val] <= 0:
                del b_counts[val]

        a_remain = list(a_counts.elements())
        b_remain = list(b_counts.elements())

        if tolerance is not None:
            # tolerance-based cancellation for numeric values
            b_used = [False] * len(b_remain)
            new_a = []
            for av in a_remain:
                matched = False
                for j, bv in enumerate(b_remain):
                    if b_used[j]:
                        continue
                    if amounts_equal(av, bv, tolerance):
                        b_used[j] = True
                        matched = True
                        break
                if not matched:
                    new_a.append(av)
            new_b = [bv for j, bv in enumerate(b_remain) if not b_used[j]]
            a_remain, b_remain = new_a, new_b

        driver, dt, code, unit = key

        if len(a_remain) == 0 and len(b_remain) == 0:
            continue

        if len(a_remain) == len(b_remain) and len(a_remain) > 0:
            # Pair in file order-ish (stable), but treat 0 as "empty" for Change classification
            for av, bv in zip(a_remain, b_remain):
                av0 = is_zero_amount(av, unit)
                bv0 = is_zero_amount(bv, unit)

                if av0 and bv0:
                    continue

                if av0 and (not bv0):
                    diffs.append({
                        COL_DRIVER: driver,
                        COL_DATE: dt,
                        COL_CODE: code,
                        COL_UNIT: unit,
                        "Change": "addition",
                        "Pre-changes": "",
                        "Post-changes": bv,
                    })
                    continue

                if bv0 and (not av0):
                    diffs.append({
                        COL_DRIVER: driver,
                        COL_DATE: dt,
                        COL_CODE: code,
                        COL_UNIT: unit,
                        "Change": "removed",
                        "Pre-changes": av,
                        "Post-changes": "",
                    })
                    continue

                diffs.append({
                    COL_DRIVER: driver,
                    COL_DATE: dt,
                    COL_CODE: code,
                    COL_UNIT: unit,
                    "Change": "modified",
                    "Pre-changes": av,
                    "Post-changes": bv,
                })
        else:
            for av in a_remain:
                diffs.append({
                    COL_DRIVER: driver,
                    COL_DATE: dt,
                    COL_CODE: code,
                    COL_UNIT: unit,
                    "Change": "removed",
                    "Pre-changes": av,
                    "Post-changes": "",
                })
            for bv in b_remain:
                diffs.append({
                    COL_DRIVER: driver,
                    COL_DATE: dt,
                    COL_CODE: code,
                    COL_UNIT: unit,
                    "Change": "addition",
                    "Pre-changes": "",
                    "Post-changes": bv,
                })

    df = pd.DataFrame(diffs, columns=DIFF_COLS)
    # stable sort similar to your earlier approach
    df[COL_DRIVER] = pd.to_numeric(df[COL_DRIVER], errors="ignore")
    df = df.sort_values([COL_DRIVER, COL_DATE, COL_CODE, COL_UNIT, "Change"], kind="stable")
    # Excel-safe diff amounts
    if "Pre-changes" in df.columns:
        df["Pre-changes"] = df["Pre-changes"].apply(lambda v: excel_sanitize_cell(excel_unsanitize_cell(v)))
    if "Post-changes" in df.columns:
        df["Post-changes"] = df["Post-changes"].apply(lambda v: excel_sanitize_cell(excel_unsanitize_cell(v)))
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"  ✅ Saved differences CSV: {out_csv}  (rows={len(df):,})")


# -----------------------------
# Enrichment logic (ported from your comparepayrollanddaysoff.py)
# -----------------------------
def parse_date_str(s) -> pd.Timestamp:
    """Parses dates like 2026-02-01 and Excel-ish strings like ="2026-02-01"."""
    if pd.isna(s):
        return pd.NaT
    s = str(s).strip()
    if s.startswith('="') and s.endswith('"'):
        s = s[2:-1]
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    return pd.to_datetime(s, errors="coerce", dayfirst=False)


def build_needed_by_driver(amount_df: pd.DataFrame) -> dict[int, list[pd.Timestamp]]:
    needed = amount_df[[COL_DRIVER, COL_DATE]].dropna().drop_duplicates()
    needed_by_driver: dict[int, list[pd.Timestamp]] = {}
    for d, grp in needed.groupby(COL_DRIVER):
        dates = sorted(set(grp[COL_DATE].tolist()))
        needed_by_driver[int(d)] = dates
    return needed_by_driver


def enrich_differences(
    amount_path: Path,
    absences_path: Path,
    allocation_actual_path: Path,
    allocation_planned_path: Path,
    out_path: Path,
) -> None:
    """
    Enrich the differences CSV by adding:
      - Absences (from driver_absences export)
      - Actual Allocation (from operational plan actual assignments)
      - Planned Allocation (from operational plan planned assignments)

    All allocations are looked up by (DriverID, Date) using the wide matrix exports.
    """
    amount = pd.read_csv(amount_path, encoding="utf-8-sig")

    # Ensure DriverID + Date exist
    for col in [COL_DRIVER, COL_DATE]:
        if col not in amount.columns:
            raise ValueError(f"differences CSV missing required column: {col}")

    # ---------------- Absences ----------------
    abs_df = pd.read_csv(absences_path, encoding="utf-8-sig")
    if "Driver Id" not in abs_df.columns or "Start date" not in abs_df.columns or "End date" not in abs_df.columns or "Absence code" not in abs_df.columns:
        raise ValueError("absences CSV missing required columns: Driver Id, Start date, End date, Absence code")

    def _parse_excel_date(s: Any) -> Optional[pd.Timestamp]:
        if pd.isna(s):
            return None
        txtv = str(s).strip()
        txtv = txtv.replace('="', '').replace('"', '')
        try:
            return pd.to_datetime(txtv, format="%Y-%m-%d")
        except Exception:
            try:
                return pd.to_datetime(txtv, dayfirst=False)
            except Exception:
                return None

    abs_df["_start"] = abs_df["Start date"].apply(_parse_excel_date)
    abs_df["_end"] = abs_df["End date"].apply(_parse_excel_date)
    # Cast to string to avoid erasing alphanumeric driver IDs
    abs_df["_driver"] = abs_df["Driver Id"].astype(str).str.strip()

    abs_map: Dict[Tuple[str, pd.Timestamp], List[str]] = defaultdict(list)
    for _, r in abs_df.iterrows():
        d = r["_driver"]
        s = r["_start"]
        e = r["_end"]
        code = str(r["Absence code"]).strip()
        if not d or d in ("nan", "None") or s is None or e is None or not code:
            continue
        cur = s.normalize()
        endd = e.normalize()
        while cur <= endd:
            abs_map[(str(d), cur)].append(code)
            cur += pd.Timedelta(days=1)

    def absences_for(driver_id: str, dt: pd.Timestamp) -> str:
        codes = abs_map.get((driver_id, dt.normalize()), [])
        if not codes:
            return ""
        # Keep order but de-dup
        seen = set()
        out = []
        for c in codes:
            if c not in seen:
                out.append(c); seen.add(c)
        return ", ".join(out)

    # ---------------- Allocation (Actual + Planned) ----------------
    alloc_a = pd.read_csv(allocation_actual_path, encoding="utf-8-sig")
    alloc_p = pd.read_csv(allocation_planned_path, encoding="utf-8-sig")

    for df, nm in [(alloc_a, "allocation actual"), (alloc_p, "allocation planned")]:
        if "Driver ID" not in df.columns:
            raise ValueError(f"{nm} CSV missing required column: Driver ID")

    # Build maps (driverId, date) -> str
    def _alloc_map_from_matrix(df: pd.DataFrame) -> Dict[Tuple[str, pd.Timestamp], str]:
        dmap_list: Dict[Tuple[str, pd.Timestamp], List[str]] = defaultdict(list)
        date_cols = [c for c in df.columns if re.match(r"^\d{4}-\d{2}-\d{2}$", str(c))]
        df2 = df.copy()
        df2["Driver ID"] = df2["Driver ID"].astype(str).str.strip()
        
        for _, r in df2.iterrows():
            did = r["Driver ID"]
            if not did or did in ("nan", "None"):
                continue
            for c in date_cols:
                val = r.get(c)
                if pd.isna(val):
                    continue
                s = str(val).strip()
                if not s:
                    continue
                dt = _parse_excel_date(c)
                if dt is None:
                    continue
                dmap_list[(str(did), dt.normalize())].append(s)

        out: Dict[Tuple[str, pd.Timestamp], str] = {}
        for k, vals in dmap_list.items():
            seen = set()
            ordered = []
            for v in vals:
                v2 = v.strip()
                if v2 and v2 not in seen:
                    seen.add(v2)
                    ordered.append(v2)
            out[k] = ", ".join(ordered)
        return out

    actual_alloc_map = _alloc_map_from_matrix(alloc_a)
    planned_alloc_map = _alloc_map_from_matrix(alloc_p)

    def alloc_for(did: str, dt: pd.Timestamp, which: str) -> str:
        k = (did, dt.normalize())
        if which == "actual":
            return actual_alloc_map.get(k, "")
        return planned_alloc_map.get(k, "")

    # ---------------- Write columns ----------------
    amount[COL_DRIVER] = amount[COL_DRIVER].astype(str).str.strip()
    amount[COL_DATE] = pd.to_datetime(amount[COL_DATE], errors="coerce")

    amount["Absences"] = [
        absences_for(str(d), dt) if (str(d) not in ("", "nan", "None") and not pd.isna(dt)) else ""
        for d, dt in zip(amount[COL_DRIVER], amount[COL_DATE])
    ]
    amount["Actual Allocation"] = [
        alloc_for(str(d), dt, "actual") if (str(d) not in ("", "nan", "None") and not pd.isna(dt)) else ""
        for d, dt in zip(amount[COL_DRIVER], amount[COL_DATE])
    ]
    amount["Planned Allocation"] = [
        alloc_for(str(d), dt, "planned") if (str(d) not in ("", "nan", "None") and not pd.isna(dt)) else ""
        for d, dt in zip(amount[COL_DRIVER], amount[COL_DATE])
    ]

    # Clean Amount columns for readability + Excel
    for c in ["Pre-changes", "Post-changes"]:
        if c in amount.columns:
            amount[c] = amount[c].apply(lambda v: "" if is_zero_amount(v, "") else excel_sanitize_cell(excel_unsanitize_cell(v)))

    amount.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  ✅ Saved enriched differences CSV: {out_path}  (rows={len(amount):,})")

# -----------------------------
# Main pipeline
# -----------------------------
def main() -> None:
    p = argparse.ArgumentParser(description="Fetch+compare Optibus payroll entities via API (pre/post work-entity change).")
    p.add_argument("--no-gui", action="store_true", help="Disable macOS AppleScript dialogs and use CLI prompts only")
    p.add_argument("--no-save", action="store_true", help="Do not save base URL/client settings or store token in Keychain")
    p.add_argument("--forget-saved", action="store_true", help="Forget saved settings and Keychain token for this (or last) base URL, then exit")

    p.add_argument("--base-url", default=os.getenv("OPTIBUS_BASE_URL", ""), help="External API base URL, e.g. https://YOUR-ACCOUNT.api.ops.optibus.co")
    p.add_argument("--api-key", default=os.getenv("OPTIBUS_API_KEY", ""), help="API key (or set OPTIBUS_API_KEY env var)")
    p.add_argument("--api-client", default=os.getenv("OPTIBUS_API_CLIENT", ""), help='Value for header X-Optibus-Api-Client (account name), e.g. "ADO"')

    p.add_argument("--start-date", default="", help="YYYY-MM-DD")
    p.add_argument("--end-date", default="", help="YYYY-MM-DD")
    p.add_argument("--batch-days", type=int, default=None, help="Max days per payroll request batch (default: 10)")
    p.add_argument("--driver-chunk-size", type=int, default=None, help="Max drivers per /v2/payroll request before auto-splitting (default: 10)")
    p.add_argument("--paycodes", default="", help="Optional comma-separated paycodes to fetch (reduces payload). Leave blank for all.")

    p.add_argument("--depot-id", default="", help="Depot/Unit UUID. If omitted, select by --depot-name or interactively.")
    p.add_argument("--depot-name", default="", help='Depot/Unit name as shown in OPS (e.g. "Lemoa")')

    p.add_argument("--out-dir", default=".", help="Output directory")
    p.add_argument("--tolerance", type=float, default=None, help="Numeric tolerance for diffing (optional)")
    p.add_argument("--should-use-cache", action="store_true", help="Use cached payroll results if available (default: recalc)")

    args = p.parse_args()

    # Ask whether to use/ignore/wipe saved settings at the very start (before prompting).
    use_gui = _is_mac() and (not getattr(args, "no_gui", False))
    if CONFIG_FILE.exists() and (not getattr(args, "no_save", False)):
        if use_gui:
            script = f'''
            tell application "Finder"
                activate
                set choice to choose from list {{"Use saved settings", "Ignore saved settings for this run", "Wipe saved settings and tokens"}} with prompt "{_as_quote("Saved settings found. What do you want to do?")}" default items {{"Use saved settings"}}
                if choice is false then return "Use saved settings"
                return item 1 of choice
            end tell
            '''
            action = (_osascript(script) or "Use saved settings").strip()
        else:
            action_raw = prompt_text_cli("Saved settings found. Type: use / ignore / wipe", "use").strip().lower()
            action = {"use":"Use saved settings", "ignore":"Ignore saved settings for this run", "wipe":"Wipe saved settings and tokens"}.get(action_raw, "Use saved settings")

        if action.startswith("Wipe"):
            _forget_all_saved_settings()
        elif action.startswith("Ignore"):
            pass
        else:
            args = _apply_saved_settings(args)
    else:
        args = _apply_saved_settings(args)

    if getattr(args, 'forget_saved', False):
        _forget_saved_settings(args)
        print('Saved settings cleared.')
        return

    args = fill_args_interactively(args)

    # Cleanly exit with a newline if the API key fails to be collected
    if not args.api_key:
        print()
        raise SystemExit("Missing API key. Provide --api-key or set OPTIBUS_API_KEY env var.")

    # Offer to save settings only when something new needs saving.
    _maybe_persist_settings(args)

    # Show the config being used (masked API key) so you can confirm what the script picked up.
    print("\n--- Config ---")
    print(f"Base URL:    {args.base_url}")
    print(f"API Client:  {args.api_client}")
    masked = mask_api_key(args.api_key, keep=6)
    if masked:
        print(f"API Key:     {masked}")
    print(f"TLS stack:   {tls_stack_info()}")
    print("-------------\n")

    start = parse_iso_date(args.start_date)
    end = parse_iso_date(args.end_date)
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    run_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    pre_tag = f"pre_{args.start_date}_to_{args.end_date}_{run_tag}"
    post_tag = f"post_{args.start_date}_to_{args.end_date}_{run_tag}"

    client = OptibusClient(args.base_url, args.api_key, args.api_client)

    print("Fetching regions/depots...")
    regions = fetch_regions(client)
    # We do not ask the user to select a single depot/unit. We will cover ALL regions/depots in this account.
    # This avoids failures caused by mismatched depot UUIDs between endpoints and matches the desired workflow.
    try:
        region_count = len(regions)
    except Exception:
        region_count = 0
    print(f"Found {region_count} region(s). Running across all regions/depots in this account.")

    print("Fetching drivers (for ID/name mapping)...")
    drivers_payload = fetch_all_drivers(client, on_date=args.start_date)
    by_uuid, by_ext = build_driver_maps(drivers_payload)
    # Use ALL drivers in the account (no depot filter)
    driver_ids_for_depot = list(by_ext.keys())
    # Optional paycodes filter (comma-separated) to reduce payload size; leave blank for all paycodes.
    paycodes = [pc.strip() for pc in (getattr(args, "paycodes", "") or "").split(",") if pc.strip()] or None

    print(f"Drivers considered (all depots/regions): {len(driver_ids_for_depot):,}")

    # Auto-tune batch sizes to reduce 413s and keep speed reasonable
    total_days = (end - start).days + 1
    tuned_batch_days, tuned_driver_chunk = auto_tune_chunking(len(driver_ids_for_depot), total_days)
    if getattr(args, "batch_days", None) is None or int(getattr(args, "batch_days", 0) or 0) <= 0:
        args.batch_days = tuned_batch_days
    if getattr(args, "driver_chunk_size", None) is None or int(getattr(args, "driver_chunk_size", 0) or 0) <= 0:
        args.driver_chunk_size = tuned_driver_chunk
    driver_chunk_size = int(getattr(args, "driver_chunk_size", 10) or 10)
    print(f"Auto-tuned: batch_days={args.batch_days}, driver_chunk_size={driver_chunk_size}")

    # ---------------- PRE ----------------
    print("\n=== PRE: Fetch payroll entities ===")
    payroll_pre_records = fetch_payroll_chunked(
        client=client,
        start=start,
        end=end,
        driver_ids=driver_ids_for_depot,
        batch_days=args.batch_days,
        should_use_cache=args.should_use_cache,
        driver_chunk_size=driver_chunk_size,
        paycodes=paycodes,
    )
    payroll_pre_rows = to_payroll_rows_from_payroll_api(payroll_pre_records)

    pre_payroll_path = out_dir / f"{pre_tag}_payroll.csv"
    save_payroll_csv(payroll_pre_rows, pre_payroll_path)

    print("\n=== PRE: Fetch absences ===")
    abs_pre_records = fetch_absences(client, start=start, end=end)
    pre_abs_path = out_dir / f"{pre_tag}_driver_absences.csv"
    save_absences_csv(abs_pre_records, by_ext=by_ext, out_path=pre_abs_path)

    
    print("\n=== PRE: Fetch allocation (Actual + Planned) ===")
    labels_pre = fetch_driver_day_labels(client, start=start, end=end)
    pre_alloc_actual_path = out_dir / f"{pre_tag}_driver_allocation_actual.csv"
    pre_alloc_planned_path = out_dir / f"{pre_tag}_driver_allocation_planned.csv"
    save_allocation_csvs(
        client=client,
        labels_payload=labels_pre,
        by_uuid=by_uuid,
        driver_external_ids=driver_ids_for_depot,
        start=start,
        end=end,
        batch_days=args.batch_days,
        out_actual_path=pre_alloc_actual_path,
        out_planned_path=pre_alloc_planned_path,
    )
    
    # Pause
    print("\n============================================================")
    print("Now update your Work Entities configuration in the web UI.")
    print("After saving changes, press ENTER to continue.")
    print("============================================================\n")
    input()
    print("Waiting 5 seconds for Optibus backend to propagate changes...")
    time.sleep(5)

    # ---------------- POST ----------------
    print("\n=== POST: Fetch payroll entities ===")
    payroll_post_records = fetch_payroll_chunked(
        client=client,
        start=start,
        end=end,
        driver_ids=driver_ids_for_depot,
        batch_days=args.batch_days,
        should_use_cache=False,  # force recalc after config change
        driver_chunk_size=driver_chunk_size,
        paycodes=paycodes,
    )
    payroll_post_rows = to_payroll_rows_from_payroll_api(payroll_post_records)

    post_payroll_path = out_dir / f"{post_tag}_payroll.csv"
    save_payroll_csv(payroll_post_rows, post_payroll_path)
    # ---------------- DIFF + ENRICH ----------------
    print("\n=== DIFF ===")
    diff_path = out_dir / f"payroll_differences_{args.start_date}_to_{args.end_date}_{run_tag}.csv"
    compute_diffs(
        file1=str(pre_payroll_path),
        file2=str(post_payroll_path),
        out_csv=str(diff_path),
        tolerance=args.tolerance,
    )

    print("\n=== ENRICH DIFF ===")
    enriched_path = out_dir / f"payroll_differences_enriched_{args.start_date}_to_{args.end_date}_{run_tag}.csv"
    # Use PRE absences/allocation for enrichment (usually unchanged). Switch to POST if desired.
    enrich_differences(amount_path=diff_path, absences_path=pre_abs_path, allocation_actual_path=pre_alloc_actual_path, allocation_planned_path=pre_alloc_planned_path, out_path=enriched_path)

    print("\nDONE ✅")
    print(f"  PRE payroll:     {pre_payroll_path}")
    print(f"  POST payroll:    {post_payroll_path}")
    print(f"  Differences:     {diff_path}")
    print(f"  Enriched diff:   {enriched_path}")
    print(f"  PRE absences:    {pre_abs_path}")
    print(f"  PRE allocation actual:  {pre_alloc_actual_path}")
    print(f"  PRE allocation planned: {pre_alloc_planned_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelled.")
        sys.exit(1)
    except OptibusError as e:
        print(f"\nERROR: {e}")
        sys.exit(2)