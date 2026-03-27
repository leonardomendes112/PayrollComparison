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
