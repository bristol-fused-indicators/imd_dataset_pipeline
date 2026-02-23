from datetime import date
from functools import partial

import polars as pl
from icecream import ic
from loguru import logger
from project_paths import paths

from imd_pipeline.utils.lsoas import filter_bristol, map_lsoa_names_to_codes


def _month(month_decriment: int, startdate: date):
    new_month = startdate.month - 1 - month_decriment
    year = startdate.year + new_month // 12
    month = new_month % 12 + 1
    return date(year, month, 1)


def file_in_window(filename, window_months, snapshot_date) -> bool:
    start = date.fromisoformat(snapshot_date)
    month = partial(_month, startdate=start)
    valid_names = [
        month(decriment).strftime("%Y-%m") + ".parquet"
        for decriment in range(window_months)
    ]
    ic(valid_names)
    return filename in valid_names


def process(window_months, snapshot_date) -> pl.LazyFrame:
    dir = paths.data_raw / "police_uk"
    is_valid_file = partial(
        file_in_window, window_months=window_months, snapshot_date=snapshot_date
    )
    all_files = [file.name for file in dir.glob("*.parquet")]
    ic(all_files)
    files = [file for file in dir.glob("*.parquet") if is_valid_file(file.name)]

    ic(files)


if __name__ == "__main__":
    window_months = 12
    snapshot_date = "2025-12-01"
    process(window_months, snapshot_date)
