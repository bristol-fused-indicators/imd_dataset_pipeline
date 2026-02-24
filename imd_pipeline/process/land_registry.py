from datetime import date

import polars as pl
from dateutil.relativedelta import relativedelta
from icecream import ic
from project_paths import paths


def get_window_bounds(snapshot_date: str, window_months: int) -> tuple[date, date]:
    end = date.fromisoformat(snapshot_date)
    start = end - relativedelta(months=window_months)
    return start, end


def process(
    window_months, snapshot_date, persist_intermediate_file: bool = False
) -> pl.LazyFrame:
    window_bounds = get_window_bounds(snapshot_date, window_months)
    ic(window_bounds)
    years = [_date.year for _date in window_bounds]
    ic(years)

    dir = paths.data_raw / "land_registry"

    files = [
        file
        for file in dir.glob("*.csv")
        if str(file.stem).endswith(tuple(str(year) for year in years))
    ]

    ic(files)

    dataframe = pl.concat([pl.scan_parquet(file) for file in files]).filter(
        ...  # between dates
    )

    return dataframe


if __name__ == "__main__":
    snapshot_date = "2025-12-01"
    window_months = 12
    process(window_months, snapshot_date, True)
