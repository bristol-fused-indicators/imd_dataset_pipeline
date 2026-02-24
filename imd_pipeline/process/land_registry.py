from datetime import date

import polars as pl
from dateutil.relativedelta import relativedelta
from icecream import ic
from project_paths import paths

from imd_pipeline.utils.lsoas import (
    filter_bristol,
    map_postcode_to_lsoa_code,
)

COLUMNS = [
    "transaction_id",
    "price",
    "date_of_transfer",
    "postcode",
    "property_type",
    "old_new",
    "duration",
    "paon",
    "saon",
    "street",
    "locality",
    "town_city",
    "district",
    "county",
    "ppd_category",
    "record_status",
]


def get_window_bounds(snapshot_date: str, window_months: int) -> tuple[date, date]:
    end = date.fromisoformat(snapshot_date)
    start = end - relativedelta(months=window_months)
    return start, end


def aggregate_stats(lf: pl.LazyFrame) -> pl.LazyFrame:
    spine = lf.select("lsoa_code")

    all_frames = []
    for frame in all_frames:
        spine = spine.join(frame, how="left", on="lsoa_code")

    return spine


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

    dataframe = (
        pl.concat(
            [
                pl.scan_csv(
                    file,
                    has_header=False,
                    new_columns=COLUMNS,
                    schema_overrides={"date_of_transfer": pl.Datetime},
                )
                for file in files
            ]
        )
        .filter(pl.col("date_of_transfer").is_between(*window_bounds))
        .pipe(
            map_postcode_to_lsoa_code,
            postcode_col="postcode",
            lookup_path=paths.data_lookup / "postcode_lookup.csv",
        )
        .pipe(
            filter_bristol,
            code_col="lsoa_code",
            geography_path=paths.data_lookup / "geography_lookup.csv",
        )
    )

    ic(dataframe.collect().head(), dataframe.collect().height)

    if persist_intermediate_file:
        dataframe.sink_parquet(paths.data_processed / "land_registry.parquet")

    return dataframe


if __name__ == "__main__":
    snapshot_date = "2025-12-01"
    window_months = 12
    process(window_months, snapshot_date, True)
