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


def average_price_by_lsoa(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf.select("lsoa_code", "price")
        .group_by("lsoa_code")
        .mean()
        .rename({"price": "lsoa_average_price"})
    )


def max_price_by_lsoa(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf.select("lsoa_code", "price")
        .group_by("lsoa_code")
        .max()
        .rename({"price": "lsoa_max_price"})
    )


def average_price_by_property_type(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf.select("lsoa_code", "property_type", "price")
        .pivot(
            index="lsoa_code",
            on="property_type",
            on_columns=["T", "F", "S", "D", "O"],
            aggregate_function="mean",
        )
        .fill_null(0)
        .rename(
            {
                "T": "T_mean_price",
                "F": "F_mean_price",
                "S": "S_mean_price",
                "D": "D_mean_price",
                "O": "O_mean_price",
            }
        )
    )


def transactions_in_lsoa(lf: pl.LazyFrame) -> pl.LazyFrame:
    return lf.group_by("lsoa_code").len(name="total_transactions")


def transactions_per_property_type(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf.select("lsoa_code", "property_type", pl.lit("dummy"))
        .pivot(
            index="lsoa_code",
            on="property_type",
            on_columns=["T", "F", "S", "D", "O"],
            aggregate_function="len",
        )
        .fill_null(0)
        .rename(
            {
                "T": "T_count_transactions",
                "F": "F_count_transactions",
                "S": "S_count_transactions",
                "D": "D_count_transactions",
                "O": "O_count_transactions",
            }
        )
    )


def aggregate_stats(lf: pl.LazyFrame) -> pl.LazyFrame:
    spine = lf.select("lsoa_code").unique()

    all_frames = [
        average_price_by_lsoa(lf),
        max_price_by_lsoa(lf),
        average_price_by_property_type(lf),
        transactions_in_lsoa(lf),
        transactions_per_property_type(lf),
    ]
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
        .pipe(aggregate_stats)
    )

    ic(dataframe.collect().head(), dataframe.collect().height)

    if persist_intermediate_file:
        dataframe.sink_parquet(paths.data_processed / "land_registry.parquet")

    return dataframe


if __name__ == "__main__":
    snapshot_date = "2025-12-01"
    window_months = 12
    process(window_months, snapshot_date, True)
