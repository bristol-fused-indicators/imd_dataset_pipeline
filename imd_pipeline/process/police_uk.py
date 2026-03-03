from datetime import date
from functools import cache, partial

import polars as pl
import polars.selectors as slt
from loguru import logger
from project_paths import paths

from imd_pipeline.utils.lsoas import filter_bristol
from imd_pipeline.utils.timeframes import months_in_window

CRIME_CATEGORIES = [
    "robbery",
    "drugs",
    "possession-of-weapons",
    "vehicle-crime",
    "other-theft",
    "anti-social-behaviour",
    "public-order",
    "bicycle-theft",
    "criminal-damage-arson",
    "other-crime",
    "theft-from-the-person",
    "violent-crime",
    "shoplifting",
    "burglary",
]

CRIME_OUTCOMES = [
    "Further action is not in the public interest",
    "Court result unavailable",
    "Local resolution",
    "Offender given a caution",
    "Unable to prosecute suspect",
    "Under investigation",
    "Awaiting court outcome",
    "Suspect charged as part of another case",
    "Further investigation is not in the public interest",
    "Formal action is not in the public interest",
    "Status update unavailable",
    "Offender given a drugs possession warning",
    "Investigation complete; no suspect identified",
    "Action to be taken by another organisation",
]


@cache
def _valid_names(window_months: int, snapshot_date: str) -> frozenset[str]:
    return frozenset(
        f"{month}.parquet" for month in months_in_window(snapshot_date, window_months)
    )


def file_in_window(filename, window_months, snapshot_date) -> bool:
    return filename in _valid_names(window_months, snapshot_date)


def aggregate_by_category(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf.select(pl.col("lsoa_code"), pl.col("category"))
        .with_columns(pl.lit(1).alias("_dummy_count_"))
        .pivot(
            "category",
            on_columns=CRIME_CATEGORIES,
            index="lsoa_code",
            values="_dummy_count_",
            aggregate_function="len",
        )
        .fill_null(0)
    )


def aggregate_by_outcome(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf.select(pl.col("lsoa_code"), pl.col("outcome_status"))
        .with_columns(pl.lit(1).alias("_dummy_count_"))
        .pivot(
            "outcome_status",
            on_columns=CRIME_OUTCOMES,
            index="lsoa_code",
            values="_dummy_count_",
            aggregate_function="len",
        )
        .fill_null(0)
    )


def aggregate_to_lsoa(lf: pl.LazyFrame) -> pl.LazyFrame:
    cats = aggregate_by_category(lf)
    outcomes = aggregate_by_outcome(lf)
    return cats.join(outcomes, on="lsoa_code")


def derive_stats(lf: pl.LazyFrame) -> pl.LazyFrame:
    return lf.with_columns(
        pl.sum_horizontal(slt.by_name(CRIME_CATEGORIES)).alias("total_crimes")
    ).with_columns(
        (pl.sum_horizontal(slt.by_name(CRIME_OUTCOMES)) / pl.col("total_crimes")).alias(
            "resolution_rate"
        )
    )


def process(
    window_months, snapshot_date, persist_intermediate_file: bool = False
) -> pl.LazyFrame:
    logger.info(
        "processing police data",
        window_months=window_months,
        snapshot_date=snapshot_date,
    )
    dir = paths.data_raw / "police_uk"
    is_valid_file = partial(
        file_in_window, window_months=window_months, snapshot_date=snapshot_date
    )

    files = [file for file in dir.glob("*.parquet") if is_valid_file(file.name)]
    logger.info("found files in window", count=len(files))

    dataframe = (
        pl.concat([pl.scan_parquet(file) for file in files])
        .pipe(
            filter_bristol,
            code_col="lsoa_code",
            geography_path=paths.data_lookup / "geography_lookup.csv",
        )
        .pipe(aggregate_to_lsoa)
        .pipe(derive_stats)
    )

    if persist_intermediate_file:
        dataframe.sink_parquet(paths.data_processed / "police_uk.parquet")
        logger.info(
            "police data written", path=str(paths.data_processed / "police_uk.parquet")
        )

    return dataframe


if __name__ == "__main__":
    window_months = 12
    snapshot_date = "2025-12-01"
    process(window_months, snapshot_date)
