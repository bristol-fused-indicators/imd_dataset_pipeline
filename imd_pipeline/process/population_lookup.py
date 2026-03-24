import re

import polars as pl
import polars.selectors as cs
from loguru import logger
from project_paths import paths

UNDER_15 = [
    "aged_0_to_4",
    "aged_5_to_9",
    "aged_10_to_14",
    "F0 to 15",
    "M0 to 15",
]

WORKING_AGE = [
    "aged_15_to_19",
    "aged_20_to_24",
    "aged_25_to_29",
    "aged_30_to_34",
    "aged_35_to_39",
    "aged_40_to_44",
    "aged_45_to_49",
    "aged_50_to_54",
    "aged_55_to_59",
    "aged_60_to_64",
    "F16 to 29",
    "F30 to 44",
    "F45 to 64",
    "M16 to 29",
    "M30 to 44",
    "M45 to 64",
]

PENSION_AGE = [
    "aged_65_to_69",
    "aged_70_to_74",
    "aged_75_to_79",
    "aged_80_to_84",
    "aged_85_and_over",
    "F65 and over",
    "M65 and over",
]

def age_column_to_snake(name: str) -> str:
    """
    Convert age population columns like:
    '0 - 4 years old population'
    '85 years old and over population'
    into:
    aged_0_to_4
    aged_85_and_over
    """

    # Standard ranges
    m = re.match(r"(\d+)\s*-\s*(\d+)", name)
    if m:
        return f"aged_{m.group(1)}_to_{m.group(2)}"

    # Special case: 85+
    m = re.match(r"(\d+).*over", name)
    if m:
        return f"aged_{m.group(1)}_and_over"

    return name.lower().replace(" ", "_")



def process(snapshot_date: str = "2025-12-01", persist_processed_file: bool = False) -> pl.LazyFrame:

    snapshot_year = snapshot_date[:4]
    logger.info(
        "processing population lookup data",
        source=str(paths.data_raw / "lookup" / f"population_lookup_{snapshot_year}.parquet"),
    )


    if snapshot_year == "2025":
        df = (
            pl.scan_parquet(paths.data_raw / "lookup" / f"population_lookup_{snapshot_year}.parquet")
            .filter(pl.col("Local Authority") == "Bristol, City of")
            .select(cs.contains("population") | cs.by_name("LSOA code"))
            .rename(age_column_to_snake)
        )

    elif snapshot_year == "2023" or snapshot_year == "2022":
        df = (
            pl.scan_parquet(paths.data_raw / "lookup" / f"population_lookup_{snapshot_year}.parquet")
            .filter(pl.col("LAD 2023 Name") == "Bristol")
            .select(cs.contains("to") | cs.contains("over") | cs.by_name("LSOA 2021 Code"))
            .rename({"LSOA 2021 Code" : "lsoa_code"})
        )

    else:
        df = (
            pl.scan_parquet(paths.data_raw / "lookup" / f"population_lookup_{snapshot_year}.parquet")
            .filter(pl.col("LAD 2021 Name") == "Bristol")
            .select(cs.contains("to") | cs.contains("over") | cs.by_name("LSOA 2021 Code"))
            .rename({"LSOA 2021 Code" : "lsoa_code"})
        )

    existing_cols = df.collect_schema().names()
    
    # Sum them row-wise into a new column
    df = (
        df.with_columns(
            pl.sum_horizontal([c for c in UNDER_15 if c in existing_cols]).alias("aged_under_15"),
            pl.sum_horizontal([c for c in WORKING_AGE if c in existing_cols]).alias("working_age_population"),
            pl.sum_horizontal([c for c in PENSION_AGE if c in existing_cols]).alias("pension_age_population"))
        .select(["lsoa_code", "aged_under_15","working_age_population","pension_age_population"])
    )
        

    if persist_processed_file:
        df.sink_csv(paths.data_lookup / f"population_lookup_{snapshot_year}.csv")
    return df


if __name__ == "__main__":
    process(snapshot_date="2025-12-01", persist_processed_file=True)
    process(snapshot_date="2024-12-01", persist_processed_file=True)
    process(snapshot_date="2023-12-01", persist_processed_file=True)
    process(snapshot_date="2022-12-01", persist_processed_file=True)
    process(snapshot_date="2021-12-01", persist_processed_file=True)
    process(snapshot_date="2020-12-01", persist_processed_file=True)
    process(snapshot_date="2019-12-01", persist_processed_file=True)
