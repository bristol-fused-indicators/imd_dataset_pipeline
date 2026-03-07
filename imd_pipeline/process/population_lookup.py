import polars as pl
import polars.selectors as cs
from loguru import logger
from project_paths import paths
import re

UNDER_15 = ["aged_0_to_4", "aged_5_to_9", "aged_10_to_14"]

WORKING_AGE = [
    "aged_15_to_19","aged_20_to_24","aged_25_to_29","aged_30_to_34",
    "aged_35_to_39","aged_40_to_44","aged_45_to_49","aged_50_to_54",
    "aged_55_to_59","aged_60_to_64"
]

PENSION_AGE = [
    "aged_65_to_69","aged_70_to_74","aged_75_to_79",
    "aged_80_to_84","aged_85_and_over"
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

    return name.lower().replace(' ', '_')



def process(save_processed_data: bool = False) -> pl.LazyFrame:
    logger.info("processing population lookup data", source=str(paths.data_raw / "lookup" / "population_lookup.parquet"))
    df = (
        pl.scan_parquet(paths.data_raw / "lookup" / "population_lookup.parquet")
        .filter(pl.col("Local Authority") == "Bristol, City of")
        .select(cs.contains("population") | cs.by_name("LSOA code"))
        .rename(age_column_to_snake)
        .with_columns(
            pl.sum_horizontal(UNDER_15).alias("aged_under_15"),
            pl.sum_horizontal(WORKING_AGE).alias("working_age_population"),
           pl.sum_horizontal(PENSION_AGE).alias("pension_age_population"),
        )
    )
            
    if save_processed_data:
            df.sink_csv(paths.data_lookup / "population_lookup.csv")
    return df



if __name__ == '__main__':
    process()

