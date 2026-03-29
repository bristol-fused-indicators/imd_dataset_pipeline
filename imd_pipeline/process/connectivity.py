import polars as pl
from loguru import logger
from polars import selectors as sls
from project_paths import paths

from imd_pipeline.utils.lsoas import filter_lsoas

INPUT_DIR = paths.data_raw / "connectivity"


def process(district_name: str, persist_processed_file: bool = False) -> pl.LazyFrame:
    """Loads raw connectivity data, standardises the LSOA code column, and filters to Bristol.

    Returns:
        LazyFrame of Bristol connectivity metrics with lsoa_code as the key column.
    """

    logger.info(
        "processing connectivity data",
        source=str(INPUT_DIR / "connectivity.parquet"),
    )

    df = (
        pl.scan_parquet(INPUT_DIR / "connectivity.parquet")
        .select(pl.col("LSOA21CD").alias("lsoa_code"), sls.exclude(pl.col("LSOA21CD")))
        .pipe(
            filter_lsoas,
            "lsoa_code",
            district_name,
            paths.data_reference / "lsoa_lookup.csv",
        )
    )

    if persist_processed_file:
        df.sink_parquet(INPUT_DIR / "connectivity.parquet")

    return df
