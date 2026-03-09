import polars as pl
from loguru import logger
from polars import selectors as sls
from project_paths import paths

from imd_pipeline.utils.lsoas import filter_bristol


def process(persist_processed_file: bool = False) -> pl.LazyFrame:
    """Loads raw connectivity data, standardises the LSOA code column, and filters to Bristol.

    Returns:
        LazyFrame of Bristol connectivity metrics with lsoa_code as the key column.
    """

    logger.info(
        "processing connectivity data",
        source=str(paths.data_raw / "connectivity.parquet"),
    )

    df = (
        pl.scan_parquet(paths.data_raw / "connectivity.parquet")
        .select(pl.col("LSOA21CD").alias("lsoa_code"), sls.exclude(pl.col("LSOA21CD")))
        .pipe(filter_bristol, "lsoa_code", paths.data_lookup / "geography_lookup.csv")
    )

    if persist_processed_file:
        df.sink_parquet(paths.data_processed / "connectivity.parquet")

    return df
