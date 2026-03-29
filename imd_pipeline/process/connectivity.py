import polars as pl
from loguru import logger
from polars import selectors as sls
from project_paths import paths

from imd_pipeline.utils.lsoas import filter_lsoas, get_district_slug


def process(district_name: str, persist_processed_file: bool = False) -> pl.LazyFrame:
    """Loads raw connectivity data, standardises the LSOA code column, and filters to Bristol.

    Returns:
        LazyFrame of Bristol connectivity metrics with lsoa_code as the key column.
    """
    district_slug = get_district_slug(district_name)
    input_dir = paths.data_raw / "connectivity"

    logger.info(
        "processing connectivity data",
        source=str(input_dir / "connectivity.parquet"),
    )

    df = (
        pl.scan_parquet(input_dir / "connectivity.parquet")
        .select(pl.col("LSOA21CD").alias("lsoa_code"), sls.exclude(pl.col("LSOA21CD")))
        .pipe(
            filter_lsoas,
            "lsoa_code",
            district_name,
            paths.data_reference / "lsoa_lookup.csv",
        )
    )

    if persist_processed_file:
        df.sink_parquet(paths.data_processed / district_slug / "connectivity.parquet")

    return df
