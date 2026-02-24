import polars as pl
from polars import selectors as sls
from project_paths import paths

from imd_pipeline.utils.lsoas import filter_bristol


def process() -> pl.LazyFrame:
    return (
        pl.scan_parquet(paths.data_raw / "connectivity.parquet")
        .select(pl.col("LSOA21CD").alias("lsoa_code"), sls.exclude(pl.col("LSOA21CD")))
        .pipe(filter_bristol, "lsoa_code", paths.data_lookup / "geography_lookup.csv")
    )
