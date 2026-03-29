import polars as pl
from loguru import logger
from project_paths import paths


def process(
    district_name: str,
    persist_processed_file: bool = True,
) -> pl.LazyFrame:
    logger.info(
        "processing postcode lookup data",
        source=str(paths.data_raw / "lookup" / "postcode_lookup.parquet"),
    )

    df = (
        pl.scan_parquet(paths.data_raw / "lookup" / "postcode_lookup.parquet")
        .filter(pl.col("ladnm") == district_name)
        .select(
            [
                pl.col("pcds").alias("postcode"),
                pl.col("lsoa21cd").alias("lsoa_code"),
                pl.col("msoa21cd").alias("msoa_code"),
            ]
        )
    )

    if persist_processed_file:
        df.sink_csv(paths.data_lookup / "postcode_lookup.csv")

    return df


if __name__ == "__main__":
    district_name = "Bristol, City of"
    process(district_name)
