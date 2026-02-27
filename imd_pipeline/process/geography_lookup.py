import polars as pl
from loguru import logger
from project_paths import paths

def process() -> pl.LazyFrame:
    logger.info("processing geography lookup data", source=str(paths.data_raw / "geography_lookup.parquet"))
    return (
        pl.scan_parquet(paths.data_raw / "geography_lookup.parquet")
        .select(
            [pl.col("Geo Point").alias("geo_point"),
             pl.col("Geo Shape").alias("geo_shape"),
             pl.col("LSOA Code").alias("lsoa_code"),
             pl.col("Easting").alias("easting"),
             pl.col("Northing").alias("northing"),
             pl.col("Longitude").alias("longitude"),
             pl.col("Latitude").alias("latitude"),
             pl.col("Local Authority Code").alias("local_authority_code")]
             ).sink_csv(paths.data_lookup / "geography_lookup.csv")
             )





if __name__ == '__main__':
    process()