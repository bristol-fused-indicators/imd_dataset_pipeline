import polars as pl
from loguru import logger
from project_paths import paths

def process() -> pl.LazyFrame:
    logger.info("processing postcode lookup data", source=str(paths.data_raw / "lookup" / "postcode_lookup.parquet"))
    return (
        pl.scan_parquet(paths.data_raw / "lookup" / "postcode_lookup.parquet")
        .filter(pl.col('ladnm') == 'Bristol, City of')
        .select([pl.col('pcds').alias('postcode'),
        pl.col('lsoa21cd').alias('lsoa_code'),
        pl.col('msoa21cd').alias('msoa_code')])
        ).sink_csv(paths.data_lookup / "postcode_lookup.csv")



if __name__ == '__main__':
    process()