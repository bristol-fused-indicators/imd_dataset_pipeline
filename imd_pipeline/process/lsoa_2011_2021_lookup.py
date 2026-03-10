import polars as pl
from loguru import logger
from project_paths import paths

def process() -> pl.LazyFrame:
    logger.info("processing lsoa 2011 to 2021 lookup data", source=str(paths.data_raw / "lookup" / "lsoa_2011_2021_lookup.parquet"))
    return (
        pl.scan_parquet(paths.data_raw / "lookup" / "lsoa_2011_2021_lookup.parquet")
        .filter(pl.col('LAD22NM') == 'Bristol, City of')
        .select([
            pl.col('LAD22CD').alias('local_authority_code'),
            pl.col('LSOA21CD').alias('lsoa_code_21'),
            pl.col('LSOA21NM').alias('lsoa_name_21'),
            pl.col('LSOA11CD').alias('lsoa_code_11'),
            pl.col('LSOA11NM').alias('lsoa_name_11')
            ])
        ).sink_csv(paths.data_lookup / "lsoa_2011_2021_lookup.csv")



if __name__ == '__main__':
    process()