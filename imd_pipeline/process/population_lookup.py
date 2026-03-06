import polars as pl
from loguru import logger
from project_paths import paths

def process(save_processed_data: bool = False) -> pl.LazyFrame:
    logger.info("processing population lookup data", source=str(paths.data_raw / "lookup" / "population_lookup.parquet"))
    df = pl.scan_parquet(
        paths.data_raw / "lookup" / "population_lookup.parquet").filter(
            pl.col('Local Authority') == 'Bristol, City of').select([
            pl.col('LSOA code').alias('lsoa_code'),
            pl.col('LSOA population').alias('lsoa_population'),
            pl.col('0 - 4 years old population').alias('aged_0_to_4'),
            pl.col('5 - 9 years old population').alias('aged_5_to_9'),
            pl.col('10 - 14 years old population').alias('aged_10_to_14'),
            pl.col('15 - 19 years old population').alias('aged_15_to_19'),
            pl.col('20 - 24 years old population').alias('aged_20_to_24'),
            pl.col('25 - 29 years old population').alias('aged_25_to_29'),
            pl.col('30 - 34 years old population').alias('aged_30_to_34'),
            pl.col('35 - 39 years old population').alias('aged_35_to_39'),
            pl.col('40 - 44 years old population').alias('aged_40_to_44'),
            pl.col('45 - 49 years old population').alias('aged_45_to_49'),
            pl.col('50 - 54 years old population').alias('aged_50_to_54'),
            pl.col('55 - 59 years old population').alias('aged_55_to_59'),
            pl.col('60 - 64 years old population').alias('aged_60_to_64'),
            pl.col('65 - 69 years old population').alias('aged_65_to_69'),
            pl.col('70 - 74 years old population').alias('aged_70_to_74'),
            pl.col('75 - 79 years old population').alias('aged_75_to_79'),
            pl.col('80 - 84 years old population').alias('aged_80_to_84'),
            pl.col('85 years old and over population').alias('aged_85_and_over')]).with_columns((
            pl.col('aged_15_to_19') +
            pl.col('aged_20_to_24') +
            pl.col('aged_25_to_29') +
            pl.col('aged_30_to_34') +
            pl.col('aged_35_to_39') +
            pl.col('aged_40_to_44') +
            pl.col('aged_45_to_49') +
            pl.col('aged_50_to_54') +
            pl.col('aged_55_to_59') +
            pl.col('aged_60_to_64')).alias('working_age_population')).with_columns((pl.col('aged_65_to_69') +pl.col('aged_70_to_74') +pl.col('aged_75_to_79') +pl.col('aged_80_to_84') +
            pl.col('aged_85_and_over')).alias('pension_age_population')).with_columns((
            pl.col('aged_0_to_4') +
            pl.col('aged_5_to_9') +
            pl.col('aged_10_to_14')).alias('aged_under_15'))
            
    if save_processed_data:
            df.sink_csv(paths.data_lookup / "population_lookup.csv")
    return df



if __name__ == '__main__':
    process()

