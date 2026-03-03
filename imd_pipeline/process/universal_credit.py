import polars as pl
from loguru import logger
from project_paths import paths

from imd_pipeline.utils.lsoas import filter_bristol, map_lsoa_names_to_codes


def aggregate_to_lsoa(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Pipeable func - groups by LSOA, computing total and mean monthly claims for each UC conditionality group."""
    return lf.group_by(pl.col("lsoa_code")).agg(
        [
            pl.col("value").sum().alias("total_claims"),
            pl.col("value").sum().over("month").mean().alias("mean_monthly_claims"),
            pl.col("value")
            .filter(pl.col("condition_group").eq("universal_credit_no_work_required"))
            .sum()
            .alias("total_nwr_claims"),
            pl.col("value")
            .filter(pl.col("condition_group").eq("universal_credit_planning_for_work"))
            .sum()
            .alias("total_planfw_claims"),
            pl.col("value")
            .filter(pl.col("condition_group").eq("universal_credit_preparing_for_work"))
            .sum()
            .alias("total_prepfw_claims"),
            pl.col("value")
            .filter(pl.col("condition_group").eq("universal_credit_searching_for_work"))
            .sum()
            .alias("total_sfw_claims"),
            pl.col("value")
            .filter(pl.col("condition_group").eq("universal_credit_no_work_required"))
            .sum()
            .over("month")
            .mean()
            .alias("mean_monthly_nwr_claims"),
            pl.col("value")
            .filter(pl.col("condition_group").eq("universal_credit_planning_for_work"))
            .sum()
            .over("month")
            .mean()
            .alias("mean_monthly_planfw_claims"),
            pl.col("value")
            .filter(pl.col("condition_group").eq("universal_credit_preparing_for_work"))
            .sum()
            .over("month")
            .mean()
            .alias("mean_monthly_prepfw_claims"),
            pl.col("value")
            .filter(pl.col("condition_group").eq("universal_credit_searching_for_work"))
            .sum()
            .over("month")
            .mean()
            .alias("mean_monthly_sfw_claims"),
        ]
    )


def calculate_ratios(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Pipeable func - adds percentage columns for each UC conditionality group relative to total claims."""
    return lf.with_columns(
        (pl.col("total_nwr_claims") / pl.col("total_claims")).alias("%_claims_nwr"),
        (pl.col("total_planfw_claims") / pl.col("total_claims")).alias(
            "%_claims_planfw"
        ),
        (pl.col("total_prepfw_claims") / pl.col("total_claims")).alias(
            "%_claims_prepfw"
        ),
        (pl.col("total_sfw_claims") / pl.col("total_claims")).alias("%_claims_sfw"),
    )


def process(persist_intermediate_file: bool = False) -> pl.LazyFrame:
    """Loads raw UC data, maps LSOA names to codes, filters to Bristol, aggregates by LSOA, and calculates ratios.

    Args:
        persist_intermediate_file: If True, sinks the result to a parquet file before returning.

    Returns:
        LazyFrame of aggregated UC stats per Bristol LSOA.
    """

    logger.info(
        "processing universal credit data",
        source=str(paths.data_raw / "universal_credit.parquet"),
    )
    df = (
        pl.scan_parquet(paths.data_raw / "universal_credit.parquet")
        .pipe(
            map_lsoa_names_to_codes,
            name_col="lsoa_name",
            lookup_path=paths.data_lookup / "lsoa_2011_2021_lookup.csv",
        )
        .pipe(
            filter_bristol,
            code_col="lsoa_code",
            geography_path=paths.data_lookup / "geography_lookup.csv",
        )
        .pipe(aggregate_to_lsoa)
        .pipe(calculate_ratios)
    )

    if persist_intermediate_file:
        df.sink_parquet(paths.data_processed / "universal_credit.parquet")
        logger.info(
            "universal credit data written",
            path=str(paths.data_processed / "universal_credit.parquet"),
        )

    return df


if __name__ == "__main__":
    process()
