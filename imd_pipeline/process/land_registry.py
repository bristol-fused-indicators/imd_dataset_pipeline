import polars as pl
from project_paths import paths

from imd_pipeline.utils.lsoas import (
    filter_bristol,
    map_postcode_to_lsoa_code,
)
from imd_pipeline.utils.timeframes import get_window_bounds

COLUMNS = [
    "transaction_id",
    "price",
    "date_of_transfer",
    "postcode",
    "property_type",
    "old_new",
    "duration",
    "paon",
    "saon",
    "street",
    "locality",
    "town_city",
    "district",
    "county",
    "ppd_category",
    "record_status",
]


def mean_price_by_lsoa(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Pipeable func - computes mean transaction price per LSOA as lsoa_mean_price."""
    return (
        lf.select("lsoa_code", "price")
        .group_by("lsoa_code")
        .mean()
        .rename({"price": "lsoa_mean_price"})
    )

def median_price_by_lsoa(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Pipeable func - computes median transaction price per LSOA as lsoa_median_price."""
    return (
        lf.select("lsoa_code", "price")
        .group_by("lsoa_code")
        .median()
        .rename({"price": "lsoa_median_price"})
    )

def stdev_price_by_lsoa(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Pipeable func - computes stdev of transaction price per LSOA as lsoa_stdev_price."""
    return (
        lf.select("lsoa_code", "price")
        .group_by("lsoa_code")
        .agg(pl.col("price").std())
        .rename({"price": "lsoa_stdev_price"})
    )


def max_price_by_lsoa(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Pipeable func - computes max transaction price per LSOA as lsoa_max_price."""
    return (
        lf.select("lsoa_code", "price")
        .group_by("lsoa_code")
        .max()
        .rename({"price": "lsoa_max_price"})
    )

def min_price_by_lsoa(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Pipeable func - computes min transaction price per LSOA as lsoa_min_price."""
    return (
        lf.select("lsoa_code", "price")
        .group_by("lsoa_code")
        .min()
        .rename({"price": "lsoa_min_price"})
    )

def range_price_by_lsoa(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Pipeable func - computes range of transaction price per LSOA as lsoa_range_price."""
    return (
        lf.select("lsoa_code", "price")
        .group_by("lsoa_code")
        .agg(pl.col("price").max() - pl.col("price").min())
        .rename({"price": "lsoa_range_price"})
    )

def price_inequality_by_lsoa(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Pipeable func - computes price inequality per LSOA as lsoa_price_inequality,
    defined as p90 / p10."""
    return (
        lf.select("lsoa_code", "price")
        .group_by("lsoa_code")
        .agg([ 
            pl.col("price").quantile(0.9).alias("p90"),
            pl.col("price").quantile(0.1).alias("p10")])
        .with_columns((pl.col("p90") / pl.col("p10")).alias("lsoa_price_inequality"))
        .select(["lsoa_code", "lsoa_price_inequality"])
    )


def average_price_by_property_type(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Pipeable func - computes mean price per property type per LSOA, pivoting to columns T/F/S/D/O_mean_price."""
    return (
        lf.select("lsoa_code", "property_type", "price")
        .pivot(
            index="lsoa_code",
            on="property_type",
            on_columns=["T", "F", "S", "D", "O"],
            aggregate_function="mean",
        )
        .fill_null(0)
        .rename(
            {
                "T": "T_mean_price",
                "F": "F_mean_price",
                "S": "S_mean_price",
                "D": "D_mean_price",
                "O": "O_mean_price",
            }
        )
    )


def transactions_in_lsoa(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Pipeable func - counts total transactions per LSOA as total_transactions."""
    return lf.group_by("lsoa_code").len(name="total_transactions")


def transactions_per_property_type(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Pipeable func - counts transactions per property type per LSOA, pivoting to columns T/F/S/D/O_count_transactions."""
    return (
        lf.select("lsoa_code", "property_type", pl.lit("dummy"))
        .pivot(
            index="lsoa_code",
            on="property_type",
            on_columns=["T", "F", "S", "D", "O"],
            aggregate_function="len",
        )
        .fill_null(0)
        .rename(
            {
                "T": "T_count_transactions",
                "F": "F_count_transactions",
                "S": "S_count_transactions",
                "D": "D_count_transactions",
                "O": "O_count_transactions",
            }
        )
    )

def proportion_of_new_builds(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Pipeable func - computes proportion of transactions that are new builds per LSOA as new_build_proportion."""
    total_transactions = lf.group_by("lsoa_code").len().rename({"len": "total_transactions"})
    new_build_transactions = lf.filter(pl.col("old_new") == "Y").group_by("lsoa_code").len().rename({"len": "new_build_transactions"})
    return (
        total_transactions.join(
            new_build_transactions,
             on="lsoa_code",
              how="left"
              )
        .with_columns(
            (pl.col("new_build_transactions") / pl.col("total_transactions")
                )
            .fill_null(0)
            .alias("new_build_proportion")
            )
        .select("lsoa_code", "new_build_proportion")
    )

def proportion_of_freehold(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Pipeable func - computes proportion of transactions that are freehold per LSOA as freehold_proportion."""
    total_transactions = lf.group_by("lsoa_code").len().rename({"len": "total_transactions"})
    freehold_transactions = lf.filter(pl.col("duration") == "F").group_by("lsoa_code").len().rename({"len": "freehold_transactions"})
    return (
        total_transactions.join(
            freehold_transactions,
            on="lsoa_code",
            how="left"
        )
        .with_columns(
            (pl.col("freehold_transactions") / pl.col("total_transactions")
            )
            .fill_null(0)
            .alias("freehold_proportion")
        )
        .select("lsoa_code", "freehold_proportion")
    )   

def proportion_of_terraced(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Pipeable func - computes proportion of transactions that are terraced per LSOA as terraced_proportion."""
    total_transactions = lf.group_by("lsoa_code").len().rename({"len": "total_transactions"})
    terraced_transactions = lf.filter(pl.col("property_type") == "T").group_by("lsoa_code").len().rename({"len": "terraced_transactions"})
    return (
        total_transactions.join(
            terraced_transactions,
            on="lsoa_code",
            how="left"
        )
        .with_columns(
            (pl.col("terraced_transactions") / pl.col("total_transactions")
            )
            .fill_null(0)
            .alias("terraced_proportion")
        )
        .select("lsoa_code", "terraced_proportion")
    )   


def aggregate_stats(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Pipeable func - orchestrates all LSOA aggregations by joining average_price_by_lsoa, max_price_by_lsoa, average_price_by_property_type, transactions_in_lsoa, and transactions_per_property_type onto an lsoa_code spine."""
    # the "spine" will define the primary key of the data and we will left join all the stats to it
    # this ensures we don't fan out and we preserve lsoas even if they are missing some aggregations
    spine = lf.select("lsoa_code").unique()

    # each func in this list extracts different features from the same raw data
    # these are broken up for neatness, and this code can be extended by writing a new agg function
    # then adding it to this list
    all_frames = [
        mean_price_by_lsoa(lf),
        median_price_by_lsoa(lf),
        stdev_price_by_lsoa(lf),
        max_price_by_lsoa(lf),
        min_price_by_lsoa(lf),
        range_price_by_lsoa(lf),
        price_inequality_by_lsoa(lf),
        average_price_by_property_type(lf),
        transactions_in_lsoa(lf),
        transactions_per_property_type(lf),
        proportion_of_new_builds(lf),
        proportion_of_freehold(lf),
        proportion_of_terraced(lf),
    ]
    for frame in all_frames:
        spine = spine.join(frame, how="left", on="lsoa_code")

    return spine


def process(
    window_months, snapshot_date, persist_processed_file: bool = False
) -> pl.LazyFrame:
    """Loads Land Registry CSVs for the time window, maps postcodes to LSOA codes, filters to Bristol, and aggregates stats.

    Args:
        window_months: Number of months in the time window.
        snapshot_date: End date of the window in YYYY-MM-DD format.
        persist_intermediate_file: If True, sinks the result to a parquet file before returning.

    Returns:
        LazyFrame of aggregated Land Registry stats per Bristol LSOA.
    """
    window_bounds = get_window_bounds(snapshot_date, window_months)
    years = [_date.year for _date in window_bounds]

    dir = paths.data_raw / "land_registry"

    files = [
        file
        for file in dir.glob("*.csv")
        if str(file.stem).endswith(tuple(str(year) for year in years))
    ]

    dataframe = (
        pl.concat(
            [
                pl.scan_csv(
                    file,
                    has_header=False,
                    new_columns=COLUMNS,
                    schema_overrides={"date_of_transfer": pl.Datetime},
                )
                for file in files
            ]
        )
        .filter(pl.col("date_of_transfer").is_between(*window_bounds))
        .pipe(
            map_postcode_to_lsoa_code,
            postcode_col="postcode",
            lookup_path=paths.data_lookup / "postcode_lookup.csv",
        )
        .pipe(
            filter_bristol,
            code_col="lsoa_code",
            geography_path=paths.data_lookup / "geography_lookup.csv",
        )
        .pipe(aggregate_stats)
    )

    if persist_processed_file:
        dataframe.sink_parquet(paths.data_processed / "land_registry.parquet")

    return dataframe


if __name__ == "__main__":
    snapshot_date = "2025-12-01"
    window_months = 12
    process(window_months, snapshot_date, True)
