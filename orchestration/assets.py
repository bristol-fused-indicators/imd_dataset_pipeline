import polars as pl
from dagster import AssetExecutionContext, asset
from project_paths import paths

from imd_pipeline import combine, fetch, process

from .configs import TimeframeConfig
from .policies import (
    annual_freshness_policy,
    monthly_freshness_policy,
    quarterly_freshness_policy,
)


@asset(freshness_policy=annual_freshness_policy)
def connectivity_raw_data(context: AssetExecutionContext, config: TimeframeConfig):
    fetch.connectivity.fetch(force_refresh=config.force_refresh)


@asset(freshness_policy=monthly_freshness_policy)
def land_registry_raw_data(context: AssetExecutionContext, config: TimeframeConfig):

    fetch.land_registry.fetch(
        snapshot_date=config.snapshot_date,
        window_months=config.window_months,
        force_refresh=config.force_refresh,
    )


@asset(freshness_policy=quarterly_freshness_policy)
def open_street_map_raw_data(context: AssetExecutionContext, config: TimeframeConfig):
    fetch.open_street_map.fetch(force_refresh=config.force_refresh)


@asset(freshness_policy=monthly_freshness_policy)
def crime_raw_data(context: AssetExecutionContext, config: TimeframeConfig):
    fetch.police_uk.fetch(
        force_refresh=config.force_refresh,
        snapshot_date=config.snapshot_date,
        window_months=config.window_months,
    )


@asset(freshness_policy=monthly_freshness_policy)
def universal_credit_raw_data(context: AssetExecutionContext, config: TimeframeConfig):
    fetch.universal_credit.fetch(
        force_refresh=config.force_refresh,
        snapshot_date=config.snapshot_date,
        window_months=config.window_months,
    )


@asset(deps=[connectivity_raw_data])
def connectivity_processed_data(
    context: AssetExecutionContext, config: TimeframeConfig
):
    process.connectivity.process()


@asset(deps=[land_registry_raw_data])
def land_registry_processed_data(
    context: AssetExecutionContext, config: TimeframeConfig
):
    process.land_registry.process(
        snapshot_date=config.snapshot_date,
        window_months=config.window_months,
        persist_processed_file=True,
    )


@asset(deps=[open_street_map_raw_data])
def open_street_map_processed_data(
    context: AssetExecutionContext, config: TimeframeConfig
):
    process.open_street_map.process()


@asset(deps=[crime_raw_data])
def crime_processed_data(context: AssetExecutionContext, config: TimeframeConfig):
    process.police_uk.process(
        snapshot_date=config.snapshot_date,
        window_months=config.window_months,
        persist_processed_file=True,
    )


@asset(deps=[universal_credit_raw_data])
def universal_credit_processed_data(
    context: AssetExecutionContext, config: TimeframeConfig
):
    process.universal_credit.process(persist_processed_file=True)


@asset(
    deps=[
        connectivity_processed_data,
        land_registry_processed_data,
        open_street_map_processed_data,
        crime_processed_data,
        universal_credit_processed_data,
    ]
)
def combined_data():
    all_frames = [
        pl.scan_parquet(paths.data_processed / "police_uk.parquet"),
        pl.scan_parquet(paths.data_processed / "universal_credit.parquet"),
        pl.scan_parquet(paths.data_processed / "open_street_map.parquet"),
        pl.scan_parquet(paths.data_processed / "land_registry.parquet"),
        pl.scan_parquet(paths.data_processed / "connectivity.parquet"),
    ]
    combine.join(*all_frames)


all_assets = [
    connectivity_raw_data,
    land_registry_raw_data,
    open_street_map_raw_data,
    crime_raw_data,
    universal_credit_raw_data,
    connectivity_processed_data,
    land_registry_processed_data,
    open_street_map_processed_data,
    crime_processed_data,
    universal_credit_processed_data,
    combined_data,
]
