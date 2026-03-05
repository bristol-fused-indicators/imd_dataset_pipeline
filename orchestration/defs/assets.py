from dagster import AssetExecutionContext, asset

from imd_pipeline import combine, fetch, process

from .policies import (
    annual_freshness_policy,
    monthly_freshness_policy,
    quarterly_freshness_policy,
)


@asset(freshness_policy=annual_freshness_policy)
def connectivity_raw_data():
    fetch.connectivity.fetch()


@asset(freshness_policy=monthly_freshness_policy)
def land_registry_raw_data():
    fetch.land_registry.fetch()


@asset(freshness_policy=quarterly_freshness_policy)
def open_street_map_raw_data():
    fetch.open_street_map.fetch()


@asset(freshness_policy=monthly_freshness_policy)
def crime_raw_data():
    fetch.police_uk.fetch()


@asset(freshness_policy=monthly_freshness_policy)
def universal_credit_raw_data():
    fetch.universal_credit.fetch()


@asset(deps=[connectivity_raw_data])
def connectivity_processed_data():
    process.connectivity.process()


@asset(deps=[land_registry_raw_data])
def land_registry_processed_data():
    process.land_registry.process()


@asset(deps=[open_street_map_raw_data])
def open_street_map_processed_data():
    process.open_street_map.process()


@asset(deps=[crime_raw_data])
def crime_processed_data():
    process.police_uk.process()


@asset(deps=[universal_credit_raw_data])
def universal_credit_processed_data():
    process.universal_credit.process()


@asset(
    deps=[
        connectivity_processed_data,
        land_registry_processed_data,
        open_street_map_processed_data,
        crime_processed_data,
        universal_credit_processed_data,
    ]
)
def combined_data(): ...


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
