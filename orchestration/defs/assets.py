from dagster import asset

from imd_pipeline import combine, fetch, process


@asset
def connectivity_raw_data():
    fetch.connectivity.fetch()


@asset
def land_registry_raw_data():
    fetch.land_registry.fetch()


@asset
def open_street_map_raw_data():
    fetch.open_street_map.fetch()


@asset
def crime_raw_data():
    fetch.police_uk.fetch()


@asset
def universal_credit_raw_data():
    fetch.universal_credit.fetch()


@asset
def connectivity_processed_data():
    process.connectivity.process()


@asset
def land_registry_processed_data():
    process.land_registry.process()


@asset
def open_street_map_processed_data():
    process.open_street_map.process()


@asset
def crime_processed_data():
    process.police_uk.process()


@asset
def universal_credit_processed_data():
    process.universal_credit.process()


@asset
def combined_data(): ...
