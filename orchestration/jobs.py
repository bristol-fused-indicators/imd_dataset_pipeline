from dagster import AssetSelection, define_asset_job

from .assets import (
    connectivity_raw_data,
    crime_processed_data,
    crime_raw_data,
    land_registry_processed_data,
    land_registry_raw_data,
    open_street_map_raw_data,
    universal_credit_raw_data,
)

refresh_crime_job = define_asset_job(
    name="refresh_crime_data",
    selection=AssetSelection.assets(crime_raw_data, crime_processed_data),
)

refresh_lr_job = define_asset_job(
    name="refresh_land_registry_data",
    selection=AssetSelection.assets(
        land_registry_raw_data, land_registry_processed_data
    ),
)

refresh_uc_job = define_asset_job(
    name="refresh_universal_credit_data",
    selection=AssetSelection.assets(universal_credit_raw_data),
)

refresh_connectivity_job = define_asset_job(
    name="refresh_connectivity_data",
    selection=AssetSelection.assets(connectivity_raw_data),
)

refresh_osm_job = define_asset_job(
    name="refresh_open_street_map_data",
    selection=AssetSelection.assets(open_street_map_raw_data),
)

all_jobs = [
    refresh_crime_job,
    refresh_lr_job,
    refresh_uc_job,
    refresh_connectivity_job,
    refresh_osm_job,
]
