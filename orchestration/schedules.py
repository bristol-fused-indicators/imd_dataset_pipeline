from datetime import date

from dagster import RunConfig, ScheduleDefinition

from .configs import TimeframeConfig
from .jobs import (
    refresh_connectivity_job,
    refresh_crime_job,
    refresh_lr_job,
    refresh_osm_job,
    refresh_uc_job,
)

crons = {
    "monthly": "0 0 1 * *",
    "quarterly": "0 0 1 */3 *",
    "annual": "0 0 1 1 *",
}


def _run_config(asset_name: str):
    def _inner(context):
        snapshot = date.today().replace(day=1).strftime("%Y-%m-%d")
        return RunConfig(ops={asset_name: TimeframeConfig(snapshot_date=snapshot)})

    return _inner


land_registry_schedule = ScheduleDefinition(
    name="land_registry_schedule",
    cron_schedule=crons.get("monthly"),
    job=refresh_lr_job,
    run_config_fn=_run_config("land_registry_raw_data"),
)

crime_schedule = ScheduleDefinition(
    name="crime_schedule",
    cron_schedule=crons.get("monthly"),
    job=refresh_crime_job,
    run_config_fn=_run_config(asset_name="crime_raw_data"),
)

universal_credit_schedule = ScheduleDefinition(
    name="universal_credit_schedule",
    cron_schedule=crons.get("monthly"),
    job=refresh_uc_job,
    run_config_fn=_run_config(asset_name="universal_credit_raw_data"),
)

connectivity_schedule = ScheduleDefinition(
    name="connectivity_schedule",
    cron_schedule=crons.get("annual"),
    job=refresh_connectivity_job,
    run_config_fn=_run_config(asset_name="connectivity_raw_data"),
)

open_street_map_schedule = ScheduleDefinition(
    name="open_street_map_schedule",
    cron_schedule=crons.get("quarterly"),
    job=refresh_osm_job,
    run_config_fn=_run_config(asset_name="open_street_map_raw_data"),
)

all_schedules = [
    land_registry_schedule,
    crime_schedule,
    universal_credit_schedule,
    connectivity_schedule,
    open_street_map_schedule,
]
