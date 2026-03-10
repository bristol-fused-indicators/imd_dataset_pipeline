from datetime import date

from dagster import RunConfig, ScheduleDefinition

from .configs import TimeframeConfig
from .jobs import refresh_lr_job

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

all_schedules = [
    land_registry_schedule,
]
