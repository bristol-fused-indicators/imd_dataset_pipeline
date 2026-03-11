from dagster import Definitions

from .assets import all_assets
from .jobs import all_jobs
from .schedules import all_schedules

definitions = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
)
