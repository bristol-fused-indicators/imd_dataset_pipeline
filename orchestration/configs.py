from datetime import date

from dagster import Config


def _default_snapshot_date():
    now = date.today()
    month_start = now.replace(day=1)
    return month_start.strftime("%Y-%m-%d")


class TimeframeConfig(Config):
    snapshot_date: str = _default_snapshot_date()
    window_months: int = 12
    force_refresh: bool = False
