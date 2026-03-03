from pathlib import Path

from loguru import logger
from project_paths import paths
from requests import Session

from imd_pipeline.utils.http import cached_fetch, create_session
from imd_pipeline.utils.timeframes import get_window_bounds

BASE_URL = (
    "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/"
)
MONTHLY_UPDATE_URL = f"{BASE_URL}pp-monthly-update-new-version.csv"
YEARLY_URL_TEMPLATE = f"{BASE_URL}pp-{{year}}.csv"

RAW_DIR = paths.data_raw / "land_registry"


def fetch_yearly(session: Session, year: int, force_refresh: bool = False) -> Path:
    url = YEARLY_URL_TEMPLATE.format(year=year)
    output_path = RAW_DIR / f"land_registry_price_paid_{year}.csv"
    logger.info("fetching yearly land registry data", year=year)
    return cached_fetch(
        url=url, output_path=output_path, session=session, force=force_refresh
    )


def _required_years(snapshot_date: str, window_months: int) -> list[int]:
    start, end = get_window_bounds(
        snapshot_date=snapshot_date, window_months=window_months
    )
    return list(range(start.year, end.year + 1))


def fetch(snapshot_date: str, window_months: int, force_refresh: bool = False):
    session = create_session()

    required_years = _required_years(snapshot_date, window_months)
    for year in required_years:
        fetch_yearly(session=session, year=year)


if __name__ == "__main__":
    snapshot_date = "2025-12-01"
    window_months = 12
    fetch(snapshot_date, window_months)
