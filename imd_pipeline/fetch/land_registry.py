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
    """Downloads a yearly Land Registry price paid CSV using cached_fetch,
    which will skip the download if the file already exists.

    Args:
        session: requests Session to use.
        year: Year of data to fetch.
        force_refresh: If True, re-download even if the file exists.This arg is passed along to `cached_fetch`

    Returns:
        Path to the downloaded CSV.
    """

    url = YEARLY_URL_TEMPLATE.format(year=year)
    output_path = RAW_DIR / f"land_registry_price_paid_{year}.csv"
    logger.info("fetching yearly land registry data", year=year)
    return cached_fetch(
        url=url, output_path=output_path, session=session, force_refresh=force_refresh
    )


def _required_years(snapshot_date: str, window_months: int) -> list[int]:
    """Returns the calendar years spanned by the time window.

    Args:
        snapshot_date: End date of the window in YYYY-MM-DD format.
        window_months: Number of months in the window.

    Returns:
        List of years covered by the window.
    """

    start, end = get_window_bounds(
        snapshot_date=snapshot_date, window_months=window_months
    )
    return list(range(start.year, end.year + 1))


def fetch(snapshot_date: str, window_months: int, force_refresh: bool = False):
    """Fetches all yearly Land Registry CSVs required for the time window.

    Uses _required_years to determine which years to fetch, then calls
    fetch_yearly for each, passing through force_refresh.

    Args:
        snapshot_date: End date of the window in YYYY-MM-DD format.
        window_months: Number of months in the window.
        force_refresh: If True, re-download files even if they exist.
    """
    session = create_session()

    required_years = _required_years(snapshot_date, window_months)
    for year in required_years:
        fetch_yearly(session=session, year=year, force_refresh=force_refresh)


if __name__ == "__main__":
    snapshot_date = "2025-12-01"
    window_months = 12
    fetch(snapshot_date, window_months)
