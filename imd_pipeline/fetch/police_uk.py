import json
import os
import zipfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import geopandas as gpd
import polars as pl
import requests
from bs4 import BeautifulSoup as bs
from dateutil.relativedelta import relativedelta
from geopolars.datasets import available
from loguru import logger
from project_paths import paths
from ratelimit import limits
from shapely.geometry import Polygon, shape

from imd_pipeline.utils.http import create_session
from imd_pipeline.utils.lsoas import convert_2011_to_2021, get_district_slug, get_target_codes
from imd_pipeline.utils.timeframes import get_window_bounds, months_in_window

STREETLEVEL_URL = "https://data.police.uk/api/crimes-street/all-crime"
ARCHIVE_URL = "https://data.police.uk/data/archive/"
MAX_MONTHS = 36


def extract_largest_polygon(geom) -> Polygon:
    """Returns the largest polygon from a geometry, or the input if already a Polygon.

    The Police UK API only accepts a single polygon per request, so for LSOAs
    represented as MultiPolygons, the largest part is used as the representative shape.
    If there is only one polygon for the LSOA, then it is returned directly.

    Args:
        geom: A Shapely Polygon or MultiPolygon.

    Returns:
        The largest Polygon by area.
    """

    if isinstance(geom, Polygon):
        return geom
    return max(geom.geoms, key=lambda p: p.area)


def format_coords(polygon: Polygon) -> str:
    """Formats polygon exterior coordinates as a colon separated lat,lon string for the Police UK API.

    Args:
        polygon: A Shapely Polygon.

    Returns:
        Coordinate string in the format "lat,lon:lat,lon:...".
    """
    return ":".join(f"{lat},{lon}" for lon, lat in polygon.exterior.coords)


def simplify_and_format(polygon: Polygon) -> str:
    """Simplifies a polygon and formats it for the Police UK API's poly parameter.

    LSOA boundaries can be highly detailed with loads of points,
    producing coordinate strings that exceed the API's 300 character limit.
    This function rounds and deduplicates coordinates (reduces precision),
    then iteratively increases simplification tolerance until the string fits (reduces resolution).

    Args:
        polygon: A Shapely Polygon.

    Returns:
        Coordinate string suitable for the Police UK API poly parameter.
    """
    coords = polygon.exterior.coords
    rounded = [(round(lat, 5), round(lon, 5)) for lon, lat in coords]
    deduped = list(dict.fromkeys(rounded))
    # rebuild as lon,lat for Shapely (internal), but format_coords swaps back
    poly = Polygon([(lon, lat) for lat, lon in deduped])

    formatted = format_coords(poly)
    tolerance = 0.000001
    while len(formatted) > 300:
        poly = poly.simplify(tolerance, preserve_topology=True)
        formatted = format_coords(poly)  # type: ignore
        tolerance *= 1.25  # this is a geometric progression - the tolerence will grow slowly so that we can avoid oversimplifying and loosing information. 1.25 was chosen arbitraily after a bit of trial and error, and has not been tuned

    return formatted


def load_lsoa_polygons(district_name: str, lsoa_lookup_path: Path, boundary_lookup_path: Path) -> dict[str, str]:
    """Loads LSOA geometries from the geography lookup and formats them for the Police UK API.

    For each LSOA, calls `extract_largest_polygon` to get a single useable shape,
    then calls `simplify_and_format` to produce a coordinate string within the API's 300 character limit

    Args:
        lookup_path: Path to the geography lookup CSV.

    Returns:
        Dict mapping lsoa_code to a formatted coordinate string.
    """
    target_codes = get_target_codes(district_name)

    gdf = gpd.read_file(boundary_lookup_path)
    gdf = gdf[gdf["lsoa_code"].isin(target_codes)]
    gdf_4326 = gdf.to_crs(epsg=4326)
    result = {}
    for _, row in gdf_4326.iterrows():
        largest = extract_largest_polygon(row.geometry)  # type: ignore
        result[row["lsoa_code"]] = simplify_and_format(largest)  # type: ignore
    logger.info(f"loaded {len(result)} LSOA polygons")
    return result


@limits(calls=15, period=1)
def request_crimes(url: str, session: requests.Session) -> list[dict]:
    response = session.get(url)

    if response.status_code == 404:
        return []
    if response.status_code != 200:
        logger.warning(f"{response.status_code} returned for {url}")
        response.raise_for_status()

    return response.json()


def fetch_month(
    month: str,
    lsoa_polys: dict[str, str],
    session: requests.Session,
    output_dir: Path,
    force_refresh: bool,
) -> Path:
    """Fetches crime data for a single month across all LSOA polygons and saves to parquet.

    Skips the download if the file already exists. Calls request_crimes for each LSOA.

    Args:
        month: Month string in YYYY-MM format.
        lsoa_polys: Dict mapping lsoa_code to a formatted coordinate string.
        session: requests Session to use.
        output_dir: Directory to save the parquet file.
        force_refresh: If True, refetch even if the file exists.

    Returns:
        Path to the saved parquet file.
    """

    month_path = output_dir / f"{month}.parquet"

    if month_path.exists() and not force_refresh:
        logger.debug(f"cache hit: {month_path}")
        return month_path

    logger.info(f"fetching police data for {month} ({len(lsoa_polys)} LSOAs)")
    rows = []

    for i, (lsoa_code, poly_str) in enumerate(lsoa_polys.items()):
        url = f"{STREETLEVEL_URL}?date={month}&poly={poly_str}"
        crimes = request_crimes(url, session)

        for crime in crimes:
            outcome = (crime.get("outcome_status") or {}).get("category")
            rows.append(
                {
                    "lsoa_code": lsoa_code,
                    "month": month,
                    "category": crime["category"],
                    "outcome_status": outcome,
                }
            )

        if (i + 1) % 25 == 0:
            logger.debug(f"  {month}: {i + 1}/{len(lsoa_polys)} LSOAs fetched")

    df = pl.DataFrame(
        rows,
        schema={
            "lsoa_code": pl.Utf8,
            "month": pl.Utf8,
            "category": pl.Utf8,
            "outcome_status": pl.Utf8,
        },
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    df.write_parquet(month_path)
    logger.info(f"wrote {len(df)} crimes for {month}")
    return month_path


def fetch_api(
    snapshot_date: str,
    window_months: int,
    district_name: str,
    force_refresh: bool = False,
):
    """Fetches all Police UK crime data for a year range and consolidates into a single parquet.

    A start date and number of months must be set. Validates that the range
    does not exceed the API's 36-month limit. Calls load_lsoa_polygons and fetch_month
    for each month, then writes a consolidated all_crimes.parquet.

    Args:
        snapshot_date: a date (yyyy-mm-dd) that is the reference for creating the snapshot of data
        window_months: how many months of data should be fetched, back from the snapshot date
        force_refresh: If True, refetch all months even if files exist.

    Returns:
        Path to the consolidated parquet file.

    Raises:
        ValueError: If the year range exceeds 36 months
    """

    if window_months > MAX_MONTHS:
        raise ValueError(
            f"Date range spans {window_months} months, "
            f"but the Police UK API only serves the most recent {MAX_MONTHS} months. "
            f"Reduce the range or use bulk CSV downloads from data.police.uk for historical data."
        )

    boundary_lookup_path = paths.data_reference / "lsoa_boundaries.gpkg"
    lsoa_lookup_path = paths.data_reference / "lsoa_lookkup.csv"
    lsoa_polys = load_lsoa_polygons(
        district_name=district_name,
        lsoa_lookup_path=lsoa_lookup_path,
        boundary_lookup_path=boundary_lookup_path,
    )
    months = months_in_window(snapshot_date=snapshot_date, window_months=window_months)
    logger.info(f"fetching {len(months)} months of police data for {len(lsoa_polys)} LSOAs")

    session = create_session()
    month_paths = []
    for month in months:
        path = fetch_month(
            month,
            lsoa_polys,
            session,
            paths.data_raw / get_district_slug(district_name) / "police_uk",
            force_refresh,
        )
        month_paths.append(path)


def parse_range(text: str):
    """format and process date ranges"""
    text = text.lower().replace("contains data from ", "").strip()
    start_str, end_str = text.split(" to ")

    start_dt = datetime.strptime(start_str, "%b %Y").date()
    end_dt = datetime.strptime(end_str, "%b %Y").date()

    return start_dt, end_dt


def build_dataset_index() -> dict:
    """Scrape the police uk data archive page to find which csvs are available and the assosciated date ranges.

    Args:
        None

    Returns:
        dataset_index: dictionary with end dates as key items and then start dates and csv download extensions as a value pair.
        If none are found, returns an empty dictionary.
    """

    response = requests.get(ARCHIVE_URL)
    soup = bs(response.text, "html.parser")
    dataset_index = {}

    for p in soup.find_all("p", class_="contained-range"):
        range_text = p.get_text(strip=True)
        start_dt, end_dt = parse_range(range_text)

        parent = p.find_parent()
        link = parent.find("a", href=True)

        if link:
            dataset_index[end_dt] = {"start_dt": start_dt, "url": link["href"]}

    return dict(sorted(dataset_index.items(), reverse=True))


def fetch_url_from_dates(newest_date: datetime, oldest_date: datetime, dataset_index: dict) -> list[str]:
    """
    Find the specific download links needed to fulfill the respective given data range.

    This is done by using the dictionary structure containing date ranges and download links to find the best matches of csv files
    to cover the given start and end dates.

    Args:
        newest_date: The most recent requested date
        oldest_date: The oldes requested date
        dataset_index: a dictionary with start dates as a key and end date plus url download paths as a value pair

    Returns:
        links_to_fetch: A list containing the download urls needed for fetching the relevant csv files.

    """

    links_to_fetch = []
    newest_date_found = min(dataset_index)
    inside_newest = True

    for item in dataset_index.keys():
        if dataset_index[item]["start_dt"] == oldest_date:
            links_to_fetch.append(urljoin(ARCHIVE_URL, dataset_index[item]["url"]))
            newest_date_found = item
            oldest_date = item + relativedelta(months=1)
            inside_newest = False
        if newest_date_found >= newest_date:
            break

    if inside_newest:
        links_to_fetch.append(urljoin(ARCHIVE_URL, dataset_index[max(dataset_index)]["url"]))

    return links_to_fetch


def download_zip_files(download_url: str, zip_download_path: Path):
    """Given a download url, fetch the respective zip file to teh specified download path"""
    with requests.get(download_url, stream=True) as r:
        r.raise_for_status()
        with open(zip_download_path, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)


def produce_monthly_outputs(district_name: str, zip_path: Path, force_refresh: bool):
    """
    Given the path to the downloaded zip-file, this function extracts and processes data from it and then saves the outputs as
    monthly parquet files.

    Args:
        zip_path: Path to the downloaded zip-file
    """
    district_slug = get_district_slug(district_name)
    target_codes = get_target_codes(district_name)
    # TODO: clean up this function, not particurlalry readable

    monthly_files = defaultdict(dict)

    # Get paths to reqauired files
    with zipfile.ZipFile(zip_path, "r") as z:
        for file in z.namelist():
            month = file.split("/")[0]

            if not monthly_files.get(month):
                monthly_files[month]["street"] = []
                monthly_files[month]["outcomes"] = []

            if not force_refresh:
                month_path = paths.data_raw / get_district_slug(district_name) / "police_uk" / f"{month}.parquet"
                if month_path.exists():
                    continue
            if "street" in file:
                monthly_files[month]["street"].append(file)
            elif "outcomes" in file:
                monthly_files[month]["outcomes"].append(file)

        for month, files in monthly_files.items():
            if not files["street"]:
                continue

            street_dfs = []
            for file in files["street"]:
                with z.open(file) as f:
                    street_df = pl.read_csv(f, infer_schema=False)
                    street_dfs.append(street_df)

            street_df: pl.DataFrame = pl.concat(street_dfs, how="vertical")
            street_df = street_df.drop_nulls(subset="Crime ID")

            if files["outcomes"]:
                outcome_dfs = []
                for file in files["outcomes"]:
                    with z.open(file) as f:
                        outcomes_df = pl.read_csv(f, infer_schema=False)
                        outcome_dfs.append(outcomes_df)

                outcomes_df: pl.DataFrame = pl.concat(outcome_dfs, how="vertical")
                outcomes_df = outcomes_df.drop_nulls(subset=["Crime ID"])

                merged = street_df.join(outcomes_df, on="Crime ID", how="left")
            else:
                merged = street_df

            merged = (
                (
                    merged.lazy()
                    .pipe(
                        convert_2011_to_2021,
                        col="LSOA code",
                        lookup_path=paths.data_lookup / "lsoa_2011_2021_lookup.csv",
                    )
                    .filter(pl.col("LSOA code").is_in(target_codes))
                    .with_columns(pl.lit(month).alias("month"))
                )
                .select(
                    [
                        "month",
                        pl.col("Crime type").alias("category"),
                        pl.col("LSOA code").alias("lsoa_code"),
                        pl.col("Outcome type").alias("outcome_status"),
                    ]
                )
                .collect()
                .write_parquet(paths.data_raw / district_slug / "police_uk" / f"{month}.parquet")
            )


def fetch_bulk_csv(
    newest_date: datetime, oldest_date: datetime, district_name: str, dataset_index: dict, force_refresh: bool = False
):
    """This is the main function for fetching by bulk csv download. Once the download url assosciated with the desired
    csvs are found, loops through the list of urls for data fetching and processing. Once all processes are done, the large
    zip files are removed.

    Args:
        newest_date: The most recent requested date
        oldest_date: The oldes requested date
        district_name: Name of local authority to fetch from
        dataset_index: Dictionary of start, end dates and download link extensions"""

    # TODO: Add logic to recognise if files already downloaded, add relevance to force_refresh

    zip_path = paths.data_raw / "temp.zip"

    csv_urls = fetch_url_from_dates(newest_date, oldest_date, dataset_index)

    if not csv_urls:
        logger.debug("no police uk csv files found for specified date range")

    for csv_url in csv_urls:
        logger.info(f"downloading data from {csv_url}")
        download_zip_files(csv_url, zip_path)
        produce_monthly_outputs(district_name=district_name, zip_path=zip_path, force_refresh=force_refresh)

    # Remove large zip file?
    try:
        os.remove(zip_path)
    except Exception:
        logger.error(
            "Error in removing zip file. Script functionality unaffected, but manual removal of zip file recommended"
        )


def fetch(district_name: str, snapshot_date="2025-12-01", window_months=12, force_refresh=True):
    """
    Given a date and range of months, routes the fetching of data relevant to these to use the API or/and bulk csv downloads.
    This split is due to the API being limited to just the most recent 36 months of data. To balance speed and data quality needs,
    when possible the two most recent requested months are fetched via the API, otherwise by bulk csv downloads.

    Args:
        district_name: name of local authority to fetch data for
        snapshot_date: a date (yyyy-mm-dd) that is the reference for creating the snapshot of data
        window_months: how many months of data should be fetched, back from the snapshot date
        force_refresh: If True, refetch all months even if files exist.

    """

    newest_date_to_fetch = datetime.strptime(snapshot_date, "%Y-%m-%d").date().replace(day=1)
    oldest_date_to_fetch = (newest_date_to_fetch - relativedelta(months=window_months)).replace(day=1)
    api_date_limit = (datetime.today() - relativedelta(months=36)).date()

    """Note: Me (Dan B) and Dan H discussed that we are using .today() 
    when fetching data for today, which may create a boundry effect if the source isn't updated
    before midnight of the 1st of every month"""

    months_requested = (
        pl.date_range(oldest_date_to_fetch, newest_date_to_fetch, interval="1mo", eager=True)
        .dt.strftime("%Y-%m")
        .to_list()
    )

    months_not_cached = months_requested.copy()

    # check cache hit
    if not force_refresh:
        for month in months_requested:
            month_path = paths.data_raw / get_district_slug(district_name) / "police_uk" / f"{month}.parquet"

            if month_path.exists():
                logger.debug(f"cache hit: {month}")
                months_not_cached.remove(month)

        if not months_not_cached:
            logger.debug("cache hit for all months, skipping fetch")
            return

    # trim months to fetch based on avaiability on webpage
    dataset_index = build_dataset_index()
    available_dates = pl.Series(list(dataset_index.keys()))
    req = pl.Series(months_not_cached).str.strptime(pl.Date, "%Y-%m")

    # get bounds of available data
    min_avail = available_dates.min()
    max_avail = available_dates.max()

    # filter requested to fit inside bounds
    months_to_fetch = req.filter((req >= min_avail) & (req <= max_avail))
    oldest_date_to_fetch = months_to_fetch.min()
    newest_date_to_fetch = months_to_fetch.max()

    if newest_date_to_fetch == None:
        logger.debug("all requested police uk data found in cache")
        return

    logger.debug(f"police uk data available from {min_avail} to {max_avail}")
    logger.debug(f"newest date to fetch after trimming: {newest_date_to_fetch}")
    logger.debug(f"oldest date to fetch after trimming: {oldest_date_to_fetch}")
    logger.debug(f"api date limit: {api_date_limit}")

    api_fetch_threshold = 2  # number of months to fetch via API

    # fetch with just api

    if api_fetch_threshold >= len(months_to_fetch):
        fetch_api(
            snapshot_date=str(newest_date_to_fetch),
            window_months=api_fetch_threshold,
            district_name=district_name,
            force_refresh=force_refresh,
        )

    # fetch partially with api
    elif newest_date_to_fetch >= api_date_limit:
        """Note: we are not considering using a ceiling because we have already ensured all dates are
        changed to the 1st of the month."""

        newest_bulk_date_to_fetch = months_to_fetch.sort(descending=True)[api_fetch_threshold]

        fetch_bulk_csv(
            newest_date=newest_bulk_date_to_fetch,  # type: ignore
            oldest_date=oldest_date_to_fetch,  # type: ignore
            district_name=district_name,
            dataset_index=dataset_index,
            force_refresh=force_refresh,
        )

        fetch_api(
            snapshot_date=str(newest_date_to_fetch),
            window_months=api_fetch_threshold,
            district_name=district_name,
            force_refresh=force_refresh,
        )

    # date range not covered by api, only
    else:
        fetch_bulk_csv(
            newest_date=newest_date_to_fetch,  # type: ignore
            oldest_date=oldest_date_to_fetch,  # type: ignore
            district_name=district_name,
            dataset_index=dataset_index,
            force_refresh=force_refresh,
        )


if __name__ == "__main__":
    fetch(
        snapshot_date="2026-06-03",
        window_months=30,
        district_name="Bristol, City of",
        force_refresh=False,
    )
