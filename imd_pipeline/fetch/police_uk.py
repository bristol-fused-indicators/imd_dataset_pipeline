import json
from pathlib import Path

import os
import zipfile
import polars as pl
import requests
from loguru import logger
from project_paths import paths
from ratelimit import limits
from shapely.geometry import Polygon, shape
from bs4 import BeautifulSoup as bs
from urllib.parse import urljoin
from collections import defaultdict

from datetime import datetime
from dateutil.relativedelta import relativedelta

from imd_pipeline.utils.http import create_session
from imd_pipeline.utils.timeframes import months_in_window

STREETLEVEL_URL = "https://data.police.uk/api/crimes-street/all-crime"
ARCHIVE_URL = "https://data.police.uk/data/archive/"  
OUTPUT_DIR = paths.data_raw / "police_uk"
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


def load_lsoa_polygons(lookup_path: Path) -> dict[str, str]:
    """Loads LSOA geometries from the geography lookup and formats them for the Police UK API.

    For each LSOA, calls `extract_largest_polygon` to get a single useable shape,
    then calls `simplify_and_format` to produce a coordinate string within the API's 300 character limit

    Args:
        lookup_path: Path to the geography lookup CSV.

    Returns:
        Dict mapping lsoa_code to a formatted coordinate string.
    """

    df = pl.read_csv(lookup_path, columns=["lsoa_code", "geo_shape"])
    result = {}
    for row in df.iter_rows(named=True):
        geojson = json.loads(row["geo_shape"])
        geom = shape(geojson)
        largest = extract_largest_polygon(geom)
        result[row["lsoa_code"]] = simplify_and_format(largest)
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

    lookup_path = paths.data_lookup / "geography_lookup.csv"
    lsoa_polys = load_lsoa_polygons(lookup_path)
    months = months_in_window(snapshot_date=snapshot_date, window_months=window_months)
    logger.info(
        f"fetching {len(months)} months of police data for {len(lsoa_polys)} LSOAs"
    )

    session = create_session()
    month_paths = []
    for month in months:
        path = fetch_month(month, lsoa_polys, session, OUTPUT_DIR, force_refresh)
        month_paths.append(path)


def parse_range(text: str):
    '''format and process date ranges'''
    text = text.lower().replace("contains data from ", "").strip()
    start_str, end_str = text.split(" to ")
    
    start_dt = datetime.strptime(start_str, "%b %Y").date()
    end_dt = datetime.strptime(end_str, "%b %Y").date()
    
    return start_dt, end_dt


def build_dataset_index(url: str):
    response = requests.get(url)
    soup = bs(response.text, "html.parser")
    dataset_index = {}
    '''get dictionary item containing start/end dates of datasets, plus url download links'''
    
    for p in soup.find_all("p", class_="contained-range"):
        range_text = p.get_text(strip=True)
        start_dt, end_dt = parse_range(range_text)
        
        parent = p.find_parent()
        link = parent.find("a", href=True)
        
        if link:
            dataset_index[end_dt] = {
                "start_dt": start_dt,
                "url": link["href"]
            }
    
    return dataset_index

def fetch_download_urls(
    newest_date: str,
    oldest_date: int,
    force_refresh: bool = False,
):
    '''Get specific links for downloads'''
    dataset_index = build_dataset_index(ARCHIVE_URL)
    links_to_fetch = []
    newest_date_found = min(dataset_index)

    # TODO re-do this function without nested loop

    while newest_date_found < newest_date:
        for item in dataset_index.keys():
            if dataset_index[item]['start_dt'] == oldest_date:
                links_to_fetch.append(urljoin(ARCHIVE_URL, dataset_index[item]['url']))
                newest_date_found = item
                oldest_date = item + relativedelta(months=1)
        if newest_date_found == min(dataset_index):
            break 
    return links_to_fetch

def extract_avon_street_outcomes(url: str, download_path: str):

    zip_path = download_path / 'temp.zip'
    
    # Download ZIP
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
    
    with zipfile.ZipFile(zip_path, "r") as z:
        
        monthly_files = defaultdict(dict)
        
        # Identify relevant files
        for file in z.namelist():
            if "avon-and-somerset" in file and file.endswith(".csv"):
                month = file.split("/")[0]
                
                if "street" in file:
                    monthly_files[month]["street"] = file
                elif "outcomes" in file:
                    monthly_files[month]["outcomes"] = file
        
        # Process each month
        for month, files in monthly_files.items():
            
            if "street" not in files:
                continue  # can't do anything without base
            
            # Load street data (base)
            with z.open(files["street"]) as f:
                street_df = pl.read_csv(f)
            
            # Clean
            street_df = street_df.drop_nulls(subset="Crime ID")

            
            # Merge outcomes if available
            if "outcomes" in files:
                with z.open(files["outcomes"]) as f:
                    outcomes_df = pl.read_csv(f)
                
                outcomes_df = outcomes_df.drop_nulls(subset=["Crime ID"])

                
                merged = street_df.join(
                    outcomes_df,
                    on="Crime ID",
                    how="left"
                )
            else:
                merged = street_df
            
            merged = merged.with_columns(
                pl.lit(month).alias("month"))

            # save semi-;rocessed data
            output_path = download_path / f"{month}.parquet"            
            merged.select(['month','Crime type', 'LSOA code', 'Outcome type']).write_parquet(output_path)
    
    # Remove large zip file?
    os.remove(zip_path)

def fetch_bulk_csv(
    newest_date: str,
    oldest_date: int,
    force_refresh: bool = False,
):
    csv_links = fetch_download_urls(newest_date,oldest_date,force_refresh)
    for url in csv_links:
        extract_avon_street_outcomes(url=url, download_path=OUTPUT_DIR)


def fetch(snapshot_date="2025-12-01", window_months=12, force_refresh=True):

    newest_date_to_fetch = datetime.strptime(snapshot_date, "%Y-%m-%d").date().replace(day=1)
    oldest_date_to_fetch = (newest_date_to_fetch - relativedelta(months=window_months)).replace(day=1)
    api_date_limit = (datetime.today() - relativedelta(months=36)).date()

    print('newest date to fetch:',newest_date_to_fetch)      
    print('oldest date to fetch:',oldest_date_to_fetch)
    print('api date limit:',api_date_limit)


    if oldest_date_to_fetch >= api_date_limit:

        # fetch soley with api
        #fetch_api(snapshot_date, window_months, force_refresh)
        pass
    
    elif newest_date_to_fetch >= api_date_limit:

        # fetch as a mixture of api and bulk download
        delta = relativedelta(newest_date_to_fetch, api_date_limit)
        api_window = delta.years * 12 + delta.months
        bulk_window = window_months - api_window

        print('api_window:', api_window)

        #fetch_api(snapshot_date=str(newest_date_to_fetch), window_months=api_window, force_refresh)
        #fetch_bulk_csv(snapshot_date=api_date_limit, window_months=bulk_window, force_refresh)

    else:
        # fetch soley from bulk download
        print(fetch_bulk_csv(newest_date_to_fetch, oldest_date_to_fetch, force_refresh))
        pass



if __name__ == "__main__":
    fetch(snapshot_date="2022-12-01", window_months=70, force_refresh=True)