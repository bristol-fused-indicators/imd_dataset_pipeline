import json
import tomllib
from itertools import product
from pathlib import Path

import polars as pl
import requests
from loguru import logger
from project_paths import paths
from ratelimit import limits
from shapely.geometry import Polygon, shape

from src.utils.http import create_session

STREETLEVEL_URL = "https://data.police.uk/api/crimes-street/all-crime"
OUTPUT_DIR = paths.data_raw / "police_uk"
MAX_MONTHS = 36


def generate_months(start_year: int, end_year: int) -> list[str]:
    return [
        f"{year}-{month:02d}"
        for year, month in product(range(start_year, end_year), range(1, 13))
    ]


def extract_largest_polygon(geom) -> Polygon:
    if isinstance(geom, Polygon):
        return geom
    return max(geom.geoms, key=lambda p: p.area)


def format_coords(polygon: Polygon) -> str:
    return ":".join(f"{lat},{lon}" for lon, lat in polygon.exterior.coords)


def simplify_and_format(polygon: Polygon) -> str:
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
        tolerance *= 1.25

    return formatted


def load_lsoa_polygons(lookup_path: Path) -> dict[str, str]:
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
    force: bool,
) -> Path:
    month_path = output_dir / f"{month}.csv"

    if month_path.exists() and not force:
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

        if (i + 1) % 50 == 0:
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
    df.write_csv(month_path)
    logger.info(f"wrote {len(df)} crimes for {month}")
    return month_path


def fetch(
    force: bool = False,
    start_year: int | None = None,
    end_year: int | None = None,
) -> Path:

    pyproject_path = Path(paths._path_to_toml)
    with open(pyproject_path, "rb") as f:
        config = tomllib.load(f)["tool"]["imd-pipeline"]

    start_year = start_year or config.get("police_start_year")
    end_year = end_year or config.get("police_end_year")

    if start_year is None or end_year is None:
        raise ValueError(
            "you must set start_year and end_year, either when calling the function or in the config toml"
        )

    total_months = (end_year - start_year) * 12
    if total_months > MAX_MONTHS:
        raise ValueError(
            f"Date range {start_year}-{end_year} spans {total_months} months, "
            f"but the Police UK API only serves the most recent {MAX_MONTHS} months. "
            f"Reduce the range or use bulk CSV downloads from data.police.uk for historical data."
        )

    lookup_path = paths.data_lookup / "geography_lookup.csv"
    lsoa_polys = load_lsoa_polygons(lookup_path)
    months = generate_months(start_year, end_year)
    logger.info(
        f"fetching {len(months)} months of police data for {len(lsoa_polys)} LSOAs"
    )

    session = create_session()
    month_paths = []
    for month in months:
        path = fetch_month(month, lsoa_polys, session, OUTPUT_DIR, force)
        month_paths.append(path)

    # consolidate all months into a single file
    all_crimes_path = OUTPUT_DIR / "all_crimes.csv"
    if (
        force
        or not all_crimes_path.exists()
        or any(
            p.stat().st_mtime > all_crimes_path.stat().st_mtime
            for p in month_paths
            if p.exists()
        )
    ):
        frames = [pl.scan_csv(p) for p in month_paths if p.exists()]
        if frames:
            combined = pl.concat(frames).collect()
            combined.write_csv(all_crimes_path)
            logger.info(
                f"consolidated {len(combined)} total crimes to {all_crimes_path}"
            )
        else:
            logger.warning("no crime data found across any month")

    return all_crimes_path


if __name__ == "__main__":
    fetch(force=True)
