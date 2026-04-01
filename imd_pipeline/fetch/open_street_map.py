import json
from pathlib import Path

import geopandas as gpd
import polars as pl
from loguru import logger
from project_paths import paths

from imd_pipeline.utils.http import cached_fetch_json, create_session
from imd_pipeline.utils.lsoas import get_district_slug


def get_area_bbox(
    boundary_path: Path,
    target_codes: list,
) -> tuple[float, float, float, float]:
    """Computes the bounding box for a city from LSOA boundaries."""

    gdf = gpd.read_file(boundary_path)
    gdf = gdf[gdf["lsoa_code"].isin(target_codes)]

    gdf_4326 = gdf.to_crs(epsg=4326)
    minx, miny, maxx, maxy = gdf_4326.total_bounds

    # Return as (min_lat, min_lon, max_lat, max_lon) to match existing convention
    return (miny, minx, maxy, maxx)


def expand_bbox(bbox: tuple[float, float, float, float], buffer_m: float = 5000) -> tuple[float, float, float, float]:
    """
    Expands a bounding box by buffer meters (approximate, using degrees conversion).

    Args:
        bbox: (min_lat, min_lon, max_lat, max_lon)
        buffer_m: buffer in meters

    Returns:
        expanded bounding box
    """

    buffer_deg = buffer_m / 111000  # Rough approximation: 1 degree latitude ~ 111 km
    min_lat, min_lon, max_lat, max_lon = bbox
    return (
        min_lat - buffer_deg,
        min_lon - buffer_deg,
        max_lat + buffer_deg,
        max_lon + buffer_deg,
    )


def fetch(
    district_name: str,
    force_refresh: bool = False,
    buffer_m: float = 5000,
    snapshot_date: str | None = None,
):
    """Fetches Bristol OSM data from the Overpass API using cached_fetch_json,
    which skips the download if the file already exists.

    Queries amenities, shops, landuse, and highways within the Bristol area.

    Args:
        force_refresh: If True, re-fetch even if the file exists.
        buffer_m: Buffer in meters to expand the bounding box around Bristol.
        snapshot_date: optional historical snapshot
            format: 'YYYY-MM-DD'

    Returns:
        Path to the saved JSON response.
    """

    logger.info(
        "fetching open street map data from overpass api",
        snapshot_date=snapshot_date,
    )

    target_codes = (
        pl.read_csv(paths.data_reference / "lsoa_lookup.csv")
        .filter(pl.col("lad_name") == district_name)
        .get_column("lsoa_code_21")
        .to_list()
    )
    boundary_path = paths.data_reference / "lsoa_boundaries.gpkg"
    min_lat, min_lon, max_lat, max_lon = expand_bbox(
        get_area_bbox(target_codes=target_codes, boundary_path=boundary_path), buffer_m
    )

    if snapshot_date:
        timestamp = f"{snapshot_date}T00:00:00Z"
        date_clause = f'[date:"{timestamp}"]'
        filename = f"overpass_response_{snapshot_date}.json"
    else:
        date_clause = ""
        filename = "overpass_response.json"

    district_slug = get_district_slug(district_name)
    output_path = paths.data_raw / district_slug / "osm" / filename
    if not output_path.exists():
        output_path.parent.mkdir(parents=True, exist_ok=True)

    overpass_url = "https://overpass-api.de/api/interpreter"

    query = f"""
[out:json]{date_clause};
(
    node["amenity"]({min_lat},{min_lon},{max_lat},{max_lon});
    way["amenity"]({min_lat},{min_lon},{max_lat},{max_lon});
    node["shop"]({min_lat},{min_lon},{max_lat},{max_lon});
    way["shop"]({min_lat},{min_lon},{max_lat},{max_lon});
    node["landuse"]({min_lat},{min_lon},{max_lat},{max_lon});
    way["landuse"]({min_lat},{min_lon},{max_lat},{max_lon});
    node["highway"]({min_lat},{min_lon},{max_lat},{max_lon});
    way["highway"]({min_lat},{min_lon},{max_lat},{max_lon});
);
out geom;
"""

    response_path = cached_fetch_json(
        url=overpass_url,
        output_path=output_path,
        session=create_session(),
        force_refresh=force_refresh,
        params={"data": query},
    )

    logger.info("open street map data saved", path=response_path)


if __name__ == "__main__":
    fetch(district_name="Bristol, City of")
