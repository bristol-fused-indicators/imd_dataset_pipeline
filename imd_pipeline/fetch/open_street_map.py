from loguru import logger
from project_paths import paths
import json

from imd_pipeline.utils.http import cached_fetch_json, create_session

def get_area_bbox(force_refresh: bool = False) -> tuple[float, float, float, float]:
    """Fetches the bounding box for Bristol using the Overpass API."""

    query = """
    [out:json];
    relation["ISO3166-2"="GB-BST"];
    out bb;
    """

    output_path = paths.data_raw / "osm" / "bristol_bbox.json"

    response_path = cached_fetch_json(
        url="https://overpass-api.de/api/interpreter",
        output_path=output_path,
        session=create_session(),
        force_refresh=force_refresh,
        params={"data": query},
    )

    with open(response_path) as f:
        data = json.load(f)

    bounds = data["elements"][0]["bounds"]

    return (
        bounds["minlat"],
        bounds["minlon"],
        bounds["maxlat"],
        bounds["maxlon"],
    )

def expand_bbox(bbox: tuple[float, float, float, float], buffer_m: float = 5000) -> tuple[float, float, float, float]:
    """
    Expands a bounding box by buffer meters (approximate, using degrees conversion).

    Args:
        bbox: (min_lat, min_lon, max_lat, max_lon)
        buffer_m: buffer in meters

    Returns:
        expanded bounding box
    """
    
    buffer_deg = buffer_m / 111000 # Rough approximation: 1 degree latitude ~ 111 km
    min_lat, min_lon, max_lat, max_lon = bbox
    return (
        min_lat - buffer_deg,
        min_lon - buffer_deg,
        max_lat + buffer_deg,
        max_lon + buffer_deg,
    )

def fetch(force_refresh: bool = False, buffer_m: float = 5000, snapshot_date: str | None = None):
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

    min_lat, min_lon, max_lat, max_lon = expand_bbox(get_area_bbox(), buffer_m)

    if snapshot_date:
        timestamp = f'{snapshot_date}T00:00:00Z'
        date_clause = f'[date:"{timestamp}"]'
        filename = f"overpass_response_{snapshot_date}.json"
    else:
        date_clause = ""
        filename = "overpass_response.json"

    output_path = paths.data_raw / "osm" / filename

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
    fetch()
