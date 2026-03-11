from loguru import logger
from project_paths import paths
import json

from imd_pipeline.utils.http import cached_fetch_json, create_session


def get_area_bbox(force_refresh: bool = False):
    """"Fetches the bounding box for Bristol using the Overpass API.

    Returns:
        A tuple of (minlat, minlon, maxlat, maxlon) with a buffer applied.
    """

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

def expand_bbox(bbox, buffer_km=5):
    """Expands a bounding box by a specified buffer in kilometers.

    Args:
        bbox: A tuple of (minlat, minlon, maxlat, maxlon).
        buffer_km: The buffer distance in kilometers to expand the bbox.  Default is 5 km.

    Returns:
        A new tuple of (minlat, minlon, maxlat, maxlon) with the buffer applied."""

    min_lat, min_lon, max_lat, max_lon = bbox

    deg_buffer = buffer_km / 111 # converts km to degrees (approx)

    return (
        min_lat - deg_buffer,
        min_lon - deg_buffer,
        max_lat + deg_buffer,
        max_lon + deg_buffer,
    )

def fetch(force_refresh: bool = False):
    """Fetches Bristol OSM data from the Overpass API using cached_fetch_json,
    which skips the download if the file already exists.

    Queries amenities, shops, landuse, and highways within the Bristol area.

    Args:
        force_refresh: If True, re-fetch even if the file exists.

    Returns:
        Path to the saved JSON response.
    """

    logger.info(
        "fetching open street map data from overpass api", force_refresh=force_refresh
    )

    output_path = paths.data_raw / "osm" / "overpass_response.json"

    overpass_url = "https://overpass-api.de/api/interpreter"
    min_lat, min_lon, max_lat, max_lon = expand_bbox(get_area_bbox(), 5)
    bristol_data_query = f"""
[out:json];
(
    node["amenity"]({min_lat},{min_lon},{max_lat},{max_lon});
    way["amenity"]({min_lat},{min_lon},{max_lat},{max_lon});
    node["shop"]({min_lat},{min_lon},{max_lat},{max_lon});
    way["shop"]({min_lat},{min_lon},{max_lat},{max_lon});
);
out geom;
"""

    response_path = cached_fetch_json(
        url=overpass_url,
        output_path=output_path,
        session=create_session(),
        force_refresh=force_refresh,
        params={"data": bristol_data_query},
    )

    logger.info("open street map data saved", path=output_path)


if __name__ == "__main__":
    fetch()
