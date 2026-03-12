from loguru import logger
from project_paths import paths
import requests

from imd_pipeline.utils.http import cached_fetch_json, create_session

def get_area_bbox(area_name: str = "Bristol") -> tuple[float, float, float, float]:
    """
    Get the bounding box of a city from Overpass API.

    Returns:
        (min_lat, min_lon, max_lat, max_lon)
    """
    overpass_url = "https://overpass-api.de/api/interpreter"
    query = f"""
    [out:json];
    area["name"="{area_name}"]->.a;
    .a->.searchArea;
    out geom;
    """
    output_path = paths.data_raw / "osm" / f"{area_name}_bbox.json"
    cached_fetch_json(
        url=overpass_url,
        output_path=output_path,
        session=create_session(),
        force_refresh=False,
        params={"data": query},
    )

    data = requests.get(overpass_url, params={"data": query}).json()
    lats = []
    lons = []
    for elem in data.get("elements", []):
        if "geometry" in elem:
            for node in elem["geometry"]:
                lats.append(node["lat"])
                lons.append(node["lon"])
    return min(lats), min(lons), max(lats), max(lons)



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
    bristol_data_query = """
[out:json];
area["ISO3166-2"="GB-BST"]->.bristol;
(
    node["amenity"](area.bristol);
    way["amenity"](area.bristol);
    node["shop"](area.bristol);
    way["shop"](area.bristol);
    node["landuse"](area.bristol);
    way["landuse"](area.bristol);
    node["highway"](area.bristol);
    way["highway"](area.bristol);
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
