from pathlib import Path

from loguru import logger
from project_paths import paths

from imd_pipeline.utils.http import cached_fetch_json, create_session


def fetch(force_refresh: bool = False) -> Path:

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
        force=force_refresh,
        params={"data": bristol_data_query},
    )

    logger.info("open street map data saved", path=output_path)

    return response_path


if __name__ == "__main__":
    fetch()
