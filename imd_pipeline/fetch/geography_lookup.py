from io import BytesIO

import pandas as pd
from loguru import logger
from project_paths import paths
import json
from imd_pipeline.utils.http import create_session


def fetch(force_refresh: bool = False):

    output_path = paths.data_raw / "lookup" / "geography_lookup.geojson"

    if output_path.exists() and not force_refresh:
        logger.debug("cache hit", path=output_path)
        return

    url = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BSC_V4/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json"

    session = create_session()
    r = session.get(url, params={"returnIdsOnly": "true"})
    r.raise_for_status()

    object_ids = json.loads((r.content).decode("utf-8"))["objectIds"]
    logger.info(f"number of objects to fetch: {len(object_ids)}")

    chunk_size = 200
    all_features = []

    # 2️⃣ fetch batches using only objectIds
    for i in range(0, len(object_ids), chunk_size):
        batch_ids = object_ids[i:i+chunk_size]

        if (i // chunk_size + 1) % 25 == 0:
            logger.debug(f"Fetching batch {i//chunk_size + 1}/{(len(object_ids)-1)//chunk_size + 1}")

        r_batch = session.get(url, params={"objectIds": ",".join(map(str, batch_ids))})
        r_batch.raise_for_status()
        batch_data = json.loads(r_batch.content.decode("utf-8"))["features"]
        all_features.extend(batch_data)

    # 3️⃣ save as a single valid GeoJSON
    geojson_data = {"type": "FeatureCollection", "features": all_features}

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(geojson_data, f)

    logger.info("GeoJSON saved successfully", path=str(output_path))


if __name__ == "__main__":
    fetch(force_refresh=True)
