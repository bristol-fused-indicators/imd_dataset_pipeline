from io import BytesIO

import pandas as pd
from loguru import logger
from project_paths import paths

from imd_pipeline.utils.http import create_session


def fetch(force_refresh: bool = False):

    output_path = paths.data_raw / "lookup" / "geography_lookup.geojson"

    if output_path.exists() and not force_refresh:
        logger.debug("cache hit", path=output_path)
        return

    url = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BGC_V5/FeatureServer/0/query?where=1%3D1&outFields=LSOA21CD,LSOA21NM,LSOA21NMW,LAT,LONG,Shape__Area,Shape__Length&outSR=4326&f=json"

    session = create_session()
    logger.info("downloading geojson", url=url)

    r = session.get(url)
    r.raise_for_status()

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "wb") as f:
        f.write(r.content)

    logger.info("geojson saved", path=str(output_path))


if __name__ == "__main__":
    fetch(force_refresh=True)
