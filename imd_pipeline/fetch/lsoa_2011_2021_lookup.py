from io import BytesIO

import pandas as pd
from loguru import logger
from project_paths import paths

from imd_pipeline.utils.http import create_session


def fetch(force_refresh: bool = False):

    output_path = paths.data_raw / "lookup" / "lsoa_2011_2021_lookup.parquet"
    if output_path.exists() and not force_refresh:
        logger.debug("cache hit", path=output_path)
        return

    url = "https://hub.arcgis.com/api/v3/datasets/cbfe64cc03d74af982c1afec639bafd1_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1"
    session = create_session()
    logger.info("downloading lsoa_2011_2021_lookup data", url=url)
    r = session.get(url)

    df: pd.DataFrame = pd.read_csv(
        BytesIO(r.content),
    )

    logger.debug("lsoa_2011_2021_lookup data loaded", shape=df.shape)

    df.to_parquet(output_path)
    logger.info("lsoa_2011_2021_lookup data written", path=str(output_path))


if __name__ == "__main__":
    fetch()
