import zipfile
from io import BytesIO

import pandas as pd
from loguru import logger
from project_paths import paths

from imd_pipeline.utils.http import create_session


def fetch(force_refresh: bool = False):

    output_path = paths.data_raw / "lookup" / "postcode_lookup.parquet"
    if output_path.exists() and not force_refresh:
        logger.debug("cache hit", path=output_path)
        return

    url = "https://www.arcgis.com/sharing/rest/content/items/c4f84c38814d4b82aa4760ade686c3cc/data"
    session = create_session()
    logger.info("downloading postcode_lookup data", url=url)
    r = session.get(url)

    with zipfile.ZipFile(BytesIO(r.content)) as z:
        csv_name = z.namelist()[0]  # only one file in the zip

        with z.open(csv_name) as f:
            df: pd.DataFrame = pd.read_csv(f)

    logger.debug("postcode_lookup data loaded", shape=df.shape)

    df.to_parquet(output_path)
    logger.info("postcode_lookup data written", path=str(output_path))


if __name__ == "__main__":
    fetch()
