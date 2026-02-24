from io import BytesIO

import pandas as pd
from loguru import logger
from project_paths import paths

from imd_pipeline.utils.http import create_session


def fetch(force: bool = False):

    output_path = paths.data_raw / "connectivity.parquet"
    if output_path.exists() and not force:
        logger.debug("cache hit", path=output_path)
        return

    url = "https://assets.publishing.service.gov.uk/media/68c966fc07d9e92bc5517b80/connectivity_metrics_2025.ods"
    session = create_session()
    logger.info("downloading connectivity data", url=url)
    r = session.get(url)

    df: pd.DataFrame = pd.read_excel(
        BytesIO(r.content), engine="odf", sheet_name="LSOA", header=2
    )

    logger.debug("connectivity data loaded", shape=df.shape)

    df.to_parquet(output_path)
    logger.info("connectivity data written", path=str(output_path))


if __name__ == "__main__":
    fetch()
