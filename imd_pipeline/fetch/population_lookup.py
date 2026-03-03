from io import BytesIO

import pandas as pd
from loguru import logger
from project_paths import paths

from imd_pipeline.utils.http import create_session



def fetch(force: bool = False):

    output_path = paths.data_raw / "lookup" / "population_lookup.parquet"
    if output_path.exists() and not force:
        logger.debug("cache hit", path=output_path)
        return

    url = "https://opendata.westofengland-ca.gov.uk/api/explore/v2.1/catalog/datasets/population-by-age-band-and-lsoa/exports/csv?lang=en&timezone=Europe%2FLondon&use_labels=true&delimiter=%2C"

    session = create_session()
    logger.info("downloading population_lookup data", url=url)
    r = session.get(url)

    df: pd.DataFrame = pd.read_csv(
        BytesIO(r.content),
    )

    logger.debug("population_lookup data loaded", shape=df.shape)

    df.to_parquet(output_path)
    logger.info("population_lookup data written", path=str(output_path))


if __name__ == "__main__":
    fetch()
