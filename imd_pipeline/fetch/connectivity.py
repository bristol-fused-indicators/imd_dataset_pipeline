from io import BytesIO

import polars as pl
from loguru import logger
from project_paths import paths

from imd_pipeline.utils.http import create_session

OUTPUT_DIR = paths.data_raw / "connectivity"

def fetch(force_refresh: bool = False):
    """Downloads connectivity metrics data and saves it to parquet, skipping if already downloaded.

    Args:
        force_refresh: If True, re-download even if the file exists.

    Warning:
        This can take up to 30 minutes due to conversion of a large ODS file.
    """

    output_path = OUTPUT_DIR / "connectivity.parquet"
    if output_path.exists() and not force_refresh:
        logger.debug("cache hit", path=output_path)
        return

    url = "https://assets.publishing.service.gov.uk/media/68c966fc07d9e92bc5517b80/connectivity_metrics_2025.ods"
    session = create_session()
    logger.info("downloading connectivity data", url=url)
    r = session.get(url)

    df = pl.read_excel(
        BytesIO(r.content),
        sheet_name="LSOA",
        engine="calamine",
        read_options={"header_row": 2},
    )

    logger.debug("connectivity data loaded", shape=df.shape)

    df.write_parquet(output_path)
    logger.info("connectivity data written", path=str(output_path))


if __name__ == "__main__":
    fetch(force_refresh=True)
