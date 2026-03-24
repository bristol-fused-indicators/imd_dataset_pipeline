from io import BytesIO

import pandas as pd
from loguru import logger
from project_paths import paths

from imd_pipeline.utils.http import create_session

POPULATION_DOWNLOAD_URLS = {
"2025": "https://opendata.westofengland-ca.gov.uk/api/explore/v2.1/catalog/datasets/population-by-age-band-and-lsoa/exports/csv?lang=en&timezone=Europe%2FLondon&use_labels=true&delimiter=%2C",
"2022_2023_2024" : "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/lowersuperoutputareamidyearpopulationestimatesnationalstatistics/mid2022revisednov2025tomid2024/sapelsoabroadage20222024.xlsx",
"2019_2020_2021" : "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/lowersuperoutputareamidyearpopulationestimatesnationalstatistics/mid2011tomid2022/sapelsoabroadage20112022.xlsx"
}

def fetch(snapshot_date: str = "2025-12-01", force_refresh: bool = False):

    snapshot_year = snapshot_date[:4]

    output_path = paths.data_raw / "lookup" / f"population_lookup_{snapshot_year}.parquet"
    if output_path.exists() and not force_refresh:
        logger.debug("cache hit", path=output_path)
        return

    url = next(
    (v for k, v in POPULATION_DOWNLOAD_URLS.items() if snapshot_year in k),
    None
    )

    session = create_session()
    logger.info("downloading population_lookup data", url=url)
    r = session.get(url)

    if snapshot_year == "2025":
        df: pd.DataFrame = pd.read_csv(
            BytesIO(r.content),
        )
    else:
        df: pd.DataFrame = pd.read_excel(
            BytesIO(r.content),
            sheet_name=f"Mid-{snapshot_year} LSOA 2021",
            skiprows=3
        )


    logger.debug("population_lookup data loaded", shape=df.shape)

    df.to_parquet(output_path)
    logger.info("population_lookup data written", path=str(output_path))


if __name__ == "__main__":
    fetch(snapshot_date = "2019-12-01")
    fetch(snapshot_date = "2020-12-01")
    fetch(snapshot_date = "2021-12-01")
    fetch(snapshot_date = "2022-12-01")
    fetch(snapshot_date = "2023-12-01")
    fetch(snapshot_date = "2024-12-01")
    
