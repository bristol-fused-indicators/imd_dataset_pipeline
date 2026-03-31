from io import BytesIO

import polars as pl
from loguru import logger
from project_paths import paths

from imd_pipeline.utils.http import create_session

# ons is due to publish national lsoa populations for 2025 in "autumn". For now, reusing 2024 values
POPULATION_DOWNLOAD_URLS = {
    "2025": "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/lowersuperoutputareamidyearpopulationestimatesnationalstatistics/mid2022revisednov2025tomid2024/sapelsoabroadage20222024.xlsx",
    "2022": "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/lowersuperoutputareamidyearpopulationestimatesnationalstatistics/mid2022revisednov2025tomid2024/sapelsoabroadage20222024.xlsx",
    "2023": "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/lowersuperoutputareamidyearpopulationestimatesnationalstatistics/mid2022revisednov2025tomid2024/sapelsoabroadage20222024.xlsx",
    "2024": "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/lowersuperoutputareamidyearpopulationestimatesnationalstatistics/mid2022revisednov2025tomid2024/sapelsoabroadage20222024.xlsx",
    "2019": "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/lowersuperoutputareamidyearpopulationestimatesnationalstatistics/mid2011tomid2022/sapelsoabroadage20112022.xlsx",
    "2020": "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/lowersuperoutputareamidyearpopulationestimatesnationalstatistics/mid2011tomid2022/sapelsoabroadage20112022.xlsx",
    "2021": "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/lowersuperoutputareamidyearpopulationestimatesnationalstatistics/mid2011tomid2022/sapelsoabroadage20112022.xlsx",
}


def fetch(snapshot_date: str = "2025-12-01", force_refresh: bool = False):

    snapshot_year = snapshot_date[:4]

    output_path = paths.data_raw / "lookup" / f"population_lookup_{snapshot_year}.parquet"
    if output_path.exists() and not force_refresh:
        logger.debug("cache hit", path=output_path)
        return

    if snapshot_year in POPULATION_DOWNLOAD_URLS.keys():
        url = POPULATION_DOWNLOAD_URLS[snapshot_year]
    else:
        logger.debug("Snapshot date outside of available dates for population data, feature skipped")
        return

    session = create_session()
    logger.info("downloading population_lookup data", url=url)
    r = session.get(url)

    df: pl.DataFrame = pl.read_excel(
        BytesIO(r.content),
        sheet_name=f"Mid-{snapshot_year if snapshot_year != '2025' else '2024'} LSOA 2021",
        engine="calamine",
        read_options={"header_row": 3},
    )

    logger.debug("population_lookup data loaded", shape=df.shape)

    df.write_parquet(output_path)
    logger.info("population_lookup data written", path=str(output_path))


if __name__ == "__main__":
    fetch(snapshot_date="2019-12-01")
    fetch(snapshot_date="2020-12-01")
    fetch(snapshot_date="2021-12-01")
    fetch(snapshot_date="2022-12-01")
    fetch(snapshot_date="2023-12-01")
    fetch(snapshot_date="1990-12-01")
    fetch(snapshot_date="2024-12-01")
    fetch(snapshot_date="2025-12-01")
