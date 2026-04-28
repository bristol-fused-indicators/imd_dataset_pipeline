"""
this is for running the pipeline to get all the quarters between 2019 and 2025
im writing one script that loops through, and it will contain code very similar to main.py

It will produce the configs, feed them into main (rather than main reading them as in main.py),
and importantly copy and rename the output file at the end of each run.

The two anchors need all cities run, all the quarters in between need only bristol.
"""

import shutil
import tomllib
from datetime import date, datetime

from dateutil.relativedelta import relativedelta
from loguru import logger
from project_paths import paths

from imd_pipeline import combine, fetch, process
from imd_pipeline.config import Config
from imd_pipeline.utils.lsoas import get_district_slug

ANCHOR_DATES = [
    "2019-09-01",
    "2025-10-01",
]

ALL_CITIES = [
    "Bristol, City of",
    "Exeter",
    "Plymouth",
    "Newcastle upon Tyne",
    "Bournemouth, Christchurch and Poole",
    "Sheffield",
]

PREDICTION_CITY = [
    "Bristol, City of",
]


def generate_quarterly_dates() -> list[str]:
    start = date(2020, 1, 1)
    end = date(2025, 10, 1)
    dates = []
    current = start
    while current < end:
        dates.append(current.isoformat())
        current += relativedelta(months=3)
    return dates


def write_run_config(snapshot_date: str, lad_names: list[str]) -> None:
    lines = [
        "[snapshot]",
        f"date = {snapshot_date}",
        "",
        "[scope]",
        "lad_names = [",
    ]
    for name in lad_names:
        lines.append(f'    "{name}",')
    lines.append("]")

    paths.run_config.write_text("\n".join(lines))


def rename_outputs(snapshot_date: str, lad_names: list[str]) -> None:
    for city in lad_names:
        slug = get_district_slug(city)
        output_dir = paths.data_output / slug
        src = output_dir / "combined_indicators.parquet"
        dest = output_dir / f"combined_indicators_{snapshot_date}.parquet"
        if src.exists():
            shutil.copy2(src, dest)
            logger.info(f"copied {src} to {dest}")
        else:
            logger.warning(f"expected output not found: {src}")


def parse_config() -> Config:
    run_config = tomllib.loads(paths.run_config.read_text())
    pipeline_config = tomllib.loads(paths.pipeline_config.read_text())

    return Config(
        pipeline_config["temporal"]["window_months"],
        datetime.strftime(run_config["snapshot"]["date"], "%Y-%m-%d"),
        run_config["scope"]["lad_names"],
    )


def run_pipeline():
    config: Config = parse_config()
    logger.info(
        "pipeline starting",
        window_months=config.window_months,
        snapshot_date=config.snapshot_date,
    )

    # fetch and process lookup data

    # these are commented out - new national geography info is committed in the reference folder
    # these functions will need to be refactored to fetch/update those files
    # fetch.geography_lookup.fetch()
    # fetch.lsoa_2011_2021_lookup.fetch()
    # process.geography_lookup.process()
    # process.lsoa_2011_2021_lookup.process()

    fetch.postcode_lookup.fetch(force_refresh=True)
    logger.info("lookup data fetch complete")

    logger.info("fetching national level data")
    fetch.population_lookup.fetch(snapshot_date=config.snapshot_date, force_refresh=True)
    # fetch.connectivity.fetch(force_refresh=True)
    fetch.land_registry.fetch(
        window_months=config.window_months, snapshot_date=config.snapshot_date, force_refresh=False
    )

    for district_name in config.lad_names:
        logger.info("running city level pipeline", city=district_name)

        process.postcode_lookup.process(district_name=district_name)
        population_data = process.population_lookup.process(district_name=district_name)
        logger.info("lookup data process complete")

        # fetch the raw data from source
        fetch.police_uk.fetch(
            snapshot_date=config.snapshot_date,
            window_months=config.window_months,
            district_name=district_name,
            force_refresh=False,
        )
        fetch.universal_credit.fetch(
            snapshot_date=config.snapshot_date,
            window_months=config.window_months,
            district_name=district_name,
            force_refresh=True,
        )
        fetch.open_street_map.fetch(
            snapshot_date=config.snapshot_date, district_name=district_name, force_refresh=False
        )
        logger.info("fetch stage complete")

        # process raw data into tabular feature sets
        crime_data = process.police_uk.process(
            window_months=config.window_months,
            snapshot_date=config.snapshot_date,
            district_name=district_name,
        )
        uc_data = process.universal_credit.process(district_name=district_name)
        # connect_data = process.connectivity.process(district_name=district_name)
        price_paid_data = process.land_registry.process(
            window_months=config.window_months,
            snapshot_date=config.snapshot_date,
            district_name=district_name,
        )
        osm_data = process.open_street_map.process(district_name=district_name, snapshot_date=config.snapshot_date)
        logger.info("process stage complete")

        try:
            # combine processed data
            combine.join(
                crime_data,
                uc_data,
                # connect_data,
                price_paid_data,
                osm_data,
                population_data,
                district_name=district_name,
            )

            logger.info("pipeline complete")
        except ValueError:
            logger.exception("combine step validation failed")
            raise


def main():

    quarterly_dates = generate_quarterly_dates()
    all_dates = ANCHOR_DATES + quarterly_dates

    logger.info(f"total runs planned: {len(all_dates)}")
    logger.info(f"anchor dates: {ANCHOR_DATES}")
    logger.info(f"quarterly dates: {len(quarterly_dates)}")

    for snapshot_date in all_dates:
        is_anchor = snapshot_date in ANCHOR_DATES
        cities = ALL_CITIES if is_anchor else PREDICTION_CITY
        run_type = "ANCHOR" if is_anchor else "quarterly"

        all_exist = all(
            (paths.data_output / get_district_slug(c) / f"combined_indicators_{snapshot_date}.parquet").exists()
            for c in cities
        )
        if all_exist:
            logger.info(f"skipping {snapshot_date} - outputs already exist")
            continue

        logger.info(f"runtype={run_type}\n snapshot date={snapshot_date}\n num cities={len(cities)}")

        write_run_config(snapshot_date, cities)

        try:
            run_pipeline()
        except Exception:
            logger.exception(f"pipeline failed for {snapshot_date}")
            continue

        rename_outputs(snapshot_date, cities)

    logger.info("all runs complete")


if __name__ == "__main__":
    main()
