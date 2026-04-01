import tomllib
from datetime import datetime

from icecream import ic
from loguru import logger
from project_paths import paths

from imd_pipeline import combine, fetch, process
from imd_pipeline.config import Config


def parse_config() -> Config:
    run_config = tomllib.loads(paths.run_config.read_text())
    pipeline_config = tomllib.loads(paths.pipeline_config.read_text())

    return Config(
        pipeline_config["temporal"]["window_months"],
        datetime.strftime(run_config["snapshot"]["date"], "%Y-%m-%d"),
        run_config["scope"]["lad_names"],
    )


def main():
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

    fetch.postcode_lookup.fetch()
    logger.info("lookup data fetch complete")

    logger.info("fetching national level data")
    fetch.population_lookup.fetch(snapshot_date=config.snapshot_date, force_refresh=True)
    fetch.connectivity.fetch()
    ic(config.window_months, config.snapshot_date, config.lad_names)
    fetch.land_registry.fetch(window_months=config.window_months, snapshot_date=config.snapshot_date)

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
        )
        fetch.open_street_map.fetch(
            snapshot_date=config.snapshot_date,
            district_name=district_name,
        )
        logger.info("fetch stage complete")

        # process raw data into tabular feature sets
        crime_data = process.police_uk.process(
            window_months=config.window_months,
            snapshot_date=config.snapshot_date,
            district_name=district_name,
        )
        uc_data = process.universal_credit.process(district_name=district_name)
        connect_data = process.connectivity.process(district_name=district_name)
        price_paid_data = process.land_registry.process(
            window_months=config.window_months,
            snapshot_date=config.snapshot_date,
            district_name=district_name,
        )
        osm_data = process.open_street_map.process(district_name=district_name, snapshot_date=config.snapshot_date)
        logger.info("process stage complete")

        try:
            # combine processed data
            combined = combine.join(
                crime_data,
                uc_data,
                connect_data,
                price_paid_data,
                osm_data,
                population_data,
                district_name=district_name,
            )

            logger.info("pipeline complete")
        except ValueError:
            logger.exception("combine step validation failed")
            raise


if __name__ == "__main__":
    main()
