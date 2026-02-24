import tomllib

from loguru import logger
from project_paths import paths

from imd_pipeline import combine, fetch, process
from imd_pipeline.config import Config


def parse_config() -> Config:
    run_config = tomllib.loads(paths.run_config.read_text())
    pipeline_config = tomllib.loads(paths.pipeline_config.read_text())

    return Config(
        pipeline_config["temporal"]["window_months"],
        run_config["snapshot"]["date"],
    )


def main():
    config: Config = parse_config()
    logger.info("pipeline starting", window_months=config.window_months, snapshot_date=config.snapshot_date)

    # fetch the raw data from source
    fetch.police_uk.fetch()
    fetch.universal_credit.fetch()
    fetch.connectivity.fetch()
    logger.info("fetch stage complete")

    # process raw data into tabular feature sets
    crime_data = process.police_uk.process(12, "2025-12-01")
    uc_data = process.universal_credit.process()
    connect_data = process.connectivity.process()
    logger.info("process stage complete")

    # combine processed data
    combined = combine.join(crime_data, uc_data, connect_data)
    logger.info("pipeline complete")


if __name__ == "__main__":
    main()
