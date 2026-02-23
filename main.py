import tomllib
from dataclasses import dataclass

from project_paths import paths

from imd_pipeline import fetch, process

# from imd_pipeline.fetch import police_uk, universal_credit


@dataclass
class Config:
    window_months: int
    snapshot_date: str


def parse_config() -> Config:
    run_config = tomllib.loads(paths.run_config.read_text())
    pipeline_config = tomllib.loads(paths.pipeline_config.read_text())

    return Config(
        pipeline_config["temporal"]["window_months"],
        run_config["snapshot"]["date"],
    )


def main():
    config: Config = parse_config()

    # fetch the raw data from source
    fetch.police_uk.fetch()
    fetch.universal_credit.fetch()

    # process raw data into tabular feature sets
    crime_data = process.police_uk.process(12, "2025-12-01")
    uc_data = process.universal_credit.process()


if __name__ == "__main__":
    main()
