import json
import os
from pathlib import Path

import polars as pl
from dotenv import load_dotenv
from icecream import ic
from loguru import logger
from project_paths import paths
from statxplore import http_session, objects

load_dotenv()

QUERY_DIR = paths.data_config / "stat_xplore_queries"

QUERIES = {
    "universal_credit_no_work_required": QUERY_DIR / "uc_no_work_req_2025.json",
    "universal_credit_planning_for_work": QUERY_DIR / "uc_planning_work_2025.json",
    "universal_credit_preparing_for_work": QUERY_DIR / "uc_prep_work_2025.json",
    "universal_credit_searching_for_work": QUERY_DIR / "uc_search_work_2025.json",
}


def get_queries() -> dict[str, str]:
    return {name: query.read_text() for name, query in QUERIES.items()}


def get_data(query: str, session, output_path: Path, force=False) -> dict:
    if output_path.exists() and not force:
        logger.debug("reading cached file", path=output_path)
        return json.loads(output_path.read_text(encoding="utf-8"))

    logger.info("querying statxplore...")

    data = objects.Table.query_json(session, query)
    output_path.write_text(data=json.dumps(data), encoding="utf-8")
    return data


def transform_to_dataframe(dataset) -> pl.DataFrame:

    months = [
        month.replace(" ", "_").lower()  # replace whitespace with underscore
        for field in dataset.get("fields")
        for item in field.get("items")
        for month in item.get("labels", "")
        if field.get("label") == "Month"
    ]

    lsoas = [
        lsoa
        for field in dataset.get("fields")
        for item in field.get("items")
        for lsoa in item.get("labels", "")
        if field.get("label") == "National - Regional - LA - OAs"
    ]

    # access the data
    # todo rewrite this
    key = next(iter(dataset["cubes"]))
    data_body = dataset["cubes"][key]["values"]
    data_body = [
        [val[0] if isinstance(val, list) else val for val in row] for row in data_body
    ]

    # construct dataframe
    dataset = pl.DataFrame(data_body, schema=months, orient="row").with_columns(
        [pl.Series("lsoa_name", lsoas)]
    )

    return dataset


def get_all_data(force: bool = False):
    queries = get_queries()
    logger.info("loaded queries", num=len(queries))

    session = http_session.StatSession(api_key=os.environ.get("STATXPLORE_API_KEY", ""))
    logger.info("created statxplore session")

    responses = {
        name: get_data(
            query=query,
            session=session,
            output_path=paths.data_raw / f"{name}.json",
            force=force,
        )
        for name, query in queries.items()
    }

    logger.info("got responses", count=len(responses))

    dataframes = {
        name: transform_to_dataframe(response) for name, response in responses.items()
    }

    for name, dataframe in dataframes.items():
        dataframe.write_parquet(file=paths.data_raw / f"{name}.parquet")

    logger.info("universal credit data written to file")


def main():
    get_all_data(force=False)


if __name__ == "__main__":
    main()
