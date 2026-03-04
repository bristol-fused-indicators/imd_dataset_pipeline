import json
import os
from pathlib import Path
from typing import Any

import polars as pl
from dotenv import load_dotenv
from loguru import logger
from project_paths import paths
from statxplore import http_session, objects

from imd_pipeline.utils.timeframes import months_in_window

load_dotenv()

Json = dict[str, Any]

QUERY_DIR = paths.data_config / "stat_xplore_queries"

QUERY_CONDITIONS = {
    "universal_credit_no_work_required": "BC",
    "universal_credit_planning_for_work": "DF",
    "universal_credit_preparing_for_work": "CE",
    "universal_credit_searching_for_work": "AA",
}
DATE_RECODE_KEY = "str:field:UC_Monthly:F_UC_DATE:DATE_NAME"
QUERY_DATE_STEM = "str:value:UC_Monthly:F_UC_DATE:DATE_NAME:C_UC_DATE"

CONDITION_RECODE_KEY = (
    "str:field:UC_Monthly:V_F_UC_CASELOAD_FULL:CCCONDITIONALITY_REGIME"
)
QUERY_CONDITION_STEM = "str:value:UC_Monthly:V_F_UC_CASELOAD_FULL:CCCONDITIONALITY_REGIME:C_UC_CONDITIONALITY_REGIME"


def construct_queries(
    condition: str,
    months: list[str],
    template_path: Path = QUERY_DIR / "uc_template.json",
) -> dict:

    with open(template_path, encoding="utf-8") as f:
        query = json.load(f)

    if type(query) is not dict:
        raise TypeError("make sure the stat-xplore template query is valid json")

    # set date
    query["recodes"][DATE_RECODE_KEY]["map"] = [
        [f"{QUERY_DATE_STEM}:{month}"] for month in months
    ]

    # set uc condition
    query["recodes"][CONDITION_RECODE_KEY]["map"] = [
        [f"{QUERY_CONDITION_STEM}:{condition}"]
    ]

    return query


def get_queries(
    snapshot_date: str,
    window_months: int,
) -> dict[str, dict]:

    months = months_in_window(snapshot_date, window_months)
    formatted_months = [month.replace("-", "") for month in months]

    return {
        name: construct_queries(condition=condition, months=formatted_months)
        for name, condition in QUERY_CONDITIONS.items()
    }


def get_data(query: dict, session, output_path: Path, force=False) -> dict:
    if output_path.exists() and not force:
        logger.debug("reading cached file", path=output_path)
        return json.loads(output_path.read_text(encoding="utf-8"))

    logger.info("querying statxplore...")

    data = objects.Table.query_json(session, json.dumps(query))
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
    # todo rewrite this - [val[0] is fragile
    key = next(iter(dataset["cubes"]))
    data_body = dataset["cubes"][key]["values"]
    data_body = [
        [val[0] if isinstance(val, list) else val for val in row] for row in data_body
    ]

    # construct dataframe
    df = (
        pl.DataFrame(data_body, schema=months, orient="row")
        .with_columns([pl.Series("lsoa_name", lsoas)])
        .unpivot(index="lsoa_name", variable_name="month")
    )

    return df


def fetch(
    snapshot_date: str,
    window_months: int,
    force_refresh: bool = False,
):
    queries = get_queries(snapshot_date, window_months)
    logger.info("loaded queries", num=len(queries))

    session = http_session.StatSession(api_key=os.environ.get("STATXPLORE_API_KEY", ""))
    logger.info("created statxplore session")

    responses = {
        name: get_data(
            query=query,
            session=session,
            output_path=paths.data_raw / f"{name}.json",
            force=force_refresh,
        )
        for name, query in queries.items()
    }

    logger.info("got responses", count=len(responses), names=list(responses.keys()))

    dataframes = {
        name: transform_to_dataframe(response) for name, response in responses.items()
    }

    logger.debug("transformed queries to dataframes", names=list(dataframes.keys()))

    dataframes = [
        dataframe.with_columns(condition_group=pl.lit(name))
        for name, dataframe in dataframes.items()
    ]
    combined_frame = pl.concat(dataframes)
    logger.debug(
        "condition groups in combined frame",
        groups=combined_frame["condition_group"].unique().to_list(),
    )

    combined_frame.write_parquet(file=paths.data_raw / "universal_credit.parquet")

    logger.info("universal credit data written to file")


if __name__ == "__main__":
    snapshot_date = "2025-12-01"
    window_months = 12
    fetch(
        force_refresh=True,
        snapshot_date=snapshot_date,
        window_months=window_months,
    )
