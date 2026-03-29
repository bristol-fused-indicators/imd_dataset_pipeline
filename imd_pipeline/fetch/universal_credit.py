import json
import os
from pathlib import Path
from typing import Any

import polars as pl
from dotenv import load_dotenv
from loguru import logger
from project_paths import paths

from imd_pipeline.utils.http import cached_fetch_json, create_session
from imd_pipeline.utils.lsoas import get_district_slug, get_target_codes
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

LSOA_KEY = "str:field:UC_Monthly:V_F_UC_CASELOAD_FULL:COA_CODE"
LSOA_STEM = (
    "str:value:UC_Monthly:V_F_UC_CASELOAD_FULL:COA_CODE:V_C_MASTERGEOG21_LSOA_TO_MSOA"
)

STATXPLORE_URL = "https://stat-xplore.dwp.gov.uk/webapi/rest/v1/table"


def construct_queries(
    condition: str,
    months: list[str],
    district_name: str,
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

    codes = [[f"{LSOA_STEM}:{code}"] for code in get_target_codes(district_name)]
    query["recodes"][LSOA_KEY]["map"] = codes

    return query


def get_queries(
    snapshot_date: str, window_months: int, district_name: str
) -> dict[str, dict]:

    months = months_in_window(snapshot_date, window_months)
    formatted_months = [month.replace("-", "") for month in months]

    return {
        name: construct_queries(
            condition=condition, months=formatted_months, district_name=district_name
        )
        for name, condition in QUERY_CONDITIONS.items()
    }


def get_data(query: dict, output_path: Path, api_key: str, force_refresh=False) -> dict:
    """Fetch UC data from StatsExplore API, with caching to avoid redundant downloads."""

    if output_path.exists() and not force_refresh:
        logger.debug("reading cached file", path=output_path)
        return json.loads(output_path.read_text(encoding="utf-8"))

    logger.info("querying statxplore...", path=output_path)

    session = create_session()
    headers = {"APIKey": api_key, "Content-Type": "application/json"}

    response = session.post(STATXPLORE_URL, headers=headers, json=query)
    response.raise_for_status()
    data = response.json()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data), encoding="utf-8")

    logger.info("saved response", path=output_path)
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
    # todo rewrite this - [val[0] is fragile, plus nested list comp
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
    district_name: str,
    force_refresh: bool = False,
):
    queries = get_queries(snapshot_date, window_months, district_name)
    logger.info("loaded queries", num=len(queries))

    output_dir = paths.data_raw / get_district_slug(district_name) / "universal_credit"

    output_dir.mkdir(parents=True, exist_ok=True)

    responses = {
        name: get_data(
            query=query,
            output_path=output_dir / f"{name}.json",
            api_key=os.environ.get("STATXPLORE_API_KEY", ""),
            force_refresh=force_refresh,
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

    combined_frame.write_parquet(file=output_dir / "universal_credit.parquet")

    logger.info("universal credit data written to file")


if __name__ == "__main__":
    snapshot_date = "2025-12-01"
    window_months = 12
    district_name = "Bristol, City of"
    fetch(
        force_refresh=True,
        snapshot_date=snapshot_date,
        district_name=district_name,
        window_months=window_months,
    )
