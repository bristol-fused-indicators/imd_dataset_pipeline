from pathlib import Path

import polars as pl
from loguru import logger


def filter_bristol(
    lf: pl.LazyFrame, code_col: str, geography_path: Path
) -> pl.LazyFrame:
    bristol_codes = pl.scan_csv(geography_path).select("lsoa_code").unique()

    if code_col == "lsoa_code":
        result = lf.join(bristol_codes, on="lsoa_code", how="semi")
    else:
        result = lf.join(
            bristol_codes,
            left_on=code_col,
            right_on="lsoa_code",
            how="semi",
        ).rename({code_col: "lsoa_code"})

    logger.debug("filter_bristol applied on column '{}'", code_col)
    return result


def convert_2011_to_2021(
    lf: pl.LazyFrame, col: str, lookup_path: Path, by: str = "code"
) -> pl.LazyFrame:
    old_col = f"lsoa_{by}_11"
    new_col = f"lsoa_{by}_21"

    mapping = pl.scan_csv(lookup_path).select(old_col, new_col).unique()

    result = (
        lf.join(mapping, left_on=col, right_on=old_col, how="left")
        .with_columns(pl.coalesce(new_col, col).alias(col))
        .drop(new_col)
    )

    logger.debug("convert_2011_to_2021 applied on column '{}' by={}", col, by)
    return result


def map_lsoa_names_to_codes(
    lf: pl.LazyFrame, name_col: str, lookup_path: Path
) -> pl.LazyFrame:
    mapping = pl.scan_csv(lookup_path).select("lsoa_name_21", "lsoa_code_21").unique()

    result = (
        lf.join(mapping, left_on=name_col, right_on="lsoa_name_21", how="left")
        .drop(name_col)
        .rename({"lsoa_code_21": "lsoa_code"})
    )

    logger.debug("map_lsoa_names_to_codes applied on column '{}'", name_col)
    return result


def map_postcode_to_lsoa_code(
    lf: pl.LazyFrame, postcode_col: str, lookup_path: Path
) -> pl.LazyFrame:

    logger.debug("mapping postcode to lsoa", on_col=postcode_col)
    mapping = pl.scan_csv(lookup_path).select("postcode", "lsoa_code")

    return lf.join(mapping, how="left", left_on=postcode_col, right_on="postcode").drop(
        postcode_col
    )
