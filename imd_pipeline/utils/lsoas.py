from pathlib import Path

import polars as pl
from loguru import logger
from project_paths import paths


def get_target_codes(district_name: str):
    return (
        pl.read_csv(paths.data_reference / "lsoa_lookup.csv")
        .filter(pl.col("lad_name") == district_name)
        .get_column("lsoa_code_21")
        .to_list()
    )


def get_district_slug(district_name: str) -> str:
    """Converts a LAD name to a filesystem safe string.

    Example:
        >>> district_slug("Bristol, City of")
        'bristol_city_of'
        >>> district_slug("Bournemouth, Christchurch and Poole")
        'bournemouth_christchurch_and_poole'
    """
    return district_name.replace(" ", "_").lower().strip("_").replace(",", "")


def filter_lsoas(lf: pl.LazyFrame, code_col: str, district_name: str, geography_path: Path) -> pl.LazyFrame:
    """Filters a Polars LazyFrame to region's LSOAs only.

    Args:
        lf: Input LazyFrame.
        code_col: Column containing LSOA codes to filter on.
        geography_path: Path to the geography lookup CSV.

    Returns:
        Filtered LazyFrame with the LSOA code column renamed to lsoa_code.
    """
    lsoa_codes = (
        pl.scan_csv(geography_path)
        .filter(pl.col("lad_name") == district_name)
        .select(pl.col("lsoa_code_21").alias("lsoa_code"))
        .unique()
    )

    if code_col == "lsoa_code":
        result = lf.join(lsoa_codes, on="lsoa_code", how="semi")
    else:
        result = lf.join(
            lsoa_codes,
            left_on=code_col,
            right_on="lsoa_code",
            how="semi",
        ).rename({code_col: "lsoa_code"})

    logger.debug("filter_lsoas applied on column '{}'", code_col)
    return result


def convert_2011_to_2021(lf: pl.LazyFrame, col: str, lookup_path: Path, by: str = "code") -> pl.LazyFrame:
    """Converts LSOA codes or names from the 2011 to 2021 classification.

    Args:
        lf: Input LazyFrame.
        col: Column containing 2011 LSOA codes or names to convert.
        lookup_path: Path to the 2011-to-2021 lookup CSV.
        by: Whether to match on "code" or "name".

    Returns:
        LazyFrame with col updated to 2021 values where a mapping exists.
    """
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


def map_lsoa_names_to_codes(lf: pl.LazyFrame, name_col: str, lookup_path: Path) -> pl.LazyFrame:
    """Replaces a column of 2021 LSOA names with their corresponding LSOA codes.

    Args:
        lf: Input LazyFrame.
        name_col: Column containing LSOA names.
        lookup_path: Path to the lookup CSV.

    Returns:
        LazyFrame with name_col replaced by lsoa_code.
    """
    mapping = pl.scan_csv(lookup_path).select("lsoa_name_21", "lsoa_code_21").unique()

    result = (
        lf.join(mapping, left_on=name_col, right_on="lsoa_name_21", how="left")
        .drop(name_col)
        .rename({"lsoa_code_21": "lsoa_code"})
    )

    logger.debug("map_lsoa_names_to_codes applied on column '{}'", name_col)
    return result


def map_postcode_to_lsoa_code(lf: pl.LazyFrame, postcode_col: str, lookup_path: Path) -> pl.LazyFrame:
    """Replaces a postcode column with the corresponding LSOA code.

    Args:
        lf: Input LazyFrame.
        postcode_col: Column containing postcodes.
        lookup_path: Path to the postcode lookup CSV.

    Returns:
        LazyFrame with postcode_col dropped and lsoa_code added.
    """
    logger.debug("mapping postcode to lsoa", on_col=postcode_col)
    mapping = pl.scan_csv(lookup_path).select("postcode", "lsoa_code")

    return lf.join(mapping, how="left", left_on=postcode_col, right_on="postcode").drop(postcode_col)
