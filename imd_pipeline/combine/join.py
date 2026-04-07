import os

import polars as pl

# import polars.selectors
from loguru import logger
from project_paths import paths

from imd_pipeline.process import police_uk, universal_credit
from imd_pipeline.utils.lsoas import get_district_slug


def join(*processed_frames: pl.LazyFrame, district_name: str, save_to_disk: bool = True) -> pl.DataFrame:
    """Joins all processed indicator frames onto the LSOA spine, validates, fills nulls/NaN/inf, and saves.

    Structural checks (row count, duplicate keys, null keys) raise on failure.
    Data quality issues (nulls, NaN, inf) are logged as warnings then filled with 0.

    Args:
        *processed_frames: LazyFrames to join, each keyed on lsoa_code.
        save_to_disk: If True, writes the combined result to combined_indicators.parquet.

    Returns:
        DataFrame of all indicators joined by lsoa_code, with no nulls, NaN, or inf.
    """

    logger.info("joining processed frames", frame_count=len(processed_frames))
    lsoa_frame = (
        pl.scan_csv(paths.data_reference / "lsoa_lookup.csv")
        .filter(pl.col("lad_name").eq(district_name))
        .select(pl.col("lsoa_code_21").alias("lsoa_code"))
        .unique()
    )

    spine_count = lsoa_frame.collect().height

    combined = lsoa_frame
    for frame in processed_frames:
        combined = combined.join(frame, on="lsoa_code", how="left")

    df = combined.collect()

    # structural checks - raise on failure
    if df.height != spine_count:
        raise ValueError(f"Row count mismatch: expected {spine_count} from spine, got {df.height}")

    if df["lsoa_code"].n_unique() != df.height:
        df_dupes = df.group_by(pl.col("lsoa_code")).len(name="tally").filter(pl.col("tally") > 1)
        print(df_dupes)
        raise ValueError(f"Duplicate lsoa_codes: {df.height - df['lsoa_code'].n_unique()} duplicates found")

    if df["lsoa_code"].null_count() > 0:
        raise ValueError(f"Null lsoa_codes: {df['lsoa_code'].null_count()} nulls found")

    # data quality warnings - log per column
    float_cols = [c for c in df.columns if df[c].dtype.is_float()]

    for col in float_cols:
        inf_count = df[col].is_infinite().sum()
        if inf_count > 0:
            logger.warning("inf values detected", column=col, count=inf_count)

        nan_count = df[col].is_nan().sum()
        if nan_count > 0:
            logger.warning("NaN values detected", column=col, count=nan_count)

    cols_nulls_present = []
    for col in df.columns:
        if col == "lsoa_code":
            continue
        null_count = df[col].null_count()
        if null_count > 0:
            cols_nulls_present.append(col)
            # logger.warning("null values detected", column=col, count=null_count)

    # fill inf → null (float cols only), then NaN → 0, then null → 0
    if float_cols:
        df = df.with_columns(
            pl.when(pl.col(c).is_infinite()).then(None).otherwise(pl.col(c)).alias(c)
            for c in float_cols
        )
    df = df.fill_nan(0).fill_null(0)

    logger.info(f"null values filled in columns: {cols_nulls_present}")

    remaining_null = {c: df[c].null_count() for c in df.columns if df[c].null_count() > 0}
    if remaining_null:
        raise ValueError(f"Nulls remain after fill: {remaining_null}")

    remaining_nan = {c: df[c].is_nan().sum() for c in float_cols if df[c].is_nan().sum() > 0}
    if remaining_nan:
        raise ValueError(f"NaN values remain after fill: {remaining_nan}")

    if save_to_disk:
        output_dir = paths.data_output / get_district_slug(district_name)
        if not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "combined_indicators.parquet"
        df.write_parquet(output_path)
        logger.info(
            f"combined dataset written, path={output_path}",
            path=output_path,
        )

    return df


if __name__ == "__main__":
    crime_frame = police_uk.process(12, "2025-12-01")
    uc_frame = universal_credit.process()
    join(crime_frame, uc_frame)
