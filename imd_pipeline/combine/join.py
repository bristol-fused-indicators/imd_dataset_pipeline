import polars as pl
from loguru import logger
from project_paths import paths

from imd_pipeline.process import police_uk, universal_credit


def join(*processed_frames: pl.LazyFrame, save_to_disk: bool = True) -> pl.DataFrame:
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
    lsoa_frame = pl.scan_csv(paths.data_lookup / "lsoa_2011_2021_lookup.csv").select(
        pl.col("lsoa_code_21").alias("lsoa_code")
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

    # fill inf/NaN to null, then null to 0
    if float_cols:
        df = df.with_columns(
            [
                pl.when(pl.col(c).is_infinite() | pl.col(c).is_nan()).then(None).otherwise(pl.col(c)).alias(c)
                for c in float_cols
            ]
        )

    logger.info(f"null values filled in columns: {cols_nulls_present}")

    df = df.with_columns(pl.all().exclude("lsoa_code").fill_null(0))

    # assert post fill (probably redundant?)
    remaining = {c: df[c].null_count() for c in df.columns if df[c].null_count() > 0}
    if remaining:
        raise ValueError(f"Nulls remain after fill: {remaining}")

    if save_to_disk:
        df.write_parquet(paths.data_output / "combined_indicators.parquet")
        logger.info(
            "combined dataset written",
            path=str(paths.data_output / "combined_indicators.parquet"),
        )

    return df


if __name__ == "__main__":
    crime_frame = police_uk.process(12, "2025-12-01")
    uc_frame = universal_credit.process()
    join(crime_frame, uc_frame)
