import polars as pl
from loguru import logger
from project_paths import paths

from imd_pipeline.process import police_uk, universal_credit


def join(*processed_frames: pl.LazyFrame, save_to_disk: bool = True):
    """Joins all processed indicator frames onto the Bristol LSOA spine and optionally saves to parquet.

    Args:
        *processed_frames: LazyFrames to join, each keyed on lsoa_code.
        save_to_disk: If True, sinks the combined result to combined_indicators.parquet.

    Returns:
        LazyFrame of all indicators joined by lsoa_code.
    """

    logger.info("joining processed frames", frame_count=len(processed_frames))
    lsoa_frame = pl.scan_csv(paths.data_lookup / "lsoa_2011_2021_lookup.csv").select(
        pl.col("lsoa_code_21").alias("lsoa_code")
    )

    combined = lsoa_frame
    for frame in processed_frames:
        combined = combined.join(frame, on="lsoa_code", how="left")

    if save_to_disk:
        combined.sink_parquet(paths.data_output / "combined_indicators.parquet")
        logger.info(
            "combined dataset written",
            path=str(paths.data_output / "combined_indicators.parquet"),
        )

    return combined


if __name__ == "__main__":
    crime_frame = police_uk.process(12, "2025-12-01")
    uc_frame = universal_credit.process()
    join(crime_frame, uc_frame)
