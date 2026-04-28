import polars as pl
from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    MaterializeResult,
    StaticPartitionsDefinition,
    asset,
)
from project_paths import paths

from imd_pipeline import combine, fetch, process
from imd_pipeline.utils.lsoas import get_district_slug

from .configs import TimeframeConfig
from .policies import (
    annual_freshness_policy,
    monthly_freshness_policy,
    quarterly_freshness_policy,
)

DISTRICT_NAMES = [
    "Bristol, City of",
    "Exeter",
    "Plymouth",
    "Newcastle upon Tyne",
    "Bournemouth, Christchurch and Poole",
    "Sheffield",
]
district_partitions = StaticPartitionsDefinition(DISTRICT_NAMES)


@asset(freshness_policy=annual_freshness_policy)
def connectivity_raw_data(context: AssetExecutionContext, config: TimeframeConfig):
    fetch.connectivity.fetch(force_refresh=config.force_refresh)

    output = paths.data_raw / "connectivity.parquet"
    stat = output.stat()

    return MaterializeResult(
        metadata={
            "snapshot_date": config.snapshot_date,
            "window_months": config.window_months,
            "force_refresh": config.force_refresh,
            "output_path": str(output),
            "file_size_mb": round(stat.st_size / 1_048_576, 2),
            "file_modified": stat.st_mtime,
        }
    )


@asset(freshness_policy=monthly_freshness_policy)
def land_registry_raw_data(context: AssetExecutionContext, config: TimeframeConfig):

    fetch.land_registry.fetch(
        snapshot_date=config.snapshot_date,
        window_months=config.window_months,
        force_refresh=config.force_refresh,
    )

    output = paths.data_raw / "land_registry"
    files_available = sorted(path.name for path in output.iterdir())

    return MaterializeResult(
        metadata={
            "snapshot_date": config.snapshot_date,
            "window_months": config.window_months,
            "force_refresh": config.force_refresh,
            "output_path": str(output),
            "files_available": files_available,
        }
    )


@asset(partitions_def=district_partitions, freshness_policy=quarterly_freshness_policy)
def open_street_map_raw_data(context: AssetExecutionContext, config: TimeframeConfig):
    district_name = context.partition_key
    fetch.open_street_map.fetch(
        district_name=district_name,
        force_refresh=config.force_refresh,
        snapshot_date=config.snapshot_date,
    )

    district_slug = get_district_slug(district_name)
    output = paths.data_raw / district_slug / "osm" / f"overpass_response_{config.snapshot_date}.json"
    if output.exists():
        stat = output.stat()
    else:
        stat = None

    return MaterializeResult(
        metadata={
            "district_name": district_name,
            "snapshot_date": config.snapshot_date,
            "window_months": config.window_months,
            "force_refresh": config.force_refresh,
            "output_path": str(output),
            "file_size_mb": round(stat.st_size / 1_048_576, 2) if stat else "n/a",
            "file_modified": stat.st_mtime if stat else "na/a",
        }
    )


@asset(partitions_def=district_partitions, freshness_policy=monthly_freshness_policy)
def crime_raw_data(context: AssetExecutionContext, config: TimeframeConfig):
    district_name = context.partition_key
    fetch.police_uk.fetch(
        district_name=district_name,
        force_refresh=config.force_refresh,
        snapshot_date=config.snapshot_date,
        window_months=config.window_months,
    )

    district_slug = get_district_slug(district_name)
    output = paths.data_raw / district_slug / "police_uk"
    files_available = sorted(path.name for path in output.iterdir())

    return MaterializeResult(
        metadata={
            "district_name": district_name,
            "snapshot_date": config.snapshot_date,
            "window_months": config.window_months,
            "force_refresh": config.force_refresh,
            "output_path": str(output),
            "files_available": files_available,
        }
    )


@asset(partitions_def=district_partitions, freshness_policy=monthly_freshness_policy)
def universal_credit_raw_data(context: AssetExecutionContext, config: TimeframeConfig):
    district_name = context.partition_key
    fetch.universal_credit.fetch(
        district_name=district_name,
        force_refresh=config.force_refresh,
        snapshot_date=config.snapshot_date,
        window_months=config.window_months,
    )

    district_slug = get_district_slug(district_name)
    output = paths.data_raw / district_slug / "universal_credit.parquet"
    stat = output.stat()

    return MaterializeResult(
        metadata={
            "district_name": district_name,
            "snapshot_date": config.snapshot_date,
            "window_months": config.window_months,
            "force_refresh": config.force_refresh,
            "output_path": str(output),
            "file_size_mb": round(stat.st_size / 1_048_576, 2),
            "file_modified": stat.st_mtime,
        }
    )


@asset(
    partitions_def=district_partitions,
    deps=[connectivity_raw_data],
    automation_condition=AutomationCondition.eager(),
)
def connectivity_processed_data(context: AssetExecutionContext, config: TimeframeConfig):
    district_name = context.partition_key
    process.connectivity.process(district_name=district_name, persist_processed_file=True)

    district_slug = get_district_slug(district_name)
    output = paths.data_processed / district_slug / "connectivity.parquet"
    stat = output.stat()

    return MaterializeResult(
        metadata={
            "district_name": district_name,
            "snapshot_date": config.snapshot_date,
            "window_months": config.window_months,
            "force_refresh": config.force_refresh,
            "output_path": str(output),
            "file_size_mb": round(stat.st_size / 1_048_576, 2),
            "file_modified": stat.st_mtime,
        }
    )


@asset(
    partitions_def=district_partitions,
    deps=[land_registry_raw_data],
)
def land_registry_processed_data(context: AssetExecutionContext, config: TimeframeConfig):
    district_name = context.partition_key
    process.land_registry.process(
        snapshot_date=config.snapshot_date,
        window_months=config.window_months,
        district_name=district_name,
        persist_processed_file=True,
    )

    district_slug = get_district_slug(district_name)
    output = paths.data_processed / district_slug / "land_registry.parquet"
    stat = output.stat()

    return MaterializeResult(
        metadata={
            "district_name": district_name,
            "snapshot_date": config.snapshot_date,
            "window_months": config.window_months,
            "force_refresh": config.force_refresh,
            "output_path": str(output),
            "file_size_mb": round(stat.st_size / 1_048_576, 2),
            "file_modified": stat.st_mtime,
        }
    )


@asset(
    partitions_def=district_partitions,
    deps=[open_street_map_raw_data],
    automation_condition=AutomationCondition.eager(),
)
def open_street_map_processed_data(context: AssetExecutionContext, config: TimeframeConfig):
    district_name = context.partition_key
    process.open_street_map.process(district_name=district_name, persist_processed_file=True)

    district_slug = get_district_slug(district_name)
    output = paths.data_processed / district_slug / "open_street_map.parquet"
    stat = output.stat()

    return MaterializeResult(
        metadata={
            "district_name": district_name,
            "snapshot_date": config.snapshot_date,
            "window_months": config.window_months,
            "force_refresh": config.force_refresh,
            "output_path": str(output),
            "file_size_mb": round(stat.st_size / 1_048_576, 2),
            "file_modified": stat.st_mtime,
        }
    )


@asset(
    partitions_def=district_partitions,
    deps=[crime_raw_data],
)
def crime_processed_data(context: AssetExecutionContext, config: TimeframeConfig):
    district_name = context.partition_key
    process.police_uk.process(
        snapshot_date=config.snapshot_date,
        window_months=config.window_months,
        district_name=district_name,
        persist_processed_file=True,
    )

    district_slug = get_district_slug(district_name)
    output = paths.data_processed / district_slug / "police_uk.parquet"
    stat = output.stat()

    return MaterializeResult(
        metadata={
            "district_name": district_name,
            "snapshot_date": config.snapshot_date,
            "window_months": config.window_months,
            "force_refresh": config.force_refresh,
            "output_path": str(output),
            "file_size_mb": round(stat.st_size / 1_048_576, 2),
            "file_modified": stat.st_mtime,
        }
    )


@asset(
    partitions_def=district_partitions,
    deps=[universal_credit_raw_data],
    automation_condition=AutomationCondition.eager(),
)
def universal_credit_processed_data(context: AssetExecutionContext, config: TimeframeConfig):
    district_name = context.partition_key
    process.universal_credit.process(district_name=district_name, persist_processed_file=True)

    district_slug = get_district_slug(district_name)
    output = paths.data_processed / district_slug / "universal_credit.parquet"
    stat = output.stat()

    return MaterializeResult(
        metadata={
            "district_name": district_name,
            "snapshot_date": config.snapshot_date,
            "window_months": config.window_months,
            "force_refresh": config.force_refresh,
            "output_path": str(output),
            "file_size_mb": round(stat.st_size / 1_048_576, 2),
            "file_modified": stat.st_mtime,
        }
    )


@asset(
    partitions_def=district_partitions,
    deps=[
        connectivity_processed_data,
        land_registry_processed_data,
        open_street_map_processed_data,
        crime_processed_data,
        universal_credit_processed_data,
    ],
    automation_condition=AutomationCondition.eager(),
)
def combined_data(context: AssetExecutionContext):
    district_name = context.partition_key
    district_slug = get_district_slug(district_name)
    all_frames = [
        pl.scan_parquet(paths.data_processed / district_slug / "police_uk.parquet"),
        pl.scan_parquet(paths.data_processed / district_slug / "universal_credit.parquet"),
        pl.scan_parquet(paths.data_processed / district_slug / "open_street_map.parquet"),
        pl.scan_parquet(paths.data_processed / district_slug / "land_registry.parquet"),
        pl.scan_parquet(paths.data_processed / district_slug / "connectivity.parquet"),
    ]
    combine.join(*all_frames, district_name=district_name)

    return MaterializeResult()


all_assets = [
    connectivity_raw_data,
    land_registry_raw_data,
    open_street_map_raw_data,
    crime_raw_data,
    universal_credit_raw_data,
    connectivity_processed_data,
    land_registry_processed_data,
    open_street_map_processed_data,
    crime_processed_data,
    universal_credit_processed_data,
    combined_data,
]
