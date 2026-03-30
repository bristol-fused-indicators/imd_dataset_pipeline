import polars as pl
from loguru import logger
from project_paths import paths
import json

def process(local_authorities: list[str]) -> pl.LazyFrame:

    input_path = paths.data_raw / "lookup" / "geography_lookup.geojson"
    lac_path = paths.data_lookup / "lsoa_2011_2021_lookup.csv"

    logger.info("processing geography data", source=str(input_path))

    with open(input_path, "r", encoding="utf-8") as f:
        geojson_data = json.load(f)

    features = geojson_data.get("features", [])

    rows = []
    for feat in features:
        props = feat.get("properties") or feat.get("attributes") or {}
       
        geom = feat.get("geometry")
        if geom:
            # Save geometry as JSON string with "coordinates" key
            props["geo_shape"] = json.dumps({"coordinates": [geom.get("rings")]})
       
        rows.append(props)

    full_df = pl.DataFrame(rows)

    lac_filter = pl.read_csv(lac_path).filter(pl.col("local_authority_code").is_in(local_authorities)).select(pl.col("lsoa_code_21").alias("lsoa_code"), pl.col("local_authority_code"))
    df = full_df.join(lac_filter, left_on="LSOA21CD", right_on="lsoa_code", how="inner")

    df = df.select([
        pl.col("LSOA21CD").alias("lsoa_code"),
        pl.col("LAT").alias("latitude"),
        pl.col("LONG").alias("longitude"),
        pl.col("geo_shape"),
        pl.col("local_authority_code")
    ])

    return df.lazy().sink_csv(paths.data_lookup / "geography_lookup.csv")




if __name__ == '__main__':
    process(local_authorities=["E06000023"])