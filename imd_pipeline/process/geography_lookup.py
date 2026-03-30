from turtle import pd

import polars as pl
from loguru import logger
from project_paths import paths
import geopandas as gpd
import json
from shapely.geometry import shape

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
            props["geo_shape"] = json.dumps({"coordinates": geom.get("rings")})
       
        rows.append(props)


    # 4️⃣ Convert to DataFrame
    full_df = pl.DataFrame(rows)

    pl.Config.set_tbl_cols(-1)
    print(full_df.head())
    print(full_df.columns)



    lac_filter = pl.read_csv(lac_path).filter(pl.col("local_authority_code").is_in(local_authorities)).select(pl.col("lsoa_code_21").alias("lsoa_code"), pl.col("local_authority_code"))
    
    
    df = full_df.join(lac_filter, left_on="LSOA21CD", right_on="lsoa_code", how="inner")



    print(df.head())
    print(df.shape)




    df = df.select([
        pl.col("LSOA21CD").alias("lsoa_code"),
        pl.col("LAT").alias("latitude"),
        pl.col("LONG").alias("longitude"),
        pl.col("geo_shape"),
        pl.col("local_authority_code")
    ])

    print(df.head())

    return df.lazy().sink_csv(paths.data_lookup / "geography_lookup_new.csv")

# for reference, the original code was:
def process_old() -> pl.LazyFrame:
    logger.info("processing geography lookup data", source=str(paths.data_raw / "lookup" / "geography_lookup.parquet"))
    return (
        pl.scan_parquet(paths.data_raw / "lookup" / "geography_lookup.parquet")
        .select(
            [pl.col("Geo Point").alias("geo_point"),
             pl.col("Geo Shape").alias("geo_shape"),
             pl.col("LSOA Code").alias("lsoa_code"),
             pl.col("Easting").alias("easting"),
             pl.col("Northing").alias("northing"),
             pl.col("Longitude").alias("longitude"),
             pl.col("Latitude").alias("latitude"),
             pl.col("Local Authority Code").alias("local_authority_code")]
             ).sink_csv(paths.data_lookup / "geography_lookup.csv")
             )





if __name__ == '__main__':
    process(local_authorities=["E06000023"])