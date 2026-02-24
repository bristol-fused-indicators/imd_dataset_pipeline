import json
from typing import Iterable

import geopandas as gpd
import pandas as pd
import polars as pl
from geopandas import GeoDataFrame
from icecream import ic
from project_paths import paths
from shapely import LineString, Point, Polygon


def count_ammenities(
    feature_frame: GeoDataFrame,
    point_osm_data: GeoDataFrame,
    ammenities: Iterable,
    distance: int,
) -> pd.Series:
    _ammenities = {
        ammenity for ammenity in ammenities
    }  # convert to set to use membership methods

    lsoa_gdf = feature_frame[["lsoa_code", f"geom_{distance}"]]
    lsoa_gdf.set_geometry(f"geom_{distance}", inplace=True)

    joined_gdf = point_osm_data.sjoin(lsoa_gdf, how="inner", predicate="within")
    joined_gdf["helper_column"] = joined_gdf["tags"].apply(
        lambda x: (
            (not x.keys().isdisjoint({"amenity"}))
            and (x.get("amenity", "") in _ammenities)
        )
    )

    filtered_gdf = joined_gdf[joined_gdf["helper_column"]]
    gdf_agg = filtered_gdf[["lsoa_code", "id"]].groupby(["lsoa_code"]).count()

    return gdf_agg["id"]


def format_osm_geodataframes(
    map_elements: list,
) -> tuple[GeoDataFrame, GeoDataFrame, GeoDataFrame]:
    # seperate out map elements of different geometries

    point_data = [element for element in map_elements if element.get("type") == "node"]
    polygon_data = [
        element
        for element in map_elements
        if element.get("type") == "way"
        and "highway" not in element.get("tags", {}).keys()
        and len(element.get("geometry", [])) >= 4
    ]
    line_data = [
        element
        for element in map_elements
        if element.get("type") == "way" and "highway" in element.get("tags", {}).keys()
    ]

    invalid_polygons = [
        element
        for element in map_elements
        if element.get("type") == "way"
        and "highway" not in element.get("tags", {}).keys()
        and len(element.get("geometry", [])) < 4
    ]  # ? maybe have to convert these to nodes? they're all benches, probably fine to exclude

    # create a geodataframe for each geometry type.
    # specify the lon/lat degrees coord system when creating, then convert each to metric to match the lsoa dataframe format

    points_gdf = GeoDataFrame(
        data=point_data,
        geometry=[Point(element["lon"], element["lat"]) for element in point_data],
        crs="EPSG:4326",
    )
    points_gdf = points_gdf.to_crs(epsg=27700)

    polygons_gdf = GeoDataFrame(
        data=polygon_data,
        geometry=[
            Polygon(
                [
                    (node.get("lon"), node.get("lat"))
                    for node in element.get("geometry", {})
                ]
            )
            for element in polygon_data
        ],
        crs="EPSG:4326",
    )
    polygons_gdf = polygons_gdf.to_crs(epsg=27700)

    lines_gdf = GeoDataFrame(
        data=line_data,
        geometry=[
            LineString(
                [
                    (node.get("lon"), node.get("lat"))
                    for node in element.get("geometry", {})
                ]
            )
            for element in line_data
        ],
        crs="EPSG:4326",
    )
    lines_gdf = lines_gdf.to_crs(epsg=27700)

    return points_gdf, polygons_gdf, lines_gdf


def get_polygon(x) -> Polygon:
    return Polygon(json.loads(x).get("coordinates")[0][0])


def process() -> pl.LazyFrame:
    response_file = paths.data_raw / "osm" / "overpass_response.json"
    with open(response_file) as file:
        data = json.load(file)
    map_elements = data.get("elements")

    ic(len(map_elements))

    osm_points_gdf, osm_polygons_gdf, osm_lines_gdf = format_osm_geodataframes(
        map_elements=map_elements
    )
    ic(osm_lines_gdf.shape, osm_polygons_gdf.shape, osm_points_gdf.shape)

    with open(paths.data_config / "amenity_groups.json", "r") as f:
        amenity_groups: dict = json.load(f)

    ic(amenity_groups)

    # set up lsoa dataframe
    lsoa_gdf = gpd.read_file(paths.data_lookup / "geography_lookup.csv")[
        ["lsoa_code", "geo_shape"]
    ]
    lsoa_gdf["geometry"] = lsoa_gdf["geo_shape"].map(get_polygon)
    lsoa_gdf = (
        lsoa_gdf[["lsoa_code", "geometry"]]
        .set_geometry("geometry")
        .set_crs(epsg=4326)  # we set the source crs - coords start in lat/long degrees
        .to_crs(epsg=27700)  # Then convert to metric coords
    )
    lsoa_gdf = lsoa_gdf.assign(
        **{
            f"geom_{distance}": lsoa_gdf.geometry.buffer(distance)
            for distance in [0, 250, 500, 750, 1000, 1250, 1500, 2000, 2500, 5000]
        }
    ).set_index(  # create geometries of lsoas extended by different distances in meters
        "lsoa_code"
    )

    ic(lsoa_gdf.head(), lsoa_gdf.geometry)

    # for group_name, group in amenity_groups.items():
    #     for buffer_distance in [0, 250, 500, 750, 1000, 1250, 1500, 2000, 2500, 5000]:
    #         col_name = f"count_{group_name}_{buffer_distance}"
    #         result = count_ammenities(
    #             feature_frame=lsoa_gdf.reset_index(),
    #             point_osm_data=osm_points_gdf,
    #             ammenities=group,
    #             distance=buffer_distance,
    #         )
    #         lsoa_gdf[col_name] = result.reindex(lsoa_gdf.index).fillna(0)

    # nearest_shop = find_nearest_poi(
    #     feature_frame=lsoa_gdf.reset_index(),
    #     point_osm_data=osm_points_gdf,
    #     poi="shop",
    #     distance=0,
    # )
    # lsoa_gdf["nearest_shop_0"] = nearest_shop.reindex(lsoa_gdf.index)

    # ratio_fastfood_dining = calculate_ratio_of_elements(
    #     feature_frame=lsoa_gdf.reset_index(),
    #     point_osm_data=osm_points_gdf,
    #     element_groups=(
    #         amenity_groups.get("fast_food_takeaway", []),
    #         amenity_groups.get("food_dining", []),
    #     ),
    #     distance=1000,
    # )
    # lsoa_gdf["ratio_fastfood_dining_1000"] = ratio_fastfood_dining.reindex(
    #     lsoa_gdf.index
    # )

    # landuse_shares = find_landuse_share(
    #     feature_frame=lsoa_gdf.reset_index(),
    #     polygon_osm_data=osm_polygons_gdf,
    #     distance=0,
    # )
    # for landuse_type in landuse_shares.columns:
    #     col_name = f"landuse_{landuse_type}_0"
    #     lsoa_gdf[col_name] = (
    #         landuse_shares[landuse_type].reindex(lsoa_gdf.index).fillna(0)
    #     )

    # lit_pct = find_streetlit_path_percent(
    #     feature_frame=lsoa_gdf.reset_index(),
    #     line_osm_data=osm_lines_gdf,
    #     distance=0,
    # )
    # lsoa_gdf["lit_path_pct_0"] = lit_pct.reindex(lsoa_gdf.index).fillna(0)

    # # reset index to put lsoa_code back as a column
    # lsoa_gdf = lsoa_gdf.reset_index()


if __name__ == "__main__":
    process()
