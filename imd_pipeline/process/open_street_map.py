import json
from functools import partial
from typing import Iterable

import geopandas as gpd
import pandas as pd
import polars as pl
from geopandas import GeoDataFrame
from loguru import logger
from project_paths import paths
from shapely import LineString, Point, Polygon

BUFFER_DISTANCES = [
    0,
    250,
    500,
    750,
    1000,
    1250,
    1500,
    2000,
    2500,
    5000,
]

SELECTED_NEAREST_POI = [
    'hospital', 'pharmacy', 'school', 'kindergarten',
    'college', 'university', 'bank', 'atm', 'ice_cream',
    'fast_food', 'pub', 'bar', 'nightclub', 'stripclub',
    'gambling', 'bicycle_parking', 'cinema', 'theatre',
    'social_facility'
]

def count_ammenities(
    feature_frame: GeoDataFrame,
    point_osm_data: GeoDataFrame,
    ammenities: Iterable,
    distance: int,
) -> pd.Series:

    _ammenities = {
        ammenity for ammenity in ammenities
    }  # convert to set to use membership methods

    mask = point_osm_data["tags"].apply(
        lambda x: any(
            x.get(key) in _ammenities
            for key in ("amenity", "shop", "landuse", "highway")
        )
    )
    point_osm_data_filtered = point_osm_data[mask]

    lsoa_gdf = feature_frame[["lsoa_code", f"geom_{distance}"]]
    lsoa_gdf.set_geometry(f"geom_{distance}", inplace=True)

    joined_gdf = point_osm_data_filtered.sjoin(
        lsoa_gdf, how="inner", predicate="within"
    )
    gdf_agg = joined_gdf[["lsoa_code", "id"]].groupby(["lsoa_code"]).count()

    return gdf_agg["id"]


def matches_poi(
    tags: dict,
    poi: str,
) -> bool:
    category_keys = {"amenity", "shop", "landuse", "highway"}
    for key in category_keys:
        if tags.get(key) == poi:
            return True
    return poi in tags.keys()


def matches_any_poi(tags: dict, pois: set) -> bool:
    category_keys = {"amenity", "shop", "landuse", "highway"}
    for key in category_keys:
        if tags.get(key) in pois:
            return True
    return not pois.isdisjoint(tags.keys())


def find_nearest_poi(
    feature_frame: GeoDataFrame,
    point_osm_data: GeoDataFrame,
    poi: str,
    distance: int,
) -> pd.Series:
    lsoa_gdf = feature_frame[["lsoa_code", f"geom_{distance}"]]
    lsoa_gdf.set_geometry(f"geom_{distance}", inplace=True)

    _matches_poi = partial(matches_poi, poi=poi)

    filtered_points_gdf = point_osm_data[point_osm_data["tags"].apply(_matches_poi)]
    joined_gdf = lsoa_gdf.sjoin_nearest(
        right=filtered_points_gdf, how="inner", distance_col="distance"
    )

    agged_gdf = joined_gdf[["lsoa_code", "distance"]].groupby(["lsoa_code"]).min()

    return agged_gdf["distance"].rename(f"nearest_{poi}")


def calculate_ratio_of_elements(
    feature_frame: GeoDataFrame,
    point_osm_data: GeoDataFrame,
    element_groups: tuple[Iterable, Iterable],
    distance: int,
    name: str | None = None,
) -> pd.Series:
    _matches_any_group_a = partial(matches_any_poi, pois=set(element_groups[0]))
    _matches_any_group_b = partial(matches_any_poi, pois=set(element_groups[1]))

    lsoa_gdf = feature_frame[["lsoa_code", f"geom_{distance}"]]
    lsoa_gdf.set_geometry(f"geom_{distance}", inplace=True)

    joined_gdf = point_osm_data.sjoin(lsoa_gdf, how="inner", predicate="within")

    joined_gdf["is_group_a"] = joined_gdf["tags"].apply(_matches_any_group_a)
    joined_gdf["is_group_b"] = joined_gdf["tags"].apply(_matches_any_group_b)

    counts = joined_gdf.groupby("lsoa_code").agg(
        count_a=("is_group_a", "sum"),
        count_b=("is_group_b", "sum"),
    )

    counts["ratio"] = counts["count_a"] / counts["count_b"].replace(0, float("nan"))

    if name:
        return counts["ratio"].rename(name)
    else:
        return counts["ratio"].rename(
            f"ratio_{'-'.join(element_groups[0])}_to_{'-'.join(element_groups[1])}_{distance}"
        )


def find_landuse_share(
    feature_frame: GeoDataFrame,
    polygon_osm_data: GeoDataFrame,
    distance: int,
) -> pd.DataFrame:
    lsoa_gdf = feature_frame[["lsoa_code", f"geom_{distance}"]].copy()
    lsoa_gdf.set_geometry(f"geom_{distance}", inplace=True)
    lsoa_areas = lsoa_gdf.set_index("lsoa_code").geometry.area
    lsoa_areas.rename("lsoa_area", inplace=True)
    lsoa_areas = lsoa_areas.to_frame().reset_index()

    landuse_gdf = polygon_osm_data[
        polygon_osm_data["tags"].apply(lambda x: "landuse" in x.keys())
    ].copy()
    landuse_gdf["landuse_type"] = landuse_gdf["tags"].apply(lambda x: x.get("landuse"))

    intersections_df = lsoa_gdf.overlay(right=landuse_gdf, how="intersection")
    intersections_df["landuse_area"] = intersections_df.geometry.area

    intersections_df = (
        intersections_df[["lsoa_code", "landuse_type", "landuse_area"]]
        .groupby(["lsoa_code", "landuse_type"])
        .sum()
    ).reset_index()

    intersections_df = intersections_df.merge(
        right=lsoa_areas, how="left", left_on="lsoa_code", right_on="lsoa_code"
    )

    intersections_df["landuse_share"] = (
        intersections_df["landuse_area"] / intersections_df["lsoa_area"]
    )

    landuse_share_df = (
        intersections_df[["lsoa_code", "landuse_type", "landuse_share"]]
        .pivot(columns="landuse_type", index="lsoa_code", values="landuse_share")
        .fillna(0)
    )

    return landuse_share_df


def find_streetlit_path_percent(
    feature_frame: GeoDataFrame,
    line_osm_data: GeoDataFrame,
    distance: int,
) -> pd.Series:
    lsoa_gdf = feature_frame[["lsoa_code", f"geom_{distance}"]].copy()
    lsoa_gdf.set_geometry(f"geom_{distance}", inplace=True)

    joined_gdf = line_osm_data.sjoin(lsoa_gdf, how="inner", predicate="intersects")

    joined_gdf["clipped_length"] = joined_gdf.apply(
        lambda row: (
            row.geometry.intersection(
                lsoa_gdf.loc[
                    lsoa_gdf["lsoa_code"] == row["lsoa_code"], f"geom_{distance}"
                ].iloc[0]
            ).length
        ),
        axis=1,
    )

    joined_gdf["is_lit"] = joined_gdf["tags"].apply(lambda x: x.get("lit") == "yes")

    total_length = joined_gdf.groupby("lsoa_code")["clipped_length"].sum()
    lit_length = (
        joined_gdf[joined_gdf["is_lit"]].groupby("lsoa_code")["clipped_length"].sum()
    )

    lit_percent = (lit_length / total_length).fillna(0)

    return lit_percent.rename("streetlit_percentage")


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

    logger.info("setting up open street map processing...")

    response_file = paths.data_raw / "osm" / "overpass_response.json"
    with open(response_file) as file:
        data = json.load(file)
    map_elements = data.get("elements")

    osm_points_gdf, osm_polygons_gdf, osm_lines_gdf = format_osm_geodataframes(
        map_elements=map_elements
    )

    with open(paths.data_config / "amenity_groups.json", "r") as f:
        amenity_groups: dict = json.load(f)

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
            for distance in BUFFER_DISTANCES
        }
    ).set_index(  # create geometries of lsoas extended by different distances in meters
        "lsoa_code"
    )

    logger.debug(
        "set up for open street map processing complete",
        len_map_elements=len(map_elements),
        lsoa_frame=lsoa_gdf.shape,
        point_frame=osm_points_gdf.shape,
        poly_frame=osm_polygons_gdf.shape,
        lines_frame=osm_lines_gdf.shape,
        ammenity_groups=len(amenity_groups),
    )

    logger.debug("extracting feature columns from osm data (could take a while)")

    count_ammenities_df = pd.DataFrame(
        {
            f"count_{group_name}_{buffer_distance}": count_ammenities(
                feature_frame=lsoa_gdf.reset_index(),
                point_osm_data=osm_points_gdf,
                ammenities=group,
                distance=buffer_distance,
            )
            .reindex(lsoa_gdf.index)
            .fillna(0)
            for group_name, group in amenity_groups.items()
            for buffer_distance in BUFFER_DISTANCES
        },
        index=lsoa_gdf.index,
    )

    nearest_poi_df = pd.DataFrame(
        {f"nearest_{ammenity}": find_nearest_poi(
            feature_frame=lsoa_gdf.reset_index(),
            point_osm_data=osm_points_gdf,
            poi=f"{ammenity}",
            distance=0,
            )
        for ammenity in SELECTED_NEAREST_POI
        }
    )

    with open(paths.data_config / "ratio_pairs.json", "r") as f:
        ratio_pairs: dict = json.load(f)
    
    ratio_poi_df = pd.DataFrame(
        {f"ratio_{ratio_pairs[item]['numerator']}_to_{ratio_pairs[item]['denominator']}_{distance}":
        calculate_ratio_of_elements(
            feature_frame=lsoa_gdf.reset_index(),
            point_osm_data=osm_points_gdf,
            element_groups=(
                amenity_groups.get(ratio_pairs[item]['numerator'], []),
                amenity_groups.get(ratio_pairs[item]['denominator'], []),
            ),
            distance=distance
        ) for item in ratio_pairs.keys() for distance in [500, 1000, 2000]}
    )

    landuse_shares = find_landuse_share(
        feature_frame=lsoa_gdf.reset_index(),
        polygon_osm_data=osm_polygons_gdf,
        distance=0,
    )

    landuse_df = pd.DataFrame(
        {
            f"landuse_{landuse_type}_0": landuse_shares[landuse_type]
            .reindex(lsoa_gdf.index)
            .fillna(0)
            for landuse_type in landuse_shares.columns
        },
        index=lsoa_gdf.index,
    )

    lit_pct = find_streetlit_path_percent(
        feature_frame=lsoa_gdf.reset_index(),
        line_osm_data=osm_lines_gdf,
        distance=0,
    )

    lsoa_gdf = pd.concat(
        [
            lsoa_gdf,
            count_ammenities_df,
            nearest_poi_df,
            ratio_poi_df,
            landuse_df,
            lit_pct,
        ],
        axis=1,
    )

    # reset index to put lsoa_code back as a column
    lsoa_gdf = lsoa_gdf.reset_index()

    lsoa_gdf.drop(
        columns=[col for col in lsoa_gdf.columns if col.startswith("geom")],
        inplace=True,
    )
    logger.info("osm process complete")

    return pl.from_pandas(lsoa_gdf).lazy()


if __name__ == "__main__":
    process()
