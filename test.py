import geopandas
from shapely.geometry import Polygon

polys1 = geopandas.GeoSeries(
    [
        Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
        Polygon([(2, 2), (4, 2), (4, 4), (2, 4)]),
    ]
)


polys2 = geopandas.GeoSeries(
    [
        Polygon([(1, 1), (3, 1), (3, 3), (1, 3)]),
        Polygon([(3, 3), (5, 3), (5, 5), (3, 5)]),
    ]
)


df1 = geopandas.GeoDataFrame({"geometry": polys1, "df1": [1, 2]})

df2 = geopandas.GeoDataFrame({"geometry": polys2, "df2": [1, 2]})


print(df1)
print(df2)

res_intersection = df1.overlay(df2, how="intersection")

print(res_intersection)
