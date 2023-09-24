import dask_geopandas as dgpd
import geopandas as gpd
import pandas as pd
import shapely.geometry

df = pd.read_csv(
    "https://files.codeocean.com/files/verified/"
    "fa908bbc-11f9-4421-8bd3-72a4bf00427f_v2.0/data/int/applications/population/"
    "outcomes_sampled_population_CONTUS_16_640_UAR_100000_0.csv?download",
    index_col=0,
    na_values=["-999"],
)
gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat))
ddf = dgpd.from_geopandas(gdf, npartitions=1)
dgdf = dgpd.from_geopandas(
    gdf.assign(hd=ddf.hilbert_distance().compute()).sort_values("hd"),
    npartitions=250,
    sort=False,
)
dgdf.map_partitions(
    lambda gdf: shapely.geometry.mapping(gdf.unary_union.convex_hull)
).compute().to_json("hulls.json", orient="records", indent=4)
