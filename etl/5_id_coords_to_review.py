import pandas, geopandas
LOAD_RR_BRIDGES = "../data/source/BTS/Railroad_Bridges.geojson"
LOAD_HWY_BRIDGES = "../data/source/NBI/Selected_Bridges_June_10_2024_11_47_22.txt"
LOAD_VETTED_ALLISIONS = '../data/processed/etl_4_vetted_allisions.csv'
LOAD_WATERSEG = "../data/source/IIR/IIR_water_segments.csv"
ALBERS_CONUS = 5070
MAIN_CRS = 4326
BUFFER_RADIUS = 1000


vet_allisions = pandas.read_csv(LOAD_VETTED_ALLISIONS)
waterseg = pandas.read_csv(LOAD_WATERSEG)

allision_waterseg = waterseg[waterseg["activity_id"].isin(vet_allisions["activity_id"])]
allision_waterseg_geo = geopandas.GeoDataFrame(allision_waterseg, geometry=geopandas.points_from_xy(allision_waterseg["long"], allision_waterseg["lat"]), crs=MAIN_CRS)

rr_bridges = geopandas.read_file(LOAD_RR_BRIDGES) #EPSG:4326
hwy_bridges = pandas.read_csv(LOAD_HWY_BRIDGES,quotechar="'")
hwy_bridges_geo = geopandas.GeoDataFrame(hwy_bridges, geometry=geopandas.points_from_xy(hwy_bridges["17 - Longitude (decimal)"], hwy_bridges["16 - Latitude (decimal)"]),crs=MAIN_CRS)
master_bridge_geo = geopandas.GeoDataFrame(pandas.concat([rr_bridges,hwy_bridges_geo],ignore_index=True), crs=MAIN_CRS)

# Finding incidents where all coordinates are outside 1KM of the points in master_bridge_geo
allision_waterseg_reproj = allision_waterseg_geo.to_crs(ALBERS_CONUS)
allision_waterseg_reproj["geometry"] = allision_waterseg_reproj.buffer(BUFFER_RADIUS)
master_bridge_reproj = master_bridge_geo.to_crs(ALBERS_CONUS)

allisions_1km_bridges = allision_waterseg_reproj.sjoin(master_bridge_reproj, how="inner", predicate="intersects")
not_approx_allisions = allision_waterseg_geo[~(allision_waterseg_geo["description"].str.contains("approx",case=False))]["activity_id"].to_list()
no_1km_or_approx_bridge_allisions = allision_waterseg_geo[~(allision_waterseg_geo["activity_id"].isin(allisions_1km_bridges["activity_id"]))|~(allision_waterseg_geo["activity_id"].isin(not_approx_allisions))]

no_1km_or_approx_bridge_allisions.to_crs(MAIN_CRS).join(vet_allisions.set_index("activity_id")[["title","brief","facility_name"]], on="activity_id").to_csv("../data/processed/etl_5_no_1km_or_approx_bridge_allisions.csv",index=False)