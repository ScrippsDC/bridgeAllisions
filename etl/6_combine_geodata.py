import pandas, geopandas
from shapely.geometry import Point, LineString, Polygon

LOAD_FILLED_COORDS = "../data/manual/filled_missing_coords.csv"
LOAD_CORRECTED_COORDS = "../data/manual/corr_coords.csv"
LOAD_WATERSEG = "../data/source/IIR/IIR_water_segments.csv"
LOAD_VET_ALLISIONS = "../data/processed/etl_4_vetted_allisions.csv"
MAIN_CRS = 4326
COORD_DECIMALS = 5

LOAD_STATE = "../data/source/census/tl_2023_us_state.zip"
LOAD_COUNTY = "../data/source/census/tl_2023_us_county.zip"

EDIT_COORDINATES = {7496626:{'lat': 38.956243, 'long': -74.874100}}

def get_centroid(p_ls):
    if len(p_ls)==2:
        line_s = LineString(p_ls)
        return line_s.centroid
    polygon = Polygon(p_ls)
    return polygon.centroid


def geosum_points(point_ls):
    n_points = len(point_ls)
    if n_points==1:
        return point_ls[0]
    centroid = get_centroid(point_ls)
    return centroid

def correct_coords(activity_id):
    if activity_id in corrected_coords["activity_id"].to_list():
        return corrected_coords[corrected_coords["activity_id"]==activity_id][["lat","long"]].iloc[0].to_list()
    return None

vetted_allisions = pandas.read_csv(LOAD_VET_ALLISIONS, encoding = "utf-8")

# adding in coordinates missing from the CGMIX data
filled_coords = pandas.read_csv(LOAD_FILLED_COORDS,encoding="utf-8")
waterseg = pandas.read_csv(LOAD_WATERSEG,encoding="utf-8")
master_waterseg = pandas.concat([waterseg,filled_coords]).reset_index(drop=True)

# writing over coordinates that were corrected (reviewed for either being approximate, or far from bridges)
corrected_coords = pandas.read_csv(LOAD_CORRECTED_COORDS,encoding="utf-8")
master_waterseg["lat"] = master_waterseg.apply(lambda x: correct_coords(x["activity_id"])[0] if correct_coords(x["activity_id"]) else x["lat"],axis=1)
master_waterseg["long"] = master_waterseg.apply(lambda x: correct_coords(x["activity_id"])[1] if correct_coords(x["activity_id"]) else x["long"],axis=1)


# Flattening allisions with mutliple coordinates associated with them into a single point
master_geo_waterseg = geopandas.GeoDataFrame(master_waterseg,geometry=geopandas.points_from_xy(master_waterseg.long, master_waterseg.lat), crs="EPSG:4326")
master_geosum_activity = master_geo_waterseg.groupby("activity_id")[["geometry","description","waterway_name"]].agg(list)
master_geosum_activity["summary_point"] = master_geosum_activity["geometry"].apply(geosum_points)
master_geosum_activity["lat"] = master_geosum_activity["summary_point"].apply(lambda x: round(x.y,COORD_DECIMALS) if x else None)
master_geosum_activity["long"] = master_geosum_activity["summary_point"].apply(lambda x: round(x.x,COORD_DECIMALS) if x else None)

# Adding in the rest of our allision data
allisions_w_coords = vetted_allisions.join(master_geosum_activity,on = "activity_id")
allisions_w_coords = geopandas.GeoDataFrame(allisions_w_coords,geometry="summary_point", crs="EPSG:4326")

# Adding in state and county info
state_boundaries = geopandas.read_file(LOAD_STATE).to_crs(MAIN_CRS)
county_boundaries = geopandas.read_file(LOAD_COUNTY).to_crs(MAIN_CRS)
allisions_w_coords = allisions_w_coords.sjoin(state_boundaries[["STUSPS","geometry"]],how="left",predicate="intersects").drop("index_right",axis=1).sjoin(county_boundaries[["GEOID","NAMELSAD","geometry"]],how="left",predicate="intersects").drop("index_right",axis=1)
allisions_w_coords["county_state"] = allisions_w_coords.apply(lambda x: str(x["NAMELSAD"])+";"+str(x["STUSPS"]),axis=1)


allisions_w_coords.to_csv("../data/processed/etl_6_allisions_w_coords.csv",index=False,encoding="utf-8")


# Two different tables for buildling web vs television gfx
for_web_map = allisions_w_coords
for_web_map["latlong_str"] = for_web_map.apply(lambda x: f"{x['lat']},{x['long']}",axis=1)
for_web_map[["brief","start_dt","facility_name","latlong_str","NAMELSAD","STUSPS","vessel_name","waterway_name","activity_id","lat","long"]].to_csv("../data/export/etl_6_allisions_for_map.csv",index=False,encoding="utf-8")

for_gfx = for_web_map[["facility_damage","summary_point","lat","long"]]
for_gfx["highlight_in_map2"] = for_gfx["facility_damage"].apply(lambda x: True if x>0 else False)
for_gfx.to_csv("../data/export/etl_6_for_gfx.csv",index=False,encoding="utf-8")
