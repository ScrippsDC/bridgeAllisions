import geopandas,pandas
from shapely.geometry import Point, LineString, Polygon

LOAD_WATERSEG = "../data/source/IIR/IIR_water_segments.csv"
LOAD_ALLISIONS = "../data/processed/etl_3_IIR_allisions.csv"

# Spreadsheet of manually reviewed allisions. Cases were only included if the "brief" column
# specified that a vessel struck some part of a bridge. Cases where the allided structure was
# not a bridge, or where there was insufficient information in the "brief" column were excluded.
#
LOAD_MANUAL_REVIEW = "../data/manual/manual_bridge_review.csv"

orig_allisions = pandas.read_csv(LOAD_ALLISIONS, encoding="utf-8")
manual_review = pandas.read_csv(LOAD_MANUAL_REVIEW)

vetted_allisions = orig_allisions[orig_allisions["activity_id"].isin(manual_review[manual_review["passed_review"]]["activity_id"])]

vetted_allisions = vetted_allisions.join(manual_review.set_index("activity_id")["ambiguous_facility_cost"],on="activity_id").reset_index()

vetted_allisions.to_csv("../data/processed/etl_4_vetted_allisions.csv")

print(vetted_allisions.shape[0])

waterseg = pandas.read_csv(LOAD_WATERSEG,encoding="utf-8")
vetted_allisions[~(vetted_allisions["activity_id"].isin(waterseg["activity_id"]))].to_csv("../data/processed/etl_4_missing_coordinates.csv")