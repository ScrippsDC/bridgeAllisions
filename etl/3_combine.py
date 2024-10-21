import bs4,pandas,math,os,random,time,requests,re

LOAD_FILE = "../data/source/IIR/IIR_search--20240401.csv"
LOAD_CASUALTIES = "../data/source/IIR/IIR_casualties.json"
LOAD_DAMAGES = "../data/source/IIR/IIR_damages.csv"
LOAD_SUMMARY = "../data/source/IIR/IIR_summary.csv"
LOAD_BRIEF = "../data/source/IIR/IIR_brief.json"
LOAD_FACILITIES = "../data/source/IIR/IIR_facilities.csv"
LOAD_VESSELS = "../data/source/IIR/IIR_vessels.csv"

ALLISION_KW = ["allision","allide","alllision","allison","alision"]

activities = pandas.read_csv(LOAD_FILE, encoding="utf-8",parse_dates=["start_dt","end_dt"],dtype={"activity_id":int})
casualties = pandas.read_json(LOAD_CASUALTIES, lines=True, encoding="utf-8")
damages = pandas.read_csv(LOAD_DAMAGES, encoding="utf-8")
summary = pandas.read_csv(LOAD_SUMMARY, encoding="utf-8",index_col=0)
brief = pandas.read_json(LOAD_BRIEF, lines=True, encoding="utf-8")
facilities = pandas.read_csv(LOAD_FACILITIES, encoding="utf-8",dtype={"activity_id":int})
vessels = pandas.read_csv(LOAD_VESSELS, encoding="utf-8")

master_iir = activities.join(casualties.set_index("activity_id"), on="activity_id").join(damages.set_index("activity_id"), on="activity_id").join(summary.set_index("activity_id"), on="activity_id").join(brief.set_index("activity_id"), on="activity_id")

def dict_from_cols(row, cols):
    return_dict = {}
    for col in cols:
        value = row[col]
        if type(value) == str:
            value = value.strip().lower()
        return_dict[col] = value
    return return_dict

def tuple_from_cols(row, cols):
    return tuple([str(row[col]).lower() for col in cols])


vessels["vessel_tuple"] = vessels.apply(lambda x: dict_from_cols(x,["vessel_name","primary_vessel_id"]), axis=1)
vessels_by_activity = vessels.groupby("activity_id")["vessel_tuple"].apply(lambda x: list(x))
vessel_names_by_activity = vessels.groupby("activity_id")["vessel_name"].apply(lambda x: list(x))
vessel_ids_by_activity = vessels.groupby("activity_id")["primary_vessel_id"].apply(lambda x: list(x))

def allision_related(row):
    title = str(row["title"]).lower()
    brief = str(row["brief"]).lower()
    for word in ALLISION_KW:
        if word in title or word in brief:
            return True
    return False


def make_list_str(ls):
    return_str = ""
    if type(ls) == list:
        if len(ls)>0:
            for item in ls:
                return_str += str(item).replace(",",";").lower().strip() + ","
            return return_str[:-1]
    return return_str

facility_names_by_activity = facilities.groupby("activity_id")["facility_name"].apply(list)

master_iir = master_iir.join(facility_names_by_activity,on="activity_id")
master_iir['year'] = pandas.to_datetime(master_iir["start_dt"]).apply(lambda x: x.year)

print("Taking out briefless records, mostly recent incidents.")
print(master_iir.pivot_table(index="year",columns=master_iir["brief"].isna(),values="activity_id",aggfunc="count").rename(columns={True:"no brief",False:"has brief"}).fillna(0).astype(int))


allisions = master_iir[(master_iir.apply(allision_related,axis=1))&(master_iir["year"]>2018)&(master_iir["brief"].notna())]

# Flag cases to review for whether positive "facility_damage" includes bridge damage
allisions["facility_damage_review"] = (allisions["facility_damage"]>0) & ((~(allisions["facility_name"].astype(str).str.contains(r"\bBRIDGE\b",case=False))&~(allisions["facility_name"].astype(str).str.contains(r"\bDRAWBRIDGE\b", case=False))) | (allisions["facility_name"].apply(lambda x: len(x)>1 if type(x)!= float else False)))

allisions.to_csv("../data/processed/etl_3_IIR_allisions.csv", encoding="utf-8", index=False)


