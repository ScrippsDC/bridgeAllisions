import bs4,pandas,os,random,time,requests
SOAP_URL = "https://cgmix.uscg.mil/xml/IIRData.asmx"
SOAP_HEADERS = {
    'Content-Type': 'text/xml; charset=utf-8',
}

CASUALTIES_SAVE_FILE = "data/source/IIR/IIR_casualties.json"
WATERSEG_SAVE_FILE = "data/source/IIR/IIR_water_segments.csv"
BRIEF_SAVE_FILE = "data/source/IIR/IIR_briefs.csv"
DAMAGES_SAVE_FILE = "data/source/IIR/IIR_damages.csv"
SUMMARY_SAVE_FILE = "data/source/IIR/IIR_summary.csv"
FACILITIES_SAVE_FILE = "data/source/IIR/IIR_facilities.csv"
VESSELS_SAVE_FILE = "data/source/IIR/IIR_vessels.csv"
ORGS_SAVE_FILE = "data/source/IIR/IIR_orgs.csv"

def find_item_in_soup(soup,item):
    return soup.find(item).text if soup.find(item) else None

def limit_to_unprocessed_records(df,savefile):
    if os.path.exists(savefile):
        current_savefile = pandas.read_csv(savefile)
        return df[~df["activity_id"].isin(current_savefile["activity_id"])]
    else:
        return df

def save_csv(activity_id,savefile,fn):
    time.sleep(random.gauss(1,.1))
    try: 
        data = fn(activity_id)
        if type(data) == list:
            df = pandas.DataFrame(data)
        else:
            df = pandas.DataFrame([data])
        if os.path.exists(savefile):
          df.to_csv(savefile,mode="a",header=False,index=False)
        else:
          df.to_csv(savefile,mode="w",header=True,index=False)
    except:
      print(f"Failed to get data for activity {activity_id}")

def save_json(activity_id,savefile,fn):
    time.sleep(random.gauss(1,.1))
    try: 
      summary = fn(activity_id)
      df = pandas.DataFrame([summary])
      if os.path.exists(savefile):
          df.to_json(savefile,mode="a",lines=True,orient="records")
      else:
          df.to_json(savefile,mode="w",lines=True,orient="records")
    except:
      print(f"Failed to get data for activity {activity_id}")

def construct_water_segment_payload(activity_id):
    return f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
        <soap:Body>
            <getIIRWaterSegments xmlns="https://cgmix.uscg.mil/xml/">
            <ActivityId>{activity_id}</ActivityId>
            </getIIRWaterSegments>
        </soap:Body>
        </soap:Envelope>"""

def construct_brief_payload(activity_id):
    return f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <getIIRIncidentBrief xmlns="https://cgmix.uscg.mil/xml/">
          <ActivityId>{activity_id}</ActivityId>
        </getIIRIncidentBrief>
      </soap:Body>
    </soap:Envelope>"""

def construct_damages_payload(activity_id):
    return f"""<?xml version="1.0" encoding="utf-8"?>
            <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
                <getIIRVesselDamageSummary xmlns="https://cgmix.uscg.mil/xml/">
                <ActivityId>{activity_id}</ActivityId>
                </getIIRVesselDamageSummary>
            </soap:Body>
            </soap:Envelope>"""

def construct_casualty_payload(activity_id):
    return f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
        <soap:Body>
            <getIIRPersonalCasualtySummary xmlns="https://cgmix.uscg.mil/xml/">
            <ActivityId>{str(activity_id)}</ActivityId>
            </getIIRPersonalCasualtySummary>
        </soap:Body>
        </soap:Envelope>"""

def construct_summary_payload(activity_id):
    return f"""<?xml version="1.0" encoding="utf-8"?>
            <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
              <soap:Body>
                <getIIRIncidentSummary xmlns="https://cgmix.uscg.mil/xml/">
                  <ActivityId>{activity_id}</ActivityId>
                </getIIRIncidentSummary>
              </soap:Body>
            </soap:Envelope>"""

def construct_facilities_payload(activity_id):
    return f"""<?xml version="1.0" encoding="utf-8"?>
            <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
                <getIIRInvolvedFacilities xmlns="https://cgmix.uscg.mil/xml/">
                <ActivityId>{activity_id}</ActivityId>
                </getIIRInvolvedFacilities>
            </soap:Body>
            </soap:Envelope>"""

def construct_vessel_payload(activity_id):
    return f"""<?xml version="1.0" encoding="utf-8"?>
            <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
                <getIIRInvolvedVessels xmlns="https://cgmix.uscg.mil/xml/">
                <ActivityId>{activity_id}</ActivityId>
                </getIIRInvolvedVessels>
            </soap:Body>
            </soap:Envelope>"""

def construct_orgs_payload(activity_id):
    return f"""<?xml version="1.0" encoding="utf-8"?>
            <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
                <getIIRInvolvedOrganizations xmlns="https://cgmix.uscg.mil/xml/">
                <ActivityId>{activity_id}</ActivityId>
                </getIIRInvolvedOrganizations>
            </soap:Body>
            </soap:Envelope>"""


def get_orgs(activity_id):
    payload = construct_orgs_payload(activity_id)
    response = requests.request("POST",SOAP_URL,data=payload,headers=SOAP_HEADERS)
    soup = bs4.BeautifulSoup(response.text,features="xml")
    orgs = []
    for org in soup.find_all("IIRInvolvedOrganizations"):
        name = find_item_in_soup(org,"Name")
        addr = find_item_in_soup(org,"Address")
        addr_type = find_item_in_soup(org,"AddressTypeLookupName")
        party_role = find_item_in_soup(org,"PartyRoleLookupName")
        org_dict = {"acitivity_id":activity_id,
                "name":name,
                "addr":addr,
                "addr_type":addr_type,
                "party_role":party_role}
        orgs.append(org_dict)
    return orgs

def get_brief(activity_id):
    payload = construct_brief_payload(activity_id)
    response = requests.request("POST", SOAP_URL, data=payload,headers=SOAP_HEADERS)
    brief = bs4.BeautifulSoup(response.text,features="xml").find("IncidentBrief").text
    return {"activity_id":activity_id,"brief":brief}

def get_waterseg(activity_id):
    payload = construct_water_segment_payload(activity_id)
    response = requests.request("POST", SOAP_URL, data=payload,headers=SOAP_HEADERS)
    soup = bs4.BeautifulSoup(response.text,features="xml")
    segments = []
    for segment in soup.find_all("IIRWaterSegments"):
        waterway_role = find_item_in_soup(segment,"WaterwayRoleLookupName")
        description = find_item_in_soup(segment,"Description")
        lat = find_item_in_soup(segment,"Latitude")
        long = find_item_in_soup(segment,"Longitude")
        waterway_name = find_item_in_soup(segment,"WaterwayName")
        seg_dict = {"activity_id":activity_id,
            "waterway_role":waterway_role,
            "description":description,
            "lat":lat,
            "long":long,
            "waterway_name":waterway_name}
        segments.append(seg_dict)
    return segments

def get_damages(activity_id):
    payload = construct_damages_payload(activity_id)
    response = requests.request("POST", SOAP_URL, data=payload,headers=SOAP_HEADERS)
    soup = bs4.BeautifulSoup(response.text,features="xml")
    cargo_damage = find_item_in_soup(soup,"CargoDamageInDollars")
    facility_damage = find_item_in_soup(soup,"FacilityDamageInDollars")
    vessel_damage = find_item_in_soup(soup,"VesselDamageInDollars")
    other_damage = find_item_in_soup(soup,"OtherDamageInDollars")
    return {"activity_id":activity_id,
            "cargo_damage":cargo_damage,
            "facility_damage":facility_damage,
            "vessel_damage":vessel_damage,
            "other_damage":other_damage}

def get_casualties(activity_id):
    payload = construct_casualty_payload(activity_id)
    response = requests.request("POST", SOAP_URL, data=payload,headers=SOAP_HEADERS)
    soup = bs4.BeautifulSoup(response.text,features="xml")
    casualty_categories = soup.find_all("IIRPersonalCasualtySummary")
    return_dict = {"activity_id":activity_id}
    for cat in casualty_categories:
        category_name = find_item_in_soup(cat,"CasualtyStatusLookupName")
        category_count = find_item_in_soup(cat,"TotalPeopleAtRisk")
        return_dict[category_name] = category_count
    return return_dict      

def get_summary(activity_id):
    payload = construct_summary_payload(activity_id)
    response = requests.request("POST", SOAP_URL, data=payload,headers=SOAP_HEADERS)
    soup = bs4.BeautifulSoup(response.text,features="xml")
    serious_marine_incident = find_item_in_soup(soup,"IsSeriousMarineIncident")
    marine_board_convened = find_item_in_soup(soup,"IsMarineBoardConvened")
    invest_level = find_item_in_soup(soup,"LevelofInvestigationLookupName")
    casualty_classification = find_item_in_soup(soup,"UnitedStatesMarineCasualtyClassificationLookupName")
    incident_involvement = find_item_in_soup(soup,"IncidentInvolvesLookupName")
    invest_type = find_item_in_soup(soup,"TypeLookupName")
    return {"activity_id":activity_id,
            "serious_marine_incident":serious_marine_incident,
            "marine_board_convened":marine_board_convened,
            "invest_level":invest_level,
            "casualty_classification":casualty_classification,
            "incident_involvement":incident_involvement,
            "invest_type":invest_type}

def get_facilities(activity_id):
    payload = construct_facilities_payload(activity_id)
    response = requests.request("POST", SOAP_URL, data=payload,headers=SOAP_HEADERS)
    soup = bs4.BeautifulSoup(response.text,features="xml")
    facilities = []
    for facility in soup.find_all("IIRInvolvedFacilities"):
        facility_name = find_item_in_soup(facility,"Name")
        facility_type = find_item_in_soup(facility,"TypeLookupName")
        facility_dict = {"activity_id":activity_id,
            "facility_name":facility_name,
            "facility_type":facility_type}
        facilities.append(facility_dict)
    return facilities  

def get_vessels(activity_id):
    payload = construct_vessel_payload(activity_id)
    response = requests.request("POST", SOAP_URL, data=payload,headers=SOAP_HEADERS)
    soup = bs4.BeautifulSoup(response.text,features="xml")
    vessels = []
    for vessel in soup.find_all("IIRInvolvedVessels"):
        misle_vessel_id = find_item_in_soup(vessel,"MISLEVesselId")
        vessel_name = find_item_in_soup(vessel,"Name")
        primary_vessel_id = find_item_in_soup(vessel,"PrimaryVesselIdentificationNumber")
        vessel_role = find_item_in_soup(vessel,"VesselRoleLookupName")
        vessel_dict = {"activity_id":activity_id,
            "vessel_name":str(vessel_name).strip(),
            "misle_vessel_id":misle_vessel_id,
            "primary_vessel_id":primary_vessel_id,
            "vessel_role":vessel_role}
        vessels.append(vessel_dict)
    return vessels

activities = pandas.read_csv("data/source/IIR/IIR_search.csv")

waterseg_activities_to_process = limit_to_unprocessed_records(activities,WATERSEG_SAVE_FILE)
waterseg_activities_to_process["activity_id"].apply(save_csv,args=(WATERSEG_SAVE_FILE,get_waterseg))
# damages_activities_to_process = limit_to_unprocessed_records(activities,DAMAGES_SAVE_FILE)
# damages_activities_to_process["activity_id"].apply(save_csv,args=(DAMAGES_SAVE_FILE,get_damages))
# casualty_activities_to_process = limit_to_unprocessed_records(activities,CASUALTIES_SAVE_FILE)
# casualty_activities_to_process["activity_id"].apply(save_json,args=(CASUALTIES_SAVE_FILE,get_casualties))
# brief_activities_to_process = limit_to_unprocessed_records(activities,BRIEF_SAVE_FILE)
# brief_activities_to_process["activity_id"].apply(save_json,args=(BRIEF_SAVE_FILE,get_brief))
# summary_activities_to_process = limit_to_unprocessed_records(activities,SUMMARY_SAVE_FILE)
# summary_activities_to_process["activity_id"].apply(save_csv,args=(SUMMARY_SAVE_FILE,get_summary))

#facilities_activities_to_process = limit_to_unprocessed_records(activities,FACILITIES_SAVE_FILE)
#facilities_activities_to_process["activity_id"].apply(save_csv,args=(FACILITIES_SAVE_FILE,get_facilities))
#vessels_activities_to_process = limit_to_unprocessed_records(activities,VESSELS_SAVE_FILE)
#vessels_activities_to_process["activity_id"].apply(save_csv,args=(VESSELS_SAVE_FILE,get_vessels))
orgs_to_process = limit_to_unprocessed_records(activities,ORGS_SAVE_FILE)
orgs_to_process["activity_id"].apply(save_csv,args=(ORGS_SAVE_FILE,get_orgs))