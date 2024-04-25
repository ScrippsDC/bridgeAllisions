import bs4,pandas,os,random,time,requests

SOAP_URL = "https://cgmix.uscg.mil/xml/PSIXData.asmx"
SOAP_HEADERS = {
    'Content-Type': 'text/xml; charset=utf-8',
}

LOAD_VESSELS = "data/source/IIR/IIR_vessels.csv"

DEFICIENCIES_SAVE_FILE = "data/source/PSIX/PSIX_deficiencies.json"
TONNAGE_SAVE_FILE = "data/source/PSIX/PSIX_tons.csv"
PARTICULARS_SAVE_FILE = "data/source/PSIX/PSIX_particulars.csv"

def find_item_in_soup(soup,item):
    return soup.find(item).text if soup.find(item) else None

def limit_to_unprocessed_records(df,savefile,key):
    if os.path.exists(savefile):
        current_savefile = pandas.read_csv(savefile)
        print("primary_vessel_id" in df.columns)
        return df[~df[key].isin(current_savefile[key])]
    else:
        return df

def save_csv(activity_id,savefile,fn):
    time.sleep(random.gauss(1,.1))
    try: 
      summary = fn(activity_id)
      df = pandas.DataFrame(summary)
      if os.path.exists(savefile):
          df.to_csv(savefile,mode="a",header=False,index=False)
      else:
          df.to_csv(savefile,mode="w",header=True,index=False)
    except:
      print(f"Failed to get data for activity {activity_id}")

def construct_deficiencies_payload(activity_id):
    return f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
    <soap:Body>
        <getVesselDeficiencies xmlns="https://cgmix.uscg.mil">
        <ActivityNumber>{activity_id}</ActivityNumber>
        </getVesselDeficiencies>
    </soap:Body>
    </soap:Envelope>"""

def construct_tonnage_payload(vessel_id):
    return f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
    <soap:Body>
        <getVesselTonnage xmlns="https://cgmix.uscg.mil">
        <VesselID>{vessel_id}</VesselID>
        </getVesselTonnage>
    </soap:Body>
    </soap:Envelope>"""

def construct_particulars_payload(vessel_id):
    return f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
    <soap:Body>
        <getVesselParticulars xmlns="https://cgmix.uscg.mil">
        <VesselID>{vessel_id}</VesselID>
        </getVesselParticulars>
    </soap:Body>
    </soap:Envelope>"""

def get_tonnage(vessel_id):
    payload = construct_tonnage_payload(vessel_id)
    response = requests.request("POST", SOAP_URL, data=payload,headers=SOAP_HEADERS)
    soup = bs4.BeautifulSoup(response.text,features="xml")
    tons = []
    for tonnage in soup.findAll("VesselTonnage"):
        measure_of_weight = find_item_in_soup(tonnage, "MeasureOfWeight")
        tonnage_type_id = find_item_in_soup(tonnage,"TonnageTypeLookupId")
        tonnage_type_name = find_item_in_soup(tonnage,"TonnageTypeLookupName")
        unit_of_measure_id = find_item_in_soup(tonnage,"TonnageUnitOfMeasurementFilterLookupId")
        unit_of_measure_name = find_item_in_soup(tonnage,"UnitOfMeasureLookupName")
        vessel_id = vessel_id
        tons.append({
            "measure_of_weight":measure_of_weight,
            "tonnage_type_id":tonnage_type_id,
            "tonnage_type_name":tonnage_type_name,
            "unit_of_measure_id":unit_of_measure_id,
            "unit_of_measure_name":unit_of_measure_name,
            "primary_vessel_id":vessel_id
        })
    return tons

def get_particulars(vessel_id):
    payload = construct_particulars_payload(vessel_id)
    response = requests.request("POST", SOAP_URL, data=payload,headers=SOAP_HEADERS)
    soup = bs4.BeautifulSoup(response.text,features="xml")
    vessel_particulars = soup.find_all("VesselParticulars")
    particulars = []
    for v in vessel_particulars:
        name = find_item_in_soup(v,"VesselName")
        call_sign = find_item_in_soup(v,"VesselCallSign")
        service_type = find_item_in_soup(v,"ServiceType")
        service_sub = find_item_in_soup(v,"ServiceSubType")
        status_lookup = find_item_in_soup(v,"StatusLookupName")
        construct_complete_year = find_item_in_soup(v,"ConstructionCompletedYear")
        out_of_service_date = find_item_in_soup(v,"OutOfServiceDate")
        identification = find_item_in_soup(v,"Identification")
        id_type_id = find_item_in_soup(v,"IdentificationTypeLookupId")
        id_type_name = find_item_in_soup(v,"IdentificationTypeLookupName")
        country = find_item_in_soup(v,"CountryLookupName")
        country_id = find_item_in_soup(v,"CountryLookupId")
        particulars.append({"primary_vessel_id":vessel_id,
                            "vessel_name":name,
                            "call_sign":call_sign,
                            "service_type":service_type,
                            "service_sub":service_sub,
                            "status_lookup":status_lookup,
                            "construct_complete_year":construct_complete_year,
                            "out_of_service_date":out_of_service_date,
                            "identification":identification,
                            "id_type_id":id_type_id,
                            "id_type_name":id_type_name,
                            "country":country,
                            "country_id":country_id})
    return particulars
    
# def get_deficiencies(activity_id):
#     payload = construct_tonnage_payload(activity_id)
#     response = requests.request("POST", SOAP_URL, data=payload,headers=SOAP_HEADERS)
#     soup = bs4.BeautifulSoup(response.text,features="xml")

vessels = pandas.read_csv(LOAD_VESSELS)
#tonnage_vessels_to_process = limit_to_unprocessed_records(vessels,TONNAGE_SAVE_FILE,"primary_vessel_id")
#tonnage_vessels_to_process["primary_vessel_id"].apply(save_csv,args=(TONNAGE_SAVE_FILE,get_tonnage))

particulars_vessels_to_process = limit_to_unprocessed_records(vessels,PARTICULARS_SAVE_FILE,"primary_vessel_id")
particulars_vessels_to_process["primary_vessel_id"].apply(save_csv,args=(PARTICULARS_SAVE_FILE,get_particulars))