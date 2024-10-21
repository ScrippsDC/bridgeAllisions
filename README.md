# Bridge strikes

This repository contains data and code to reproduce the data findings in [US bridges are frequently struck by ships and barges](https://www.scrippsnews.com/investigations/us-bridges-are-frequently-struck-by-barges-and-vessels) by Patrick Terpstra and Rosie Cima, with contributed reporting and data editing by Amy Fan.

This investigation focuses on the phenomenon of bridge allisions, or when a vessel strikes a bridge -- as in the case of the Francis Scott Key Bridge in Baltimore.

Our findings from the television story are:

> USING RECORDS FROM THE U-S COAST GUARD, OUR INVESTIGATION FOUND A VESSEL HAS DRIFTED INTO PART OF A BRIDGE SOMEWHERE IN THE U-S AT LEAST 650 TIMES SINCE 20-19.

> OUR ANALYSIS DISCOVERED THAT IN OVER A HUNDRED OF THESE CASES, THE IMPACT WAS HARD ENOUGH TO REQUIRE REPAIRS TO THE STRUCTURE. 

> WE FOUND A LOT OF THE BRIDGES GETTING STRUCK AGAIN AND AGAIN ARE OVER THE SAME BODY OF WATER: THE MISSISSIPPI RIVER. 

And from the accompanying web text:

> An analysis of U.S. Coast Guard records of maritime incidents shows a vessel has run into part of a bridge in America more than 650 times since 2019.


## Data sources
Our main data source for this story was the U.S. Coast Guard Maritime Information Exchange database (CGMIX).

### Incident investigation reports

#### IIRSearch
[https://cgmix.uscg.mil/IIR/IIRSearch.aspx](https://cgmix.uscg.mil/IIR/IIRSearch.aspx)

[46 CFR 4.05-1](https://www.law.cornell.edu/cfr/text/46/4.05-1) requires that mariners notify the coast guard in the event of several marine incidents, including "an unintended strike of (allision with) a bridge." The Coast Guard then investigates these incidents and records them in the IIR database.

IIRSearch reaches back to October of 2002. When pulling records for this analysis, we searched for all records that contained the keyword "bridge." This yielded investigation dates, and incident titles, and a unique activity ID number for each incident.

#### IIRData
Once we obtained a list of bridge-related activity ID numbers, we used the [XML IIRData service API](https://cgmix.uscg.mil/XML/IIRData.asmx) to get more details on each event.

This information included:
* A brief description of the incident and investigation findings (getIIRBrief)
* The watersegment(s) where the incident occurred, including coordinates (getIIRWaterSegments)
* Monetary damages to the vessel, facility, cargo, or other (getIIRVesselDamageSummary)
* Facility or facilities involved in the incident (getIIRInvolvedFacilities)
* Vessel(s) involved in the incident (getIIRInvolvedVessels)

We did several integrity checks and filters on this data, as described in the "Manual review" and "Geospatial data" section of our ETL.

### Census shapefiles
For adding state and county code to our coordinates.

### National Bridge Inventory (NBI)
[https://infobridge.fhwa.dot.gov/Data](https://infobridge.fhwa.dot.gov/Data)

For adding highway bridge locations to our data. From the Federal Highway Administration.

### Railroad Bridges
[https://data-usdot.opendata.arcgis.com/datasets/usdot::railroad-bridges-2/about](https://data-usdot.opendata.arcgis.com/datasets/usdot::railroad-bridges-2/about)

For adding railroad bridge locations to our data. From the Bureau of Transportation Statistics.

## ETL
After obtaining incident titles, investigation dates, and activity ID numbers for bridge-related incidents from the IIRSearch portal (we did a keyword search for the word "bridge"), we scraped details using the IIRData API. Those scripts are in [etl/1_iirdata_scrape.py](etl/1_iirdata_scrape.py).

### Combine into one table
*results in data/processed/etl_3_IIR_allisions.csv*

We then combined the data from disparate endpoints in [etl/3_combine.py](etl/3_combine.py). This is also where we filter down to incidents that happened in 2019 or later, where the "brief" field is not blank, and where the incident title or brief contained the string "allision" or "allide" ("allision" is a term for a vessel striking an object). We also included misspellings of "allision" that we found in the data.

If casting a wider net, we could have also included incidents where any facility involved in the incident contained the word "bridge." This would have included more incidents, but also more false positives. As we'll be manually reviewing the briefs, we decided to be somewhat conservative in our filtering.

Many true allisions are likely to have been missed by this filter. We excluded 57 briefless incidents because the brief is necessary for manual review. Most briefless incidents are recent. It is likely that the Coast Guard may eventually upload briefs for these incidents.

### Manual review
*results in data/manual/Activity details-PT review.csv*

We put these incidents through a manual review process. We also removed three incidents where the brief did not describe a bridge allision, despite describing both a bridge and an allision (e.g. a case where a vessel made a course change to avoid a closed drawbridge and ended up alliding with a canal wall.)

At least two people reviewed each of the 650 incidents we included.

Cases where multiple vessels struck the same bridge -- as in a cases where barges broke loose from a tow and struck separately -- were counted as a single incident, if they were part of the same report. This also contributes to the number of strikes being an undercount.

We also reviewed a handful of cases where it was ambiguous whether the "facility_damage" value referred to the bridge or a different facility. In cases where there were multiple facilities listed, or the facility did not include the standalone word "bridge", we reviewed the brief. We identified one record (activity_id 6927518) where it was unclear whether the damage in "facility_damage" included actual damages to the bridge. We'll exclude this record from our count of incidents that caused reported monetary damage to a bridge.

### Geospatial data
*results in data/processed/etl_6_allisions_w_coords.csv*

#### Missing coordinates
First, in [etl/4_filter_id_missing_coord.py](etl/4_filter_id_missing_coord.py), we limited our allisions to those that passed manual review. We also identified all incidents which didn't have any coordinates associated with them in IIR_water_segments.csv. We manually reviewed these incidents and reviewed the brief to determine appropriate coordinates. These are saved in [data/manual/filled_missing_coords.csv](data/manual/filled_missing_coords.csv).

#### Correcting coordinates
We manually reviewed a sample of the coordinates that were present in the data. We combined the NBI and railroad datasets into bridge_locations.csv, and included all incidents where the only given coordinates were more than 1 kilometer away from the nearest bridge location, or where they were described as "approximate" (identified in [etl/5_id_coords_to_review.py](etl/5_id_coords_to_review.py)).

Many of these were in fact in a correct location, some of them far away from the single point in bridge_locations.csv on a very long bridge. We corrected a handful of these cases manually, though all but one were in fact mapped to the correct state, and the correct county or county equivalent. One single coordinate pair (activity ID 7496626) fell outside the continetal US. This turned out to be an error. We reviewed the brief and corrected it manually.

We also checked a small random sample of other coordinates, none of which required correction. 

These are saved in [data/manual/corr_coords.csv](data/manual/corr_coords.csv).

#### Combining and summarizing incident data
In [etl/6_combine_geodata.py](etl/6_combine_geodata.py), we combined the filled coordinates with the rest of the data, and overwrote the relevant coordinates with our manually corrected data resulting in a _master_waterseg_ dataframe.

In cases where there is more than one coordinate pair in master_waterseg for a single incident, we found the centerpoint of the coordinates (either the midpoint of a line or the centroid of a polygon) and used that as the location of the incident.

We also added state and county names to the data, and joined master_waterseg to the rest of the data we have about each allision, saving it as [data/processed/etl_6_allisions_w_coords.csv](data/processed/etl_6_allisions_w_coords.csv). This is the main file in our analysis.

## Analysis and language
All of our analysis can be found in [analysis.ipynb](analysis.ipynb).

### Script and Web Piece
#### At least 650 incidents of a vessel drifting into part of a bridge since 2019

We found 650 unique incidents that passed our manual review process. We found 650 cases of a vessel making contact with part of a bridge.

"Part of a bridge" includes the structural parts of the bridge -- deck, piers, and superstructure -- and auxiliary parts such as lights, fenders, dolphins, and other proteciton systems. These cases also include a handful of incidents where a vessel struck a part of drawbridge while it was closing.

Many of these incidents involved multiple vessels striking a bridge (as in the case of barges that came loose from a tow). We're counting incidents, so the actual number of vessel strikes is higher.

#### OUR ANALYSIS DISCOVERED THAT IN OVER A HUNDRED OF THESE CASES, THE IMPACT WAS HARD ENOUGH TO REQUIRE REPAIRS TO THE STRUCTURE.

We found 109 cases where the Coast Guard recorded damage to the facility involved in the incident -- the "facility_damage" field in the data was not blank and greater than 0. We had flagged one of them as ambiguous (activity_id 6927518), as the information we had made it unclear whether the number in "facility_damage" referred to damage to the bridge or to another facility. Without it our count is 108.

#### WE FOUND A LOT OF THE BRIDGES GETTING STRUCK AGAIN AND AGAIN ARE OVER THE SAME BODY OF WATER: THE MISSISSIPPI RIVER. 

We got at this finding in two different ways. First, we looked at the number of incidents that happened in each water segment by name. We found that the upper Mississippi River ("MISSISSIPPI-UP") was associated with 114 reports and the lower Mississippi River ("MISSISSIPPI-LO") was associated with 56. 

This totals to 170 incidents, though given inconsistencies in water segment naming there may be more. These inconsistencies also make it difficult to rank water segments by the number of incidents. It does, however, seem that the Misssissippi River is among the hardest-hit water segments.

The second way we got to this finding was by visually inspecting the data. The a large number of incidents along the Mississippi River are clearly visible on a national map -- as in the one featured in the piece. Here is a web version made for exploratory purposes: https://public.tableau.com/app/profile/rosie.cima/viz/BridgeAllisionsWebGraphic/Sheet1

### Anchor Tag
And this line was in the anchor tag:

#### WE'VE LEARNED HUNDREDS OF SHIPS HAVE CRASHED INTO BRIDGES ALL OVER THE COUNTRY.
In order for this to be true, we needed to identify a set of at least 200 incidents with unique vessels. We had to work around the fact that many incidents involved multiple vessels, only one of which may have allided with the bridge.

First of all, among incidents involving only one vessel, we found 186 unique vessels.

Then, among incidents that involved multiple vessels, we found 262 incidents where _none_ of the vessels involved in the incident were involved in an additional incident. So each of these 262 incidents involved at least one unique, bridge-alliding vessel.

Adding the number of unique single-incident vessels, and the number of multivessel incidents involving only one-time vessels, totals to at least 448 unique vessels.
