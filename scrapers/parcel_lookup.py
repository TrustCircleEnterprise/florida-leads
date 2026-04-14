"""
Florida Statewide Parcel Lookup
Uses Florida DOR ArcGIS REST API to look up property addresses by owner name
Works for all 67 Florida counties
"""
import requests
import re
import time
import logging

log = logging.getLogger(__name__)

PARCEL_API = "https://services9.arcgis.com/Gh9awoU677aKree0/arcgis/rest/services/Florida_Statewide_Cadastral/FeatureServer/0/query"

# Florida county numbers
COUNTY_NUMBERS = {
    "Alachua": 1, "Baker": 2, "Bay": 3, "Bradford": 4, "Brevard": 5,
    "Broward": 6, "Calhoun": 7, "Charlotte": 8, "Citrus": 9, "Clay": 10,
    "Collier": 11, "Columbia": 12, "Miami-Dade": 13, "DeSoto": 14,
    "Dixie": 15, "Duval": 16, "Escambia": 17, "Flagler": 18, "Franklin": 19,
    "Gadsden": 20, "Gilchrist": 21, "Glades": 22, "Gulf": 23, "Hamilton": 24,
    "Hardee": 25, "Hendry": 26, "Hernando": 27, "Highlands": 28,
    "Hillsborough": 29, "Holmes": 30, "Indian River": 31, "Jackson": 32,
    "Jefferson": 33, "Lafayette": 34, "Lake": 35, "Lee": 36, "Leon": 37,
    "Levy": 38, "Liberty": 39, "Madison": 40, "Manatee": 41, "Marion": 42,
    "Martin": 43, "Monroe": 44, "Nassau": 45, "Okaloosa": 46,
    "Okeechobee": 47, "Orange": 48, "Osceola": 49, "Palm Beach": 50,
    "Pasco": 51, "Pinellas": 52, "Polk": 53, "Putnam": 54,
    "St. Johns": 55, "St. Lucie": 56, "Santa Rosa": 57, "Sarasota": 58,
    "Seminole": 59, "Sumter": 60, "Suwannee": 61, "Taylor": 62,
    "Union": 63, "Volusia": 64, "Wakulla": 65, "Walton": 66, "Washington": 67,
}

def _norm(s):
    return re.sub(r"\s+", " ", str(s or "")).strip().upper()

def lookup_by_name(owner_name, county_name, timeout=30):
    """Look up property address by owner name and county."""
    co_no = COUNTY_NUMBERS.get(county_name)
    if not co_no:
        return None

    name = _norm(owner_name)
    if not name:
        return None

    # Skip entities
    if re.search(r"\b(LLC|INC|CORP|LTD|TRUST|STATE OF|COUNTY|CITY OF|FEDERAL|USA)\b", name):
        return None

    # Try exact match first, then partial
    for where in [
        f"CO_NO={co_no} AND OWN_NAME='{name}'",
        f"CO_NO={co_no} AND OWN_NAME LIKE '{name.split()[0]}%{name.split()[-1] if len(name.split())>1 else ''}%'",
    ]:
        try:
            params = {
                "where": where,
                "outFields": "OWN_NAME,PHY_ADDR1,PHY_CITY,PHY_ZIPCD,OWN_ADDR1,OWN_CITY,OWN_STATE,OWN_ZIPCD",
                "resultRecordCount": 1,
                "f": "json",
            }
            r = requests.get(PARCEL_API, params=params, timeout=timeout)
            if not r.ok:
                continue
            data = r.json()
            features = data.get("features", [])
            if features:
                a = features[0]["attributes"]
                addr = (a.get("PHY_ADDR1") or "").strip()
                if addr and not addr.startswith("0 "):
                    return {
                        "prop_address": addr,
                        "prop_city":    (a.get("PHY_CITY") or "").strip(),
                        "prop_state":   "FL",
                        "prop_zip":     str(a.get("PHY_ZIPCD") or "").strip(),
                        "mail_address": (a.get("OWN_ADDR1") or "").strip(),
                        "mail_city":    (a.get("OWN_CITY") or "").strip(),
                        "mail_state":   (a.get("OWN_STATE") or "FL").strip(),
                        "mail_zip":     str(a.get("OWN_ZIPCD") or "").strip(),
                    }
        except Exception as e:
            log.debug(f"Parcel lookup error: {e}")
            time.sleep(1)
    return None

def enrich_records(records, county_name, delay=0.3):
    """Enrich a list of records with property addresses."""
    matched = 0
    for i, rec in enumerate(records):
        if rec.get("prop_address"):
            continue
        for name in [rec.get("grantee", ""), rec.get("owner", "")]:
            if not name:
                continue
            result = lookup_by_name(name, county_name)
            if result:
                for k, v in result.items():
                    if v:
                        rec[k] = v
                matched += 1
                break
        if i % 50 == 0:
            log.info(f"  Enriched {i}/{len(records)} records ({matched} matched)")
        time.sleep(delay)
    log.info(f"Parcel lookup complete: {matched}/{len(records)} records matched")
    return records

if __name__ == "__main__":
    # Test
    result = lookup_by_name("HARRIS WILL", "Hillsborough")
    print("Test result:", result)
