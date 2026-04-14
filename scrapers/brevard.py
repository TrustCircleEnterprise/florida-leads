"""
Brevard County, FL - Motivated Seller Lead Scraper
"""
import csv, io, json, logging, re, sys, time, requests, urllib3
from datetime import datetime, timedelta
from pathlib import Path

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger("brevard_scraper")

BASE_URL       = "https://vaclmweb1.brevardclerk.us/AcclaimWeb"
DISCLAIMER_URL = f"{BASE_URL}/search/Disclaimer"
SEARCH_URL     = f"{BASE_URL}/search/SearchTypeRecordDate"
EXPORT_URL     = f"{BASE_URL}/Search/ExportCsv"

REPO_ROOT = Path(__file__).parent.parent
DATA_DIR  = REPO_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

TARGET_TYPES = {
    "LIS PENDENS","NOTICE OF LIS PENDENS",
    "FORECLOSURE","NOTICE OF FORECLOSURE","NOTICE OF DEFAULT",
    "TAX DEED","TAX DEED SALE",
    "JUDGMENT","FINAL JUDGMENT","CERTIFIED JUDGMENT","DEFAULT JUDGMENT",
    "IRS LIEN","FEDERAL TAX LIEN","STATE TAX LIEN","TAX LIEN",
    "CLAIM OF LIEN","MECHANIC LIEN","MECHANICS LIEN",
    "HOA LIEN","HOMEOWNERS ASSOCIATION LIEN",
    "NOTICE OF COMMENCEMENT",
    "PROBATE","LETTERS OF ADMINISTRATION",
}

def classify(doc_type):
    dt = doc_type.upper().strip()
    if "LIS PENDENS" in dt: return "LP","Lis Pendens","lis_pendens"
    if "FORECLOSURE" in dt or "DEFAULT" in dt: return "FC","Foreclosure","foreclosure"
    if "TAX DEED" in dt: return "TD","Tax Deed","tax_deed"
    if "JUDGMENT" in dt: return "JUD","Judgment","judgment"
    if any(x in dt for x in ["IRS","FEDERAL TAX LIEN","STATE TAX LIEN","TAX LIEN"]): return "TL","Tax Lien","tax_lien"
    if any(x in dt for x in ["MECHANIC","CLAIM OF LIEN","HOA LIEN"]): return "CL","Claim of Lien","lien"
    if "NOTICE OF COMMENCEMENT" in dt: return "NOC","Notice of Commencement","noc"
    if "PROBATE" in dt or "LETTERS OF" in dt: return "PRO","Probate","probate"
    return dt, doc_type.title(), "other"

def norm_date(raw):
    raw = str(raw).strip()
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError: pass
    return raw.split()[0] if raw else raw

def scrape_day(date_str):
    """date_str = YYYY-MM-DD"""
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0"})

    # Accept disclaimer
    s.post(DISCLAIMER_URL, data={"disclaimer": "true"}, verify=False, timeout=30)

    # Search by record date (format MM/DD/YYYY)
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_formatted = dt.strftime("%-m/%-d/%Y")

    s.post(SEARCH_URL, data={"RecordDate": date_formatted}, verify=False, timeout=30)

    # Export CSV
    csv_res = s.get(EXPORT_URL, verify=False, timeout=30)
    if not csv_res.ok:
        log.warning(f"CSV export failed for {date_str}: {csv_res.status_code}")
        return []

    records = []
    try:
        content = csv_res.content.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(content))
        for row in reader:
            doc_type = (row.get("DocTypeDescription") or "").strip().upper()
            if not any(t in doc_type for t in TARGET_TYPES):
                continue
            grantor  = (row.get("DirectName") or "").strip()
            grantee  = (row.get("IndirectName") or "").strip()
            amount_s = re.sub(r"[^\d.]", "", row.get("Consideration") or "0")
            amount   = float(amount_s) if amount_s else 0.0
            cfn      = (row.get("InstrumentNumber") or "").strip()
            legal    = (row.get("DocLegalDescription") or "").strip()
            rec_date = norm_date(row.get("RecordDate") or date_str)
            matched, label, cat = classify(doc_type)
            clerk_url = f"https://vaclmweb1.brevardclerk.us/AcclaimWeb/search/SearchTypeInstrumentNumber?instrumentNumber={cfn}" if cfn else ""
            records.append({
                "doc_num": cfn, "doc_type": matched, "filed": rec_date,
                "cat": cat, "cat_label": label,
                "owner": grantor, "grantee": grantee,
                "amount": amount, "legal": legal, "county": "Brevard",
                "clerk_url": clerk_url,
                "prop_address": "", "prop_city": "", "prop_state": "FL", "prop_zip": "",
                "mail_address": "", "mail_city": "", "mail_state": "", "mail_zip": "",
                "flags": [], "score": 0,
            })
    except Exception as e:
        log.warning(f"CSV parse error for {date_str}: {e}")

    return records

def business_days_back(n):
    days = []
    d = datetime.now()
    while len(days) < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            days.append(d.strftime("%Y-%m-%d"))
    return days

WEEK_AGO = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

def compute_flags_and_score(rec, all_records):
    flags = []
    score = 30
    cat     = rec.get("cat", "")
    grantee = rec.get("grantee", "")
    filed   = rec.get("filed", "")
    dt      = rec.get("doc_type", "")
    if dt == "LP":              flags.append("Lis pendens")
    if dt == "FC":              flags.append("Pre-foreclosure")
    if cat == "judgment":       flags.append("Judgment lien")
    if cat == "tax_lien":       flags.append("Tax lien")
    if cat == "lien":           flags.append("Mechanic lien")
    if cat == "probate":        flags.append("Probate / estate")
    if grantee and re.search(r"\b(LLC|INC|CORP|LTD|TRUST|ESTATE)\b", grantee.upper()):
        flags.append("LLC / corp owner")
    if filed >= WEEK_AGO:       flags.append("New this week")
    score += len(flags) * 10
    amt = rec.get("amount", 0) or 0
    if amt > 100000:  score += 15
    elif amt > 50000: score += 10
    if filed >= WEEK_AGO: score += 5
    return flags, min(score, 100)

def main():
    log.info("Brevard County FL - Motivated Seller Scraper")
    days = business_days_back(14)
    log.info(f"Checking {len(days)} business days: {days[-1]} to {days[0]}")

    all_records = []
    for day in reversed(days):
        try:
            recs = scrape_day(day)
            if recs:
                all_records.extend(recs)
                log.info(f"Day {day}: {len(recs)} target records")
            else:
                log.info(f"Day {day}: 0 target records")
            time.sleep(1)
        except Exception as e:
            log.warning(f"Day {day} failed: {e}")

    seen = set()
    unique = []
    for r in all_records:
        key = r.get("doc_num") or id(r)
        if key not in seen:
            seen.add(key)
            unique.append(r)
    all_records = unique
    log.info(f"Total unique records: {len(all_records)}")

    for rec in all_records:
        try:
            flags, score = compute_flags_and_score(rec, all_records)
            rec["flags"] = flags
            rec["score"] = score
        except Exception:
            rec["flags"] = []
            rec["score"] = 30

    all_records.sort(key=lambda r: r.get("score", 0), reverse=True)

    payload = {
        "fetched_at":  datetime.utcnow().isoformat() + "Z",
        "source":      "Brevard County Clerk of Circuit Courts",
        "county":      "Brevard",
        "state":       "FL",
        "date_range":  {"from": days[-1], "to": days[0]},
        "total":       len(all_records),
        "with_address": 0,
        "records":     all_records,
    }

    out = DATA_DIR / "brevard.json"
    out.write_text(json.dumps(payload, indent=2, default=str))
    log.info(f"Wrote {len(all_records)} records to {out}")
    log.info("Done")

if __name__ == "__main__":
    main()


def enrich_with_parcels(records):
    """Add addresses to Brevard records using Florida DOR parcel API."""
    try:
        from parcel_lookup import lookup_by_name
    except ImportError:
        import sys
        sys.path.insert(0, str(Path(__file__).parent))
        from parcel_lookup import lookup_by_name

    matched = 0
    for i, rec in enumerate(records):
        if rec.get("prop_address"):
            continue
        for name in [rec.get("grantee", ""), rec.get("owner", "")]:
            if not name:
                continue
            if re.search(r"\b(LLC|INC|CORP|LTD|TRUST|STATE OF|COUNTY|CITY OF)\b", name.upper()):
                continue
            result = lookup_by_name(name, "Brevard")
            if result:
                for k, v in result.items():
                    if v:
                        rec[k] = v
                matched += 1
                break
        if i % 100 == 0 and i > 0:
            log.info(f"  Address lookup: {i}/{len(records)} records, {matched} matched")
        import time
        time.sleep(0.3)
    log.info(f"Address enrichment: {matched}/{len(records)} matched")
    return records
