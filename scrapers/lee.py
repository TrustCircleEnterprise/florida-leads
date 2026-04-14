"""
Lee County, FL - Motivated Seller Lead Scraper
Uses Selenium with visible Chrome to login and export records
"""
import json, logging, re, sys, time, glob, os
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger("lee_scraper")

REPO_ROOT    = Path(__file__).parent.parent
DATA_DIR     = REPO_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DOWNLOAD_DIR = str(Path.home() / "Downloads")

USERNAME  = "trustcircleenterprise@gmail.com"
PASSWORD  = "1556572275"
LOGIN_URL = "https://or.leeclerk.org/LandMarkWeb/Account/Logon"
SEARCH_URL= "https://or.leeclerk.org/LandMarkWeb/search/index?theme=.blue&section=searchCriteriaRecordDate"

TARGET_TYPES = {
    "LIS PENDENS","NOTICE OF LIS PENDENS",
    "FORECLOSURE","NOTICE OF FORECLOSURE","NOTICE OF DEFAULT",
    "TAX DEED","TAX DEED SALE",
    "JUDGMENT","FINAL JUDGMENT","CERTIFIED JUDGMENT","DEFAULT JUDGMENT",
    "IRS LIEN","FEDERAL TAX LIEN","STATE TAX LIEN","TAX LIEN",
    "CLAIM OF LIEN","MECHANIC LIEN","MECHANICS LIEN","HOA LIEN",
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
    for fmt in ("%m/%d/%Y %I:%M:%S %p","%m/%d/%Y %H:%M:%S","%m/%d/%Y","%Y-%m-%d"):
        try: return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except: pass
    return raw.split()[0] if raw else raw

def get_driver():
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--ignore-certificate-errors")
    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
    }
    opts.add_experimental_option("prefs", prefs)
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    return webdriver.Chrome(options=opts)

def wait_for_download(pattern, timeout=30):
    end = time.time() + timeout
    while time.time() < end:
        files = glob.glob(os.path.join(DOWNLOAD_DIR, pattern))
        if files:
            newest = max(files, key=os.path.getctime)
            if not newest.endswith(".crdownload"):
                return newest
        time.sleep(1)
    return None

def scrape():
    log.info("Lee County FL - Motivated Seller Scraper")
    driver = get_driver()
    wait = WebDriverWait(driver, 30)

    try:
        # Login
        log.info("Logging in...")
        driver.get(LOGIN_URL)
        time.sleep(5)
        wait.until(EC.presence_of_element_located((By.NAME, "UserName")))
        driver.find_element(By.NAME, "UserName").clear()
        driver.find_element(By.NAME, "UserName").send_keys(USERNAME)
        driver.find_element(By.NAME, "Password").clear()
        driver.find_element(By.NAME, "Password").send_keys(PASSWORD)
        driver.find_element(By.CSS_SELECTOR, "input[type='submit'], button[type='submit'], input[value='Log On']").click()
        time.sleep(5)
        log.info(f"After login: {driver.current_url}")

        # Navigate to record date search
        log.info("Navigating to record date search...")
        driver.get(SEARCH_URL)
        time.sleep(5)
        log.info(f"Search page: {driver.title}")

        # Set date range
        end_date   = datetime.now().strftime("%-m/%-d/%Y")
        start_date = (datetime.now() - timedelta(days=7)).strftime("%-m/%-d/%Y")
        log.info(f"Searching {start_date} to {end_date}")

        # Fill in dates
        inputs = driver.find_elements(By.TAG_NAME, "input")
        date_inputs = [i for i in inputs if "date" in (i.get_attribute("name") or "").lower()
                       or "date" in (i.get_attribute("id") or "").lower()
                       or i.get_attribute("type") == "text"]
        log.info(f"Found {len(date_inputs)} date-like inputs")
        for inp in date_inputs:
            log.info(f"  Input: name={inp.get_attribute('name')} id={inp.get_attribute('id')}")

        if len(date_inputs) >= 2:
            date_inputs[0].clear()
            date_inputs[0].send_keys(start_date)
            date_inputs[1].clear()
            date_inputs[1].send_keys(end_date)
        elif len(date_inputs) == 1:
            date_inputs[0].clear()
            date_inputs[0].send_keys(end_date)

        # Click search
        try:
            search_btn = driver.find_element(By.CSS_SELECTOR, "input[value='Search'], button[type='submit'], input[type='submit']")
            search_btn.click()
        except:
            driver.find_element(By.XPATH, "//input[@type='submit'] | //button[contains(text(),'Search')]").click()

        time.sleep(8)
        log.info(f"Results page: {driver.title}")
        driver.save_screenshot("/tmp/lee_results.png")

        # Click Export
        log.info("Clicking Export...")
        before_files = set(glob.glob(os.path.join(DOWNLOAD_DIR, "*.xlsx")))
        try:
            export_btn = driver.find_element(By.XPATH, "//input[@value='Export'] | //a[contains(text(),'Export')] | //button[contains(text(),'Export')]")
            export_btn.click()
        except Exception as e:
            log.warning(f"Export button not found: {e}")
            driver.save_screenshot("/tmp/lee_export_error.png")
            return []

        time.sleep(5)
        after_files = set(glob.glob(os.path.join(DOWNLOAD_DIR, "*.xlsx")))
        new_files = after_files - before_files
        if not new_files:
            # Wait longer
            time.sleep(15)
            after_files = set(glob.glob(os.path.join(DOWNLOAD_DIR, "*.xlsx")))
            new_files = after_files - before_files

        if not new_files:
            log.warning("No new Excel file downloaded")
            return []

        xlsx_file = max(new_files, key=os.path.getctime)
        log.info(f"Downloaded: {xlsx_file}")
        return process_excel(xlsx_file)

    except Exception as e:
        log.error(f"Error: {e}")
        driver.save_screenshot("/tmp/lee_error.png")
        return []
    finally:
        # Logoff before closing
        try:
            driver.get("https://or.leeclerk.org/LandMarkWeb/Account/LogOff")
            time.sleep(2)
        except: pass
        driver.quit()

def process_excel(xlsx_file):
    log.info(f"Processing {xlsx_file}")
    df = pd.read_excel(xlsx_file)
    log.info(f"Columns: {list(df.columns)}")
    log.info(f"Total rows: {len(df)}")

    records = []
    WEEK_AGO = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    for _, row in df.iterrows():
        doc_type = str(row.get("Doc Type") or row.get("DocType") or row.get("Document Type") or "").strip().upper()
        if not any(t in doc_type for t in TARGET_TYPES):
            continue

        grantor  = str(row.get("Grantor") or "").strip()
        grantee  = str(row.get("Grantee") or "").strip()
        amount   = float(re.sub(r"[^\d.]", "", str(row.get("Consideration") or "0")) or 0)
        cfn      = str(row.get("Clerk File Number") or row.get("ClerkFileNumber") or "").strip()
        legal    = str(row.get("Legal") or row.get("Legal Description") or "").strip()
        rec_date = norm_date(str(row.get("Record Date") or row.get("RecordDate") or ""))

        matched, label, cat = classify(doc_type)
        clerk_url = f"https://or.leeclerk.org/LandMarkWeb/search/index?theme=.blue&section=searchCriteriaClerkFileNumber&quickSearchSelection=&clerkFileNumber={cfn}" if cfn else ""

        flags = []
        score = 30
        if matched == "LP":   flags.append("Lis pendens")
        if matched == "FC":   flags.append("Pre-foreclosure")
        if cat == "judgment": flags.append("Judgment lien")
        if cat == "tax_lien": flags.append("Tax lien")
        if cat == "lien":     flags.append("Mechanic lien")
        if cat == "probate":  flags.append("Probate / estate")
        if re.search(r"\b(LLC|INC|CORP|LTD|TRUST)\b", grantee.upper()):
            flags.append("LLC / corp owner")
        if rec_date >= WEEK_AGO: flags.append("New this week")
        score += len(flags) * 10
        if amount > 100000: score += 15
        elif amount > 50000: score += 10
        if rec_date >= WEEK_AGO: score += 5
        score = min(score, 100)

        records.append({
            "doc_num": cfn, "doc_type": matched, "filed": rec_date,
            "cat": cat, "cat_label": label,
            "owner": grantor, "grantee": grantee,
            "amount": amount, "legal": legal, "county": "Lee",
            "clerk_url": clerk_url,
            "prop_address": "", "prop_city": "", "prop_state": "FL", "prop_zip": "",
            "mail_address": "", "mail_city": "", "mail_state": "", "mail_zip": "",
            "flags": flags, "score": score,
        })

    records.sort(key=lambda r: r.get("score", 0), reverse=True)
    log.info(f"Filtered to {len(records)} target records")
    return records

def main():
    records = scrape()
    if not records:
        log.warning("No records found")
        return

    payload = {
        "fetched_at":  datetime.utcnow().isoformat() + "Z",
        "source":      "Lee County Clerk of Circuit Courts",
        "county":      "Lee",
        "state":       "FL",
        "date_range":  {"from": (datetime.now()-timedelta(days=7)).strftime("%Y-%m-%d"), "to": datetime.now().strftime("%Y-%m-%d")},
        "total":       len(records),
        "with_address": sum(1 for r in records if r.get("prop_address")),
        "records":     records,
    }

    out = DATA_DIR / "lee.json"
    out.write_text(json.dumps(payload, indent=2, default=str))
    log.info(f"Wrote {len(records)} records to {out}")

if __name__ == "__main__":
    main()
