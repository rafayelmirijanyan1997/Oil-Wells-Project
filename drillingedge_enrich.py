import re
import time
import sqlite3
from urllib.parse import quote

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

DB_PATH = "wells.sqlite"
SEARCH_URL = "https://www.drillingedge.com/search"

# ---------- helpers ----------
def normalize_api(api_raw):
    """
    Your DB might store API as 3305306057 (no dashes).
    DrillingEdge expects 33-053-06057 style.
    """
    if not api_raw:
        return None
    api_raw = re.sub(r"\D", "", str(api_raw))
    if len(api_raw) == 10:
        return api_raw[0:2] + "-" + api_raw[2:5] + "-" + api_raw[5:10]
    return None

def parse_oil_gas_numbers(page_text):
    """
    Examples seen on DrillingEdge well pages:
      "396 Barrels of Oil Produced in Dec 2025"
      "1 k MCF of Gas Produced in Dec 2025"
      "2.2 k MCF of Gas Produced in May 2023"
    Returns (oil_bbl, gas_mcf, label)
    """
    t = page_text

    # Oil
    oil = None
    oil_label = None
    m = re.search(r"([\d.]+)\s*(k)?\s*Barrels of Oil Produced in\s+([A-Za-z]{3}\s+\d{4})", t, re.I)
    if m:
        val = float(m.group(1))
        if m.group(2):  # 'k'
            val *= 1000.0
        oil = val
        oil_label = m.group(3)

    # Gas
    gas = None
    gas_label = None
    m = re.search(r"([\d.]+)\s*(k)?\s*MCF of Gas Produced in\s+([A-Za-z]{3}\s+\d{4})", t, re.I)
    if m:
        val = float(m.group(1))
        if m.group(2):
            val *= 1000.0
        gas = val
        gas_label = m.group(3)

    # pick label if we have one
    label = oil_label or gas_label
    return oil, gas, label

def find_detail_value_in_text(page_text, key):
    """
    Works because the public page often contains lines like:
      "Well Status Active"
      "Well Type Oil & Gas"
      "Closest City Williston"
    """
    # Grab value after key until newline
    pattern = r"%s\s+([A-Za-z0-9 &/.-]+)" % re.escape(key)
    m = re.search(pattern, page_text, re.I)
    return m.group(1).strip() if m else None

# ---------- selenium setup ----------
def make_driver(chromedriver_path=None, headless=True):
    opts = Options()
    if headless:
        # for newer Chrome
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1400,900")

    if chromedriver_path:
        driver = webdriver.Chrome(executable_path=chromedriver_path, options=opts)
    else:
        driver = webdriver.Chrome(options=opts)  # uses PATH
    driver.set_page_load_timeout(60)
    return driver

# ---------- scraping ----------
def drillingedge_lookup(driver, api_dashed=None, well_name=None):
    """
    1) open /search
    2) fill API field (best) else Well Name
    3) submit
    4) click the result row that matches API
    5) parse details page
    """
    driver.get(SEARCH_URL)
    time.sleep(1.0)

    # Fill API or well name. (The search page has multiple inputs; we target by placeholder-ish labels.)
    if api_dashed:
        # Find the API input by looking for input elements and picking the one near "API"
        inputs = driver.find_elements(By.TAG_NAME, "input")
        api_input = None
        for inp in inputs:
            try:
                ph = (inp.get_attribute("placeholder") or "").lower()
                name = (inp.get_attribute("name") or "").lower()
                # heuristics
                if "api" in ph or "api" in name:
                    api_input = inp
                    break
            except:
                pass

        # Fallback: just pick the 3rd input (site layout tends to be stable but not guaranteed)
        if api_input is None and len(inputs) >= 3:
            api_input = inputs[2]

        if api_input is None:
            return None

        api_input.clear()
        api_input.send_keys(api_dashed)

    elif well_name:
        inputs = driver.find_elements(By.TAG_NAME, "input")
        if not inputs:
            return None
        # guess "well name" input is one of the first fields
        well_input = inputs[1] if len(inputs) > 1 else inputs[0]
        well_input.clear()
        well_input.send_keys(well_name)

    else:
        return None

    # Click Search button (there should be a button or input submit)
    # We'll try common candidates:
    buttons = driver.find_elements(By.TAG_NAME, "button")
    clicked = False
    for b in buttons:
        txt = (b.text or "").strip().lower()
        if "search" in txt:
            b.click()
            clicked = True
            break
    if not clicked:
        # try pressing enter in the last focused input
        api_input = driver.switch_to.active_element
        api_input.send_keys("\n")

    time.sleep(2.0)

    # Search results table has links; click the best match
    links = driver.find_elements(By.TAG_NAME, "a")
    best = None
    if api_dashed:
        for a in links:
            href = a.get_attribute("href") or ""
            if api_dashed in href:
                best = a
                break

    # fallback: click the first "API #" link-looking result
    if best is None:
        for a in links:
            href = a.get_attribute("href") or ""
            if "/wells/" in href and "drillingedge.com" in href:
                best = a
                break

    if best is None:
        return None

    url = best.get_attribute("href")
    best.click()
    time.sleep(2.0)

    # Now parse the detail page (use page text)
    page_text = driver.find_element(By.TAG_NAME, "body").text

    well_status = find_detail_value_in_text(page_text, "Well Status")
    well_type = find_detail_value_in_text(page_text, "Well Type")
    closest_city = find_detail_value_in_text(page_text, "Closest City")

    oil_bbl, gas_mcf, label = parse_oil_gas_numbers(page_text)

    return {
        "drillingedge_url": url,
        "well_status": well_status,
        "well_type": well_type,
        "closest_city": closest_city,
        "latest_oil_bbl": oil_bbl,
        "latest_gas_mcf": gas_mcf,
        "latest_prod_label": label
    }

# ---------- DB loop ----------
def main(chromedriver_path=None):
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # only enrich rows missing drillingedge_url (or missing fields)
    rows = cur.execute("""
        SELECT id, api, well_name
        FROM wells
        ORDER BY id
    """).fetchall()

    driver = make_driver(chromedriver_path=chromedriver_path, headless=True)

    try:
        for r in rows:
            api_dashed = normalize_api(r["api"])
            well_name = r["well_name"]

            print("\n---", r["id"], api_dashed, well_name, "---")

            data = drillingedge_lookup(driver, api_dashed=api_dashed, well_name=well_name)
            if not data:
                print("No results")
                continue

            cur.execute("""
                UPDATE wells
                SET drillingedge_url=?,
                    well_status=?,
                    well_type=?,
                    closest_city=?,
                    latest_oil_bbl=?,
                    latest_gas_mcf=?,
                    latest_prod_label=?
                WHERE id=?
            """, (
                data.get("drillingedge_url"),
                data.get("well_status"),
                data.get("well_type"),
                data.get("closest_city"),
                data.get("latest_oil_bbl"),
                data.get("latest_gas_mcf"),
                data.get("latest_prod_label"),
                r["id"]
            ))
            con.commit()

            print("Saved:", data)

            # be polite to the site
            time.sleep(1.0)

    finally:
        driver.quit()
        con.close()

if __name__ == "__main__":
    # If chromedriver is NOT on PATH, set a full path here:
    # main(chromedriver_path="/usr/local/bin/chromedriver")
    main()