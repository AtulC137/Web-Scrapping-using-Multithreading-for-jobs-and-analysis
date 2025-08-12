import os
import re
import time
import requests
import pandas as pd
from urllib.parse import urljoin
from bs4 import BeautifulSoup

# ============ CONFIG ============
INPUT_XLSX = "internship.xlsx"        # Input file path
SHEET_NAME = "Sheet1"                 # Sheet name from your file
OUTPUT_XLSX = "internship_scraped.xlsx"
START_ROW = 0                         # Row index for batch start
BATCH_COUNT = 800                    # How many companies per run
TIMEOUT = 15

HEADERS = {"User-Agent": "Mozilla/5.0"}

# Output columns
COLUMNS = [
    "Company Name",
    "Website URL",
    "Linkedin URL",
    "Careers Page URL",
    "Job listings page URL",
    "Provider",
    "job post1 URL", "job post1 title", "job post1 location", "job post1 date",
    "job post2 URL", "job post2 title", "job post2 location", "job post2 date",
    "job post3 URL", "job post3 title", "job post3 location", "job post3 date",
]

CAREERS_KEYWORDS = ["career", "careers", "jobs", "join"]

ATS_PATTERNS = {
    "Personio": "personio.com",
    "Teamtailor": "teamtailor.com",
    "Zoho Recruit": "zohorecruit.com",
    "Lever": "lever.co",
    "Greenhouse": "greenhouse.io",
}

# ============ HELPERS ============
def safe_get(url: str):
    """GET request with timeout and error handling."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code < 400:
            return r
    except:
        return None
    return None

def head_ok(url: str):
    """Check if URL exists via HEAD request."""
    try:
        r = requests.head(url, headers=HEADERS, timeout=TIMEOUT)
        return r.status_code < 400
    except:
        return False

def find_careers(base_url: str, html: str):
    """Find careers page link on homepage."""
    soup = BeautifulSoup(html, "lxml")
    for a in soup.select("a[href]"):
        href = a.get("href").strip()
        text = a.get_text(" ", strip=True).lower()
        if any(kw in href.lower() or kw in text for kw in CAREERS_KEYWORDS):
            if not href.startswith("http"):
                href = urljoin(base_url, href)
            return href
    return ""

def detect_ats(url: str):
    """Detect ATS provider from URL."""
    for name, pattern in ATS_PATTERNS.items():
        if url and pattern in url:
            return name
    return "Internal" if url else ""

def scrape_jobs_simple(listings_url: str):
    """Scrape up to 3 jobs (title + URL) from listings page."""
    jobs = []
    r = safe_get(listings_url)
    if not r:
        return jobs
    soup = BeautifulSoup(r.text, "lxml")
    for a in soup.select("a[href]"):
        title = a.get_text(" ", strip=True)
        if not title:
            continue
        if re.search(r"(engineer|manager|developer|analyst|designer|specialist|intern|consultant|coordinator|technician)", title, re.I):
            job_url = a.get("href")
            if not job_url:
                continue
            if not job_url.startswith("http"):
                job_url = urljoin(listings_url, job_url)
            jobs.append({"url": job_url, "title": title, "location": "", "date": ""})
        if len(jobs) >= 3:
            break
    return jobs

def process_company(name: str):
    """Process a single company and return dict with job info."""
    name_clean = str(name).strip()
    website, linkedin, careers, jobs_url, provider = "", "", "", "", ""

    # Guess LinkedIn
    linkedin = f"https://www.linkedin.com/company/{name_clean.lower().replace(' ', '-')}/"

    # Guess website from TLDs
    for tld in ["com", "org", "io", "ai", "net", "co"]:
        test_url = f"https://{name_clean.lower().replace(' ', '')}.{tld}"
        if head_ok(test_url):
            website = test_url
            break

    # Find careers page
    if website:
        resp = safe_get(website)
        if resp:
            careers = find_careers(website, resp.text)

    jobs_url = careers or website or ""
    provider = detect_ats(jobs_url)
    jobs = scrape_jobs_simple(jobs_url) if jobs_url else []

    row = {
        "Company Name": name_clean,
        "Website URL": website,
        "Linkedin URL": linkedin,
        "Careers Page URL": careers,
        "Job listings page URL": jobs_url,
        "Provider": provider
    }

    for i in range(3):
        if i < len(jobs):
            row[f"job post{i+1} URL"] = jobs[i]["url"]
            row[f"job post{i+1} title"] = jobs[i]["title"]
            row[f"job post{i+1} location"] = jobs[i]["location"]
            row[f"job post{i+1} date"] = jobs[i]["date"]
        else:
            row[f"job post{i+1} URL"] = ""
            row[f"job post{i+1} title"] = ""
            row[f"job post{i+1} location"] = ""
            row[f"job post{i+1} date"] = ""

    return row

# ============ MAIN ============
def main():
    df = pd.read_excel(INPUT_XLSX, sheet_name=SHEET_NAME, engine="openpyxl")
    df = df[df["Company Name"].notna()]
    df_batch = df.iloc[START_ROW:START_ROW + BATCH_COUNT]

    results = []
    for _, r in df_batch.iterrows():
        results.append(process_company(r["Company Name"]))
        time.sleep(0.8)

    out_df = pd.DataFrame(results, columns=COLUMNS)

    # Append mode without writer.book
    if os.path.exists(OUTPUT_XLSX):
        existing_df = pd.read_excel(OUTPUT_XLSX, sheet_name="Data", engine="openpyxl")
        combined_df = pd.concat([existing_df, out_df], ignore_index=True)
        combined_df.to_excel(OUTPUT_XLSX, sheet_name="Data", index=False, engine="openpyxl")
    else:
        out_df.to_excel(OUTPUT_XLSX, sheet_name="Data", index=False, engine="openpyxl")

    print(f"Added {len(out_df)} companies → {OUTPUT_XLSX}")

if __name__ == "__main__":
    main()
