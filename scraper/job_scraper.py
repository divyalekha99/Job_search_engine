# scraper/job_scraper.py

import os
import json
import time
import re
import requests
from kafka import KafkaProducer, errors
from bs4 import BeautifulSoup
from requests.exceptions import HTTPError

# ─── Persist “seen” state ─────────────────────────────────────────────────────
STATE_DIR  = os.getenv("STATE_DIR", "/data/state")
os.makedirs(STATE_DIR, exist_ok=True)
STATE_FILE = os.path.join(STATE_DIR, "seen_ids.json")

try:
    with open(STATE_FILE) as f:
        seen = set(json.load(f))
    print(f"Loaded {len(seen)} seen jobs")
except FileNotFoundError:
    seen = set()
    print("No previous state, starting fresh")
# ───────────────────────────────────────────────────────────────────────────────

# ─── Kafka setup ──────────────────────────────────────────────────────────────
bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            enable_idempotence=True,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        producer.bootstrap_connected()
        print("Connected to Kafka at", bootstrap)
        break
    except errors.NoBrokersAvailable:
        print("Kafka not yet ready… retry in 2s")
        time.sleep(2)
# ───────────────────────────────────────────────────────────────────────────────

# ─── Constants ────────────────────────────────────────────────────────────────
TOPIC    = "jobs"
KEYWORDS = "Data%20Engineer"
LOCATION = "Netherlands"
GEOID    = "102890719"
HEADERS  = {
    "User-Agent": (
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/115.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01"
}
# ───────────────────────────────────────────────────────────────────────────────

def fetch_search(start=0):
    """
    Fetch one page of public guest-API results for LinkedIn job cards,
    parse out title, company, location, skills, and the job-view URL.
    """
    url = (
        "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
        f"?keywords={KEYWORDS}"
        f"&location={LOCATION}"
        f"&geoId={GEOID}"
        f"&distance=25"
        f"&start={start}"
    )
    resp = requests.get(url, headers=HEADERS, timeout=10)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    cards = soup.select("div.job-search-card, div.base-search-card, div.base-card--link")
    print(f"[DEBUG] URL={url} → {resp.status_code}, matched {len(cards)} cards")

    results = []
    for card in cards:
        urn     = card.get("data-entity-urn", "")
        job_id  = urn.rsplit(":", 1)[-1]
        title_e = card.select_one("h3.base-search-card__title")
        comp_e  = card.select_one("h4.base-search-card__subtitle")
        loc_e   = card.select_one(".job-search-card__location")
        link_e  = card.select_one("a.base-card__full-link")

        # Extract skills snippet if present
        skills_text = None
        skills_el   = card.find(lambda t: t.name=="span" and "Skills:" in t.get_text())
        if skills_el:
            skills_text = skills_el.get_text().replace("Skills:", "").strip()

        results.append({
            "jobId":    job_id,
            "title":    title_e.get_text(strip=True) if title_e else None,
            "company":  comp_e.get_text(strip=True)  if comp_e  else None,
            "location": loc_e.get_text(strip=True)  if loc_e   else None,
            "skills":   skills_text,
            "applyUrl": link_e["href"]              if link_e  else None,
        })

    return results


def fetch_detail(job_id, apply_url=None):
    """
    1) Try the guest-API JSON endpoint.
    2) If that fails (bad JSON or rate-limit), GET the apply_url page
       and scrape HTML to extract description, requirements bullets,
       and the true apply button href.
    """
    api_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
    # Step 1: try the JSON endpoint
    try:
        resp = requests.get(api_url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            raise ValueError("not a JSON object")
    except Exception:
        # JSON failed — fall back to HTML scrape if we have an apply_url
        if not apply_url:
            return {}
        try:
            page = requests.get(apply_url, headers=HEADERS, timeout=10)
            page.raise_for_status()
        except Exception as html_err:
            print(f"Warning: cannot load HTML detail for {job_id}: {html_err}")
            return {}

        doc = BeautifulSoup(page.text, "html.parser")

        # description container
        desc_div = doc.select_one("div.description__text, div.show-more-less-html__markup")
        description = desc_div.get_text("\n", strip=True) if desc_div else ""

        # find first <h2> or <h3> containing “Requirements” or “Qualifications”,
        # then grab the next <ul> sibling’s <li> items
        requirements = []
        for hdr in doc.select("h2, h3"):
            if re.search(r"(Requirements|Qualifications)", hdr.get_text(), re.I):
                ul = hdr.find_next_sibling("ul")
                if ul:
                    requirements = [li.get_text(strip=True) for li in ul.select("li")]
                break

        # real apply button href
        apply_btn = doc.select_one("a.apply-button, a.apply__button")
        true_apply = apply_btn["href"] if apply_btn and apply_btn.has_attr("href") else apply_url

        return {
            "description":  description,
            "requirements": requirements,
            "applyLink":    true_apply,
        }

    # Step 2: JSON succeeded — parse description + bullets + applyMethod
    desc_html = data.get("description", "")
    description = BeautifulSoup(desc_html, "html.parser") \
                    .get_text("\n", strip=True)

    # pull out bulletized Requirements/Qualifications from that text blob
    reqs = []
    match = re.search(
        r"(?mi)(?:Requirements|Qualifications)[\s:\n]+((?:[-*•].*(?:\n|$))+)",
        description
    )
    if match:
        reqs = [
            line.strip("-*• ").strip()
            for line in match.group(1).splitlines()
            if line.strip()
        ]

    # prefer the applyMethod URL if present
    apply_link = (
        data.get("applyMethod", {}).get("applyUrl")
        or data.get("applyUrl")
        or None
    )

    return {
        "description":  description,
        "requirements": reqs,
        "applyLink":    apply_link,
    }

def _html_detail_fallback(job_id, apply_url):
    """
    Full HTML fallback: description + requirements + applyLink
    """
    if not apply_url:
        return {"description": "", "requirements": [], "applyLink": None}

    try:
        page = requests.get(apply_url, headers=HEADERS, timeout=10)
        page.raise_for_status()
    except Exception as e:
        print(f"Warning: cannot load HTML detail for {job_id}: {e}")
        return {"description": "", "requirements": [], "applyLink": apply_url}

    doc = BeautifulSoup(page.text, "html.parser")

    # description
    desc_div = doc.select_one("div.description__text, div.show-more-less-html__markup")
    description = desc_div.get_text("\n", strip=True) if desc_div else ""

    # requirements bullets
    requirements = []
    for hdr in doc.select("h2, h3"):
        if re.search(r"(Requirements|Qualifications|What you will do|Key Responsibilities|What you bring)", hdr.get_text(), re.I):
            ul = hdr.find_next_sibling("ul")
            if ul:
                requirements = [li.get_text(strip=True) for li in ul.select("li")]
            break
    if not requirements and desc_div:
        first_ul = desc_div.find("ul")
        if first_ul:
            requirements = [li.get_text(strip=True) for li in first_ul.select("li")]

    # apply-button
    apply_btn = doc.select_one("a.apply-button, a.apply__button")
    apply_link = apply_btn["href"] if apply_btn else apply_url

    return {
        "description":  description,
        "requirements": requirements,
        "applyLink":    apply_link,
    }

def _html_apply_fallback(url):
    """
    Only scrape the apply-button href from the HTML page.
    """
    try:
        page = requests.get(url, headers=HEADERS, timeout=10)
        page.raise_for_status()
    except Exception as e:
        return {"applyLink": url}

    doc = BeautifulSoup(page.text, "html.parser")
    btn = doc.select_one("a.apply-button, a.apply__button")
    return {"applyLink": btn["href"] if btn else url}





# def fetch_detail(job_id, apply_url=None):
#     """
#     1) Try the guest-API JSON endpoint.
#     2) If that fails, GET the apply_url page and scrape HTML.
#     """
#     api_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
#     try:
#         resp = requests.get(api_url, headers=HEADERS, timeout=10)
#         resp.raise_for_status()
#         data = resp.json()
#         if not isinstance(data, dict):
#             raise ValueError("not a JSON object")
#     except Exception:
#         # fallback to HTML scrape
#         if not apply_url:
#             return {}
#         try:
#             page = requests.get(apply_url, headers=HEADERS, timeout=10)
#             page.raise_for_status()
#         except Exception as html_err:
#             print(f"Warning: cannot load HTML detail for {job_id}: {html_err}")
#             return {}

#         doc = BeautifulSoup(page.text, "html.parser")
#         desc_div = doc.select_one("div.description__text, div.show-more-less-html__markup")
#         description = desc_div.get_text("\n", strip=True) if desc_div else ""

#         # try all list-headings
#         requirements = []
#         for hdr in doc.select("h2, h3"):
#             txt = hdr.get_text(strip=True)
#             if re.search(r"(Requirements|Qualifications|What you will do|Key Responsibilities|What you bring)", txt, re.I):
#                 ul = hdr.find_next_sibling("ul")
#                 if ul:
#                     requirements = [li.get_text(strip=True) for li in ul.select("li")]
#                     break

#         # if still empty, grab first <ul> in description
#         if not requirements and desc_div:
#             first_ul = desc_div.find("ul")
#             if first_ul:
#                 requirements = [li.get_text(strip=True) for li in first_ul.select("li")]

#         apply_btn = doc.select_one("a.apply-button, a.apply__button")
#         apply_link = apply_btn["href"] if apply_btn else apply_url

#         return {
#             "description":  description,
#             "requirements": requirements,
#             "applyLink":    apply_link,
#         }

#     # JSON path
#     desc_html   = data.get("description", "")
#     # parse description HTML so we can re-scan for bullets
#     doc = BeautifulSoup(desc_html, "html.parser")
#     description = doc.get_text("\n", strip=True)

#     # first try original heading-based regex
#     reqs = []
#     m = re.search(
#         r"(?mi)(?:Requirements|Qualifications)[\s:\n]+((?:[-*•].*(?:\n|$))+)",
#         description
#     )
#     if m:
#         reqs = [ line.strip("-*• ").strip()
#                  for line in m.group(1).splitlines()
#                  if line.strip() ]

#     # if none found, try the same heading list approach
#     if not reqs:
#         for hdr in doc.select("h2, h3"):
#             txt = hdr.get_text(strip=True)
#             if re.search(r"(Requirements|Qualifications|What you will do|Key Responsibilities|What you bring)", txt, re.I):
#                 ul = hdr.find_next_sibling("ul")
#                 if ul:
#                     reqs = [li.get_text(strip=True) for li in ul.select("li")]
#                     break

#     # last-resort: first <ul> in the description
#     if not reqs:
#         first_ul = doc.find("ul")
#         if first_ul:
#             reqs = [li.get_text(strip=True) for li in first_ul.select("li")]

#     apply_link = (
#         data.get("applyMethod", {}).get("applyUrl")
#         or data.get("applyUrl")
#         or None
#     )

#     return {
#         "description":  description,
#         "requirements": reqs,
#         "applyLink":    apply_link,
#     }


def scrape_and_publish():
    """
    Page through the search results, skip any seen jobIds,
    de-dupe identical IDs on each page, fetch their detail,
    send everything to Kafka, and persist state.
    """
    start = 0
    while True:
        jobs = fetch_search(start)
        if not jobs:
            break

        # avoid duplicate cards on same page
        seen_this_page = set()

        for job in jobs:
            jid = job["jobId"]
            if jid in seen or jid in seen_this_page:
                continue
            seen_this_page.add(jid)

            details = fetch_detail(jid, apply_url=job.get("applyUrl"))
            payload = {**job, **details}

            producer.send(TOPIC, payload)
            print(f"Published: {payload['title']} @ {payload['company']}")

            # mark as globally seen
            seen.add(jid)
            with open(STATE_FILE, "w") as f:
                json.dump(list(seen), f)

        start += len(jobs)
        time.sleep(1)  # polite pacing

if __name__ == "__main__":
    print("Starting LinkedIn scraper…")
    while True:
        scrape_and_publish()
        print("Sleeping 10 minutes…")
        time.sleep(600)



# RSS scraper for job postings

# import os
# import time
# import json
# import feedparser
# from datetime import datetime
# from kafka import KafkaProducer, errors

# # Read the broker address from env
# # bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
# bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')


# # Wait until Kafka is reachable
# while True:
#     try:
#         # Attempt to fetch metadata to verify connectivity
#         producer = KafkaProducer(
#             bootstrap_servers=bootstrap,
#             request_timeout_ms=5000,
#             api_version_auto_timeout_ms=5000,
#             value_serializer=lambda v: json.dumps(v).encode('utf-8')
#         )
#         # Force metadata fetch
#         producer.bootstrap_connected()
#         print(f"Connected to Kafka at {bootstrap}")
#         break
#     except errors.NoBrokersAvailable:
#         print(f"Kafka not ready at {bootstrap}, retrying in 2s...")
#         time.sleep(2)

# TOPIC = 'jobs'
# RSS_URL = 'https://www.indeed.com/rss?q=data+engineer&l='
# seen = set()

# def fetch_and_publish():
#     feed = feedparser.parse(RSS_URL)
#     for entry in feed.entries:
#         if entry.link not in seen:
#             seen.add(entry.link)
#             payload = {
#                 'title': entry.title,
#                 'company': entry.get('author', ''),
#                 'link': entry.link,
#                 'published': entry.published,
#                 'fetched_at': datetime.utcnow().isoformat()
#             }
#             producer.send(TOPIC, payload)
#             print(f"Published: {entry.title}")
#     producer.flush()

# if __name__ == '__main__':
#     print("Starting job-scraper…")
#     while True:
#         try:
#             fetch_and_publish()
#         except Exception as e:
#             print("Error during fetch:", e)
#         time.sleep(300)
