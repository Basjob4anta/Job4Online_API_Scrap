# import requests
from curl_cffi import requests
from bs4 import BeautifulSoup
import time
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import sys
import re
# Load environment variables
load_dotenv()

sess = requests.Session()
sess.impersonate = 'chrome'

headers = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://www.google.com/',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}

def clean_text(text):
    if not isinstance(text, str):
        return text
    # Remove invalid surrogate characters
    return re.sub(r'[\ud800-\udfff]', '', text)

# 🔹 MongoDB Setup
def get_db():
    """Connect to MongoDB and return the database."""
    try:
       
        client = MongoClient(os.getenv("MONGO_URI"), serverSelectionTimeoutMS=5000)
        client.server_info()  # Test the connection
        db = client.joblistings  # Replace with your database name
        print("✅ Connected to MongoDB")
        return db
    except Exception as e:
        print("❌ Failed to connect to MongoDB:", e)
        return None

def get_existing_job_ids():
    """Fetch existing Job IDs from MongoDB to prevent duplicates."""
    db = get_db()
    if db is None:
        return set()
    
    collection = db.jobs  # Replace with your collection name
    try:
        existing_job_ids = set(doc["id"] for doc in collection.find({}, {"id": 1}))
        return existing_job_ids
    except Exception as e:
        print("⚠️ Error fetching existing job IDs:", e)
        return set()

def get_job_description(job_url):
    """Fetch job description from the job listing page."""
    if not job_url:
        return "No job URL provided"

    headers = {"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1"}
    response = sess.get(job_url)
    
    if response.status_code != 200:
        print(f"⚠️ Failed to fetch job description: {job_url}")
        return "Failed to retrieve description"
    
    soup = BeautifulSoup(response.text, 'html.parser')
    job_detail = soup.select_one('[data-automation="jobAdDetails"]')

    work_type = soup.select_one('[data-automation="job-detail-work-type"]')

    return {
        "job_detail": job_detail.text.strip() if job_detail else "No description available",
        "work_type": work_type.text.strip() if work_type else "No work type available"
    }

def get_job_listings(search_keyword):
    """Scrape job listings from Seek and return as a list."""
    base_url = f"https://www.seek.com.au/{search_keyword.replace(' ', '-')}-jobs"
    headers = {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
        # "Cookie": "sol_id=cb1758f8-471c-44f2-ab1b-fbab7cf1efac; JobseekerSessionId=06b7f458-1310-4510-9aa3-3eb70789742d; JobseekerVisitorId=06b7f458-1310-4510-9aa3-3eb70789742d; __cf_bm=NXp3JMmc9luMKSxaudYIKOnLNkp2jpl6zkWTxmE3Ok4-1743872296-1.0.1.1-FrnIke8k1qRHLc6ZkQfgrxq6Ziwq0Ktgn7whqPF1MRnA_Ha1p3KAVHdeyo4xE7cSMdSBOmL5d_Kc3lgmkN91Wub_CL3sX0UoDIgmnpG7Yng; _cfuvid=YIbFnRQ3e.nxBTDYf3tTKiv_nAa6zKGrH_HGophySUQ-1743872296129-0.0.1.1-604800000; main=V%7C2~P%7Cjobsearch~K%7CHospitality~WID%7C3000~OSF%7Cquick&set=1743872414703; da_cdt=visid_019606e555e9000dc9f64ebf7b5f0506f00280670093c-sesid_1743872415210-hbvid_cb1758f8_471c_44f2_ab1b_fbab7cf1efac-tempAcqSessionId_1743872414689-tempAcqVisitorId_cb1758f8471c44f2ab1bfbab7cf1efac; da_anz_candi_sid=1743872415210; da_searchTerm=undefined; utag_main=v_id:019606e555e9000dc9f64ebf7b5f0506f00280670093c$_sn:1$_se:1%3Bexp-session$_ss:1%3Bexp-session$_st:1743874215210%3Bexp-session$ses_id:1743872415210%3Bexp-session$_pn:1%3Bexp-session$_prevpage:search%20results%3Bexp-1743876015400; hubble_temp_acq_session=id%3A1743872414689_end%3A1743874215404_sent%3A3; _dd_s=rum=2&id=256a9442-f146-4cb4-8050-eac15c0f5fda&created=1743872414677&expire=1743873314682&logs=0"
    }
    
    response = sess.get(base_url)

    if response.status_code != 200:
        print(f"⚠️ Failed to fetch {base_url} got status code {response.status_code}")
        return []
    
    soup = BeautifulSoup(response.text, 'html.parser')
    total_jobs_text = soup.select_one('[data-automation="totalJobsCount"]')
    total_jobs = int(total_jobs_text.text.replace(',', '')) if total_jobs_text else 0
    print(f"✅ Total jobs found: {total_jobs} for '{search_keyword}'")

    jobs = []
    total_pages = min((total_jobs // 22) + 1, 2)  # Limit to first 5 pages
    existing_job_ids = get_existing_job_ids()

    for page in range(1, total_pages + 1):
        print(f"🔄 Scraping page {page} of {total_pages} for '{search_keyword}'")
        page_url = f"{base_url}?page={page}" if page > 1 else base_url
        response = sess.get(page_url)
        
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch {page_url}")
            continue
        
        soup = BeautifulSoup(response.text, 'html.parser')
        job_elements = soup.find_all('article')
        for job in job_elements:
            id = job.get('data-job-id', "").strip()  # Ensure it's a string
            if not id or id in existing_job_ids:  # Skip duplicates
                continue
            
            
            title = job.get('aria-label', "").strip()
            jobLocation = job.select_one('[data-automation="jobLocation"]')
            employer = job.select_one('a[data-automation="jobCompany"]')
            
            salary = job.select_one('[data-automation="jobSalary"]')
            date_posted = job.select_one('[data-automation="jobListingDate"]')
            job_link = job.select_one('a[data-automation="jobTitle"]')
            
            job_url = f"https://www.seek.com.au{job_link['href']}" if job_link else None
            job_detail = get_job_description(job_url) if job_url else "No job URL"
            
            job_description =  job_detail["job_detail"] 
            work_type = job_detail["work_type"] 

            job_data = {
                "id": id,
                "search_keyword": clean_text(search_keyword),
                "title": clean_text(title),
                "jobLocation": clean_text(jobLocation.text.strip() if jobLocation else ""),
                "employer": clean_text(employer.text.strip() if employer else ""),
                "work_type": clean_text(work_type),
                "salary": clean_text(salary.text.strip() if salary else ""),
                "date_posted": clean_text(date_posted.text.strip() if date_posted else ""),
                "job_description": clean_text(job_description),
                "job_url": clean_text(job_url if job_url else "No URL")
            }
            jobs.append(job_data)
            
        time.sleep(1)  # Prevents blocking by Seek
        
    return jobs

def upload_to_mongodb(jobs):
    """Upload job listings to MongoDB without duplicates."""
    try:
        if not jobs:
            print("⚠️ No new jobs to upload.")
            return
        
        db = get_db()
        if db is None:
            return
        
        collection = db.jobs  # Replace with your collection name
        existing_job_ids = get_existing_job_ids()
        new_data = [job for job in jobs if job["id"] not in existing_job_ids]  # Avoid duplicates

        if new_data:
            print(f"✅ Uploading {len(new_data)} new job listings to MongoDB...")
            collection.insert_many(new_data)  # Insert new documents
            print(f"✅ Successfully uploaded {len(new_data)} new job listings.")
        else:
            print("⚠️ No new jobs to update.")
    
    except Exception as e:
        print("❌ Error uploading data to MongoDB:", e)

if __name__ == '__main__':
    search_keywords = [
        "Accountant",
    ]
    
    all_jobs = []
    for keyword in search_keywords:
        print(f"🔍 Searching for jobs: {keyword}")
        job_listings = get_job_listings(keyword)
        all_jobs.extend(job_listings)
        time.sleep(2)  # Prevent excessive requests
    
    # ✅ Upload to MongoDB
    upload_to_mongodb(all_jobs)
    
    print("🎉 Job listings scraping and uploading completed!")