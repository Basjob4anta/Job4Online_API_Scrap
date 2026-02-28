from fastapi import FastAPI, HTTPException, Depends
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from typing import List, Optional, Any
from pydantic import BaseModel

# Load environment variables
load_dotenv()

print("Loading environment variables...")
MONGO_URI = os.getenv("MONGO_URI")
API_KEY = os.getenv("API_KEY")
print("MONGO_URI:", MONGO_URI)
print("API_KEY:", API_KEY)

# MongoDB connection
try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    client.admin.command("ping")
    print("✅ MongoDB connection successful.")
    db = client["joblistings"]
    collection = db["jobs"]
except Exception as e:
    print("❌ MongoDB connection failed:", e)
    raise e

# FastAPI app
app = FastAPI(
    title="Job Listings API (Jora Data)",
    description="API for accessing Jora job listings with API key authentication",
    version="1.0"
)

# Pydantic model matching Jora scraper schema
class JobListing(BaseModel):
    id: str
    search_keyword: str
    title: str
    jobLocation: str
    employer: Optional[str]
    work_type: Optional[str]
    salary: Optional[str]
    min_salary: Optional[Any]
    max_salary: Optional[Any]
    payable_duration: Optional[str]
    date_posted: Optional[str]
    job_summary: Optional[str]
    job_description_html: Optional[str]
    job_url: Optional[str]
    apply_url: Optional[str]
    source: Optional[str]

def verify_api_key(api_key: str):
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    return api_key

@app.get("/", tags=["Health Check"])
async def root():
    return {"message": "Welcome to the Job Listings API for Jora! Please use an API Key to access job listings."}

@app.get("/jobs", response_model=List[JobListing], tags=["Job Listings"])
async def get_jobs(api_key: str = Depends(verify_api_key)):
    try:
        # Ensure every document has all expected fields (fill missing ones with None)
        expected_fields = [
            "id", "search_keyword", "title", "jobLocation", "employer",
            "work_type", "salary", "min_salary", "max_salary", "payable_duration",
            "date_posted", "job_summary", "job_description_html", "job_url",
            "apply_url", "source"
        ]

        jobs_raw = list(collection.find({}, {"_id": 0}))
        jobs_clean = []

        for job in jobs_raw:
            for field in expected_fields:
                if field not in job:
                    job[field] = None
            jobs_clean.append(job)

        return jobs_clean

    except Exception as e:
        print("❌ Error fetching jobs from MongoDB:", e)
        raise HTTPException(status_code=500, detail="Failed to fetch job listings.")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
