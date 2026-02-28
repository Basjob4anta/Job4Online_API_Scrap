import requests
from bs4 import BeautifulSoup
import time
from urllib.parse import urljoin, quote
from datetime import datetime, timedelta
import random
import re
import concurrent.futures
import json

class JoraEmailScraper:
    def __init__(self):
        self.jobs_with_emails = []
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
        ]
        self.session = requests.Session()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

    def get_random_headers(self):
        return {
            "User-Agent": random.choice(self.user_agents),
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
        }

    def print_status(self, message):
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] {message}")

    def extract_emails_from_html(self, html):
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text()
        email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
        return list(set(re.findall(email_pattern, text)))

    def get_job_description(self, job_url):
        try:
            response = self.session.get(job_url, headers=self.get_random_headers(), timeout=10)
            if response.status_code != 200:
                return None

            soup = BeautifulSoup(response.text, 'html.parser')
            description_html = ""
            description_elements = soup.select('div#job-description-container, div.description, div.job-details')
            if description_elements:
                description_html = description_elements[0].decode_contents().strip()

            emails = self.extract_emails_from_html(description_html)
            if not emails:
                return None  # Skip if no email

            employer = soup.select_one('span.company, div.company-name, a.company, span.employer-name')
            employer = employer.get_text(strip=True) if employer else "Employer not specified"

            summary = BeautifulSoup(description_html, 'html.parser').get_text().strip()
            summary = summary.split('.')[0] + '.' if '.' in summary else summary[:200] + '...'

            date_posted = datetime.now().strftime('%Y-%m-%d')

            return {
                "description_html": description_html,
                "summary": summary,
                "employer": employer,
                "date_posted": date_posted,
                "emails": emails
            }

        except Exception as e:
            self.print_status(f"Error fetching job description: {e}")
            return None

    def process_job_listing(self, job, keyword, location):
        try:
            title_element = job.select_one('h3.job-title, a.job-title, h2.title, a.job-link, div.job-title')
            if not title_element:
                return
            title = title_element.get_text(strip=True)
            job_url = urljoin("https://au.jora.com", title_element.get('href', ''))

            job_detail = self.get_job_description(job_url)
            if not job_detail:
                return  # Skip jobs without emails

            job_data = {
                "search_keyword": keyword,
                "title": title,
                "jobLocation": location,
                "employer": job_detail["employer"],
                "date_posted": job_detail["date_posted"],
                "job_summary": job_detail["summary"],
                "job_description_html": job_detail["description_html"],
                "emails": job_detail["emails"],
                "job_url": job_url,
                "source": "Jora"
            }
            self.jobs_with_emails.append(job_data)
        except Exception as e:
            self.print_status(f"Error processing job listing: {e}")

    def scrape_page(self, url, keyword, location, page):
        try:
            self.print_status(f"Scraping page {page} for {keyword} in {location}")
            response = self.session.get(url, headers=self.get_random_headers(), timeout=10)
            if response.status_code != 200:
                return False
            soup = BeautifulSoup(response.text, 'html.parser')
            job_elements = soup.select('div.result, div.job-card, article.job-result, div.job-item, div.job')
            if not job_elements:
                return False
            futures = [self.executor.submit(self.process_job_listing, job, keyword, location) for job in job_elements]
            concurrent.futures.wait(futures)
            return True
        except Exception as e:
            self.print_status(f"Error scraping page: {e}")
            return False

    def scrape_jobs(self, search_keyword, location="Australia", max_pages=9):
        encoded_keyword = quote(search_keyword)
        encoded_location = quote(location)
        base_url = f"https://au.jora.com/j?q={encoded_keyword}&l={encoded_location}"
        for page in range(1, max_pages + 1):
            page_url = f"{base_url}&p={page}"
            if not self.scrape_page(page_url, search_keyword, location, page):
                break
            time.sleep(random.uniform(1, 2))

    def save_jobs_with_emails(self, filename="jobs_with_emails.json"):
        if not self.jobs_with_emails:
            self.print_status("No jobs with emails to save.")
            return
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.jobs_with_emails, f, ensure_ascii=False, indent=4)
            self.print_status(f"Saved {len(self.jobs_with_emails)} jobs with emails to '{filename}'")
        except Exception as e:
            self.print_status(f"Error saving to JSON: {e}")

    def __del__(self):
        self.executor.shutdown(wait=False)
        self.session.close()

# Run the scraper
if __name__ == "__main__":
    scraper = JoraEmailScraper()
    search_keywords = ["Chef", "Waiter", "Barista", "Bartender", "Cook", "Kitchen Hand", "Dishwasher"]
    locations = ["Australia", "Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide", "Canberra"]
    for keyword in search_keywords:
        for location in locations:
            scraper.scrape_jobs(keyword, location, max_pages=3)
    scraper.save_jobs_with_emails("jobs_with_emails.json")
