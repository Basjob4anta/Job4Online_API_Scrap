import requests
from bs4 import BeautifulSoup
import time
from urllib.parse import urljoin, quote, urlparse, parse_qs, unquote
from datetime import datetime, timedelta
import json
import random
import re
import concurrent.futures

class JoraScraper:
    def __init__(self):
        self.jobs = []
        self.output_file = "jorajobs.json"
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
        ]
        self.session = requests.Session()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
        self.total_jobs_scraped = 0

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

    def extract_salary_from_listing(self, soup):
        badges = soup.find_all('div', class_='badge -default-badge')
        for badge in badges:
            content = badge.find('div', class_='content')
            if content and '$' in content.get_text():
                return content.get_text(strip=True)
        return "Salary not specified"

    def process_job_listing(self, job, search_keyword, location):
        try:
            title_element = job.select_one('h3.job-title, a.job-title, h2.title, a.job-link, div.job-title')
            if not title_element:
                return None
            title = title_element.get_text(strip=True)
            job_url = title_element.get('href')
            if not job_url:
                return None
            job_url = urljoin('https://au.jora.com', job_url)
            job_id = f"{hash(job_url)}"
            if 'job' in job_url.lower():
                job_id = job_url.split('/')[-1].split('?')[0].split('-')[-1]

            job_detail = self.get_job_description(job_url)

            job_data = {
                "id": job_id,
                "search_keyword": search_keyword,
                "title": title,
                "jobLocation": location,
                "employer": job_detail["employer"],
                "work_type": job_detail["work_type"],
                "salary": job_detail["salary"],
                "date_posted": job_detail["date_posted"],
                "job_description": job_detail["job_detail"],  # HTML format here
                "job_url": job_url if job_url else "No URL",
                "apply_url": job_detail["apply_url"],
                "source": "Jora Australia"
            }
            self.total_jobs_scraped += 1
            return job_data
        except Exception as e:
            self.print_status(f"Error processing job: {str(e)}")
            return None

    def scrape_page(self, url, search_keyword, location, page_num):
        try:
            self.print_status(f"Scraping page {page_num} for '{search_keyword}' in {location}")
            response = self.session.get(url, headers=self.get_random_headers(), timeout=10)
            if response.status_code != 200:
                self.print_status(f"Failed to fetch page {page_num} (Status {response.status_code})")
                return False

            soup = BeautifulSoup(response.text, 'html.parser')
            if soup.select_one('div.no-results, div.empty-state, div.no-jobs-found'):
                self.print_status("No more jobs found")
                return False

            job_elements = soup.select('div.result, div.job-card, article.job-result, div.job-item, div.job')
            if not job_elements:
                self.print_status("No job listings found on page")
                return False

            futures = []
            for job in job_elements:
                futures.append(self.executor.submit(self.process_job_listing, job, search_keyword, location))

            page_jobs = 0
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    self.jobs.append(result)
                    page_jobs += 1

            self.print_status(f"Found {page_jobs} jobs on page {page_num}")
            return True
        except Exception as e:
            self.print_status(f"Error scraping page: {str(e)}")
            return False

    def scrape_jobs(self, search_keyword, location="Australia", max_pages=3):
        encoded_keyword = quote(search_keyword)
        encoded_location = quote(location)
        base_url = f"https://au.jora.com/j?q={encoded_keyword}&l={encoded_location}"
        page = 1
        self.print_status(f"\nStarting scrape for '{search_keyword}' in {location}")
        while page <= max_pages:
            url = f"{base_url}&p={page}"
            if not self.scrape_page(url, search_keyword, location, page):
                break
            page += 1
            time.sleep(random.uniform(1, 2))
        self.print_status(f"Completed scrape for '{search_keyword}' in {location}")

    def get_job_description(self, job_url):
        try:
            response = self.session.get(job_url, headers=self.get_random_headers(), timeout=10)
            if response.status_code != 200:
                return self.default_job_details()

            soup = BeautifulSoup(response.text, 'html.parser')

            employer = "Employer not specified"
            employer_elements = soup.select('span.company, div.company-name, a.company, span.employer-name')
            if employer_elements:
                employer = employer_elements[0].get_text(strip=True)

            salary = None
            work_type = None

            badges = soup.find_all('div', class_='badge -default-badge')
            for badge in badges:
                content = badge.find('div', class_='content')
                if not content:
                    continue

                text = content.get_text(strip=True)
                if '$' in text:
                    salary = text
                elif any(word.lower() in text.lower() for word in ["full", "part", "contract", "temporary", "casual", "permanent"]):
                    work_type = text

            date_posted = datetime.now().strftime('%Y-%m-%d')
            date_elements = soup.select('time.date, span.date-posted, div.posted-date')
            if date_elements:
                date_text = date_elements[0].get_text(strip=True)
                date_posted = self.parse_posted_date(date_text)

            description = "No description available"
            description_elements = soup.select('div#job-description-container, div.description, div.job-details')
            if description_elements:
                description = str(description_elements[0])  # <-- HTML format

            apply_link = None
            apply_button = soup.select_one('a[data-automation="job-detail-apply-button"]')
            if apply_button and apply_button.has_attr('href'):
                apply_link = apply_button['href']
            else:
                fallback_links = soup.find_all('a', string=re.compile(r'apply', re.I))
                for link in fallback_links:
                    if link and link.has_attr('href'):
                        apply_link = link['href']
                        break

            final_url = job_url
            if apply_link:
                if apply_link.startswith("/users/sign_in"):
                    parsed = urlparse(apply_link)
                    params = parse_qs(parsed.query)
                    if 'return_to' in params:
                        inner_path = unquote(params['return_to'][0])
                        final_url = urljoin("https://au.jora.com", inner_path)
                    else:
                        final_url = urljoin("https://au.jora.com", apply_link)
                else:
                    final_url = urljoin("https://au.jora.com", apply_link)

                try:
                    r = self.session.get(final_url, headers=self.get_random_headers(), allow_redirects=True, timeout=10)
                    if r.status_code == 200:
                        final_url = r.url
                except Exception as e:
                    self.print_status(f"Error resolving final apply URL: {e}")

            return {
                "job_detail": description,
                "work_type": work_type,
                "salary": salary if salary else "Salary not specified",
                "employer": employer,
                "date_posted": date_posted,
                "apply_url": final_url
            }
        except Exception as e:
            self.print_status(f"Error getting job details: {str(e)}")
            return self.default_job_details()

    def parse_posted_date(self, text):
        text = text.lower()
        now = datetime.now()
        if 'hour' in text or 'hr' in text:
            num = int(re.search(r'\d+', text).group())
            return (now - timedelta(hours=num)).strftime('%Y-%m-%d')
        elif 'day' in text:
            num = int(re.search(r'\d+', text).group())
            return (now - timedelta(days=num)).strftime('%Y-%m-%d')
        elif 'week' in text:
            num = int(re.search(r'\d+', text).group())
            return (now - timedelta(weeks=num)).strftime('%Y-%m-%d')
        elif 'month' in text:
            num = int(re.search(r'\d+', text).group())
            return (now - timedelta(days=num*30)).strftime('%Y-%m-%d')
        return now.strftime('%Y-%m-%d')

    def default_job_details(self):
        return {
            "job_detail": "No description available",
            "work_type": "Employment type not specified",
            "salary": "Salary not specified",
            "employer": "Employer not specified",
            "date_posted": datetime.now().strftime('%Y-%m-%d'),
            "apply_url": ""
        }

    def save_to_json(self):
        if not self.jobs:
            self.print_status("No jobs to save")
            return
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(self.jobs, f, ensure_ascii=False, indent=2)
            self.print_status(f"Successfully saved {len(self.jobs)} jobs to {self.output_file}")
        except Exception as e:
            self.print_status(f"Failed to save jobs: {str(e)}")

    def __del__(self):
        self.executor.shutdown(wait=False)
        self.session.close()


if __name__ == "__main__":
    scraper = JoraScraper()
    search_keywords = ["Data Entry Clerk"]
    locations = ["Sydney NSW"]
    start_time = time.time()
    for keyword in search_keywords:
        for location in locations:
            scraper.scrape_jobs(keyword, location, max_pages=3)
            time.sleep(random.uniform(1, 2))
    scraper.save_to_json()
    elapsed_time = time.time() - start_time
    scraper.print_status(f"\nTotal scraping time: {elapsed_time:.2f} seconds")
