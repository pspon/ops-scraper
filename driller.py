#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from datetime import datetime
import pytz

# Set the timezone to Eastern Time and get the current date
eastern = pytz.timezone('America/New_York')
eastern_date = datetime.now(eastern).strftime('%Y%m%d')
eastern_date_hour = datetime.now(eastern).strftime('%Y%m%d_%H')

# Load all relevant CSV files for today's job listings
csv_files = sorted([f for f in os.listdir('data') if f.startswith('job_listings_' + eastern_date)])
df = pd.concat([pd.read_csv(os.path.join('data', file)) for file in csv_files]).drop_duplicates(subset='Job ID').reset_index(drop=True)

# Ensure the DataFrame contains the 'Job ID' column
if 'Job ID' not in df.columns:
    raise ValueError("CSV must contain a 'Job ID' column.")

def scrape_job_details_v4(job_id):
    """
    Scrapes detailed information for a given job ID from the job posting and description pages.
    """
    posting_url = f"https://www.gojobs.gov.on.ca/employees/Preview.aspx?JobID={job_id}"
    description_url = f"https://www.gojobs.gov.on.ca/employees/PDR.aspx?JobID={job_id}"
    
    details = {}

    def fetch_and_parse(url):
        """
        Fetches HTML content from the provided URL and parses it into a BeautifulSoup object.
        Implements retry logic for handling HTTP 429 errors (Too Many Requests).
        """
        max_retries = 7
        backoff_factor = 1  # Initial wait time for retries

        for attempt in range(max_retries):
            try:
                response = requests.get(url)
                response.raise_for_status()
                return BeautifulSoup(response.text, 'html.parser')
            except requests.exceptions.HTTPError as http_err:
                if response.status_code == 429:
                    wait_time = backoff_factor * (2 ** attempt)  # Exponential backoff for retries
                    print(f"429 error: Too Many Requests. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"HTTP error occurred: {http_err}")
                    return None
            except Exception as err:
                print(f"An error occurred: {err}")
                return None
        print("Max retries reached. Exiting.")
        return None
    
    # Scrape job posting details
    posting_soup = fetch_and_parse(posting_url)
    if posting_soup is None:
        return None  # Return None if there was an error fetching the posting
    
    # Extract relevant job details
    try:
        details['Position Title'] = posting_soup.find('h1').get_text(strip=True)
        details['Job Description'] = posting_soup.find('div', class_='row JobAdSpace').get_text(separator="\n", strip=True)
        details['Organization'] = posting_soup.find(string="Organization:").find_next().text.strip()
        details['Division'] = posting_soup.find(string="Division:").find_next().text.strip()
        details['City'] = posting_soup.find(string="City:").find_next().text.strip()
        details['Language of Position(s)'] = posting_soup.find(string="Language of Position(s):").find_next().text.strip()
        details['Job Term'] = posting_soup.find(string="Job Term:").find_next().text.strip()
        details['Job Code'] = posting_soup.find(string="Job Code:").find_next().text.strip()
        details['Salary'] = posting_soup.find(string="Salary:").find_next().text.strip()
        details['Posting Status'] = posting_soup.find(string="Posting Status:").find_next().text.strip()
        details['Job ID'] = posting_soup.find(string="Job ID:").find_next().text.strip()
        details['Address'] = posting_soup.find(string="Address:").find_next().text.strip()
        details['Compensation Group'] = posting_soup.find(string="Compensation Group:").find_next().text.strip()
        details['Schedule'] = posting_soup.find(string="Schedule:").find_next().text.strip()
        details['Category'] = posting_soup.find(string="Category:").find_next().text.strip()
        details['Posted on'] = posting_soup.find(string="Posted on:").find_next().text.strip()
        details['Note'] = posting_soup.find(string="Note:").find_next().text.strip() if posting_soup.find(string="Note:") else ""
    except AttributeError as e:
        print(f"Error parsing job posting details: {e}")
        return None
    
    # Scrape job description details
    description_soup = fetch_and_parse(description_url)
    if description_soup is None:
        return details  # Return details collected so far if fetching the description failed
    
    # Extract additional job details from the description
    try:
        details['Purpose of Position'] = description_soup.find_all('h2')[0].find_next('p').text.strip()
        details['Duties and Responsibility'] = description_soup.find_all('h2')[1].find_next('p').text.strip()
        details['Staffing & Licensing'] = description_soup.find_all('h2')[2].find_next('p').text.strip()
        details['Knowledge'] = description_soup.find_all('h2')[3].find_next('p').text.strip()
        details['Skills'] = description_soup.find_all('h2')[4].find_next('p').text.strip()
        details['Freedom of Action'] = description_soup.find_all('h2')[5].find_next('p').text.strip()
    except (AttributeError, IndexError) as e:
        print(f"Error parsing job description details: {e}")  # Handle missing sections

    return details

def scrape_job(job_id):
    """
    Wrapper function to scrape job details for a single job ID.
    """
    return scrape_job_details_v4(job_id)

# List to store scraped job details and failed job IDs
job_details = []
failed_job_ids = []

# Use ThreadPoolExecutor to scrape job details concurrently
with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust the number of workers as needed
    futures = {executor.submit(scrape_job, job_id): job_id for job_id in df['Job ID']}
    
    # Process each completed future
    for future in tqdm(as_completed(futures), total=len(futures)):
        job_id = futures[future]
        try:
            details = future.result()
            if details:
                job_details.append(details)
        except Exception as e:
            print(f"Error scraping job ID {job_id}: {e}")
            failed_job_ids.append(job_id)

# Summary of scraped results
print(f"Scraped details for {len(job_details)} jobs.")
print(f"Failed to scrape {len(failed_job_ids)} jobs: {failed_job_ids}")

# Create a DataFrame from the scraped job details
job_details_df = pd.DataFrame(job_details)

# Convert 'Job ID' to int64 for both DataFrames for consistency
df['Job ID'] = df['Job ID'].astype('int64')
job_details_df['Job ID'] = job_details_df['Job ID'].astype('int64')

# Identify missing job IDs that were not found during scraping
missing_job_ids = df[~df['Job ID'].isin(job_details_df['Job ID'])]['Job ID']

# Create the 'data/jobs' directory if it doesn't exist
output_dir = 'data/jobs'
os.makedirs(output_dir, exist_ok=True)

# Save missing job IDs to a text file only if there are any
if not missing_job_ids.empty:
    missing_job_ids_file = os.path.join(output_dir, f"{eastern_date}_missing_job_ids.txt")
    with open(missing_job_ids_file, 'w') as f:
        for job_id in missing_job_ids:
            f.write(f"{job_id}\n")
    
    print(f"Missing job IDs saved to {missing_job_ids_file}.")
else:
    print("No missing job IDs to save.")

# Save the results to a new CSV file in the specified directory
output_file = os.path.join(output_dir, f"{eastern_date_hour}_scraped_jobs.csv")
output_df = pd.concat([df.set_index("Job ID"), job_details_df.set_index("Job ID")], axis=1).reset_index()
output_df.to_csv(output_file, index=False)

print(f"Scraping completed. Results saved to {output_file}.")
