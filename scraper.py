from playwright.sync_api import sync_playwright
from datetime import datetime
import pytz
import os
import pandas as pd
from glob import glob
from tqdm import tqdm
from bs4 import BeautifulSoup
from time import sleep

# Configuration
default_page_limit = 40  # Default number of pages to scrape
# Set timezone to Eastern Time
eastern = pytz.timezone('America/New_York')
# Get the current date in Eastern Time
current_time_et = datetime.now(eastern).strftime('%Y%m%d_%H')
folder = f"job_listings_{current_time_et}"  # Folder name with the current date in ET

# Create directories
os.makedirs('data', exist_ok=True)
os.makedirs(os.path.join('data', 'html'), exist_ok=True)
os.makedirs(os.path.join('data', 'html', folder), exist_ok=True)

# Scrape opsjobs with playwright
def scraper(posting_type, page_limit):
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(no_viewport=True, ignore_https_errors=True)
        page = context.new_page()
        page.goto("https://www.gojobs.gov.on.ca/employees/")
        
        # Check the checkbox based on posting_type
        if posting_type == "TDA":
            page.get_by_label("Yes").check()  # Extract TDA eligible postings
        
        # Perform the search
        page.get_by_role("button", name="Search", exact=True).click()
    
        current_page = 1
    
        while current_page <= page_limit:
            print(f"Scraping page {current_page}...")
            page.wait_for_load_state('networkidle')
    
            # Save the current page's HTML content
            with open(os.path.join('data', 'html', folder, f"{posting_type}_{str(current_page).zfill(3)}.html"), 'w', encoding='utf-8') as f:
                f.write(page.content())
    
            # Navigate to the next page
            try:
                page.get_by_role("link", name="Next").click()
                current_page += 1
            except Exception:
                print('No more pages or error occurred.')
                break
            sleep(5)
            page.wait_for_load_state('networkidle')
    
        browser.close()

# Get the posting type and page limit from environment variables
posting_type = os.getenv("POSTING_TYPE", "Open")  # Default to "Open" if not set
page_limit = int(os.getenv("PAGE_LIMIT", default_page_limit))  # Default to default_page_limit if not set

scraper(posting_type, page_limit)

# Extract job information from saved HTML files
files = glob(os.path.join('data', 'html', folder, '*.html'))

output_df = []
for file_path in tqdm(files):
    with open(file_path, 'rb') as file:
        html_content = file.read()
    
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    links = soup.find_all('a', target='_self')
    ids = [a['href'].split('JobID=')[1] for a in links if 'JobID=' in a['href']]
    texts = [link.text.strip() for link in links]

    # Generate ID and Title dataframe
    df_ID_TITLE = pd.DataFrame({
        'Job ID': ids,
        'Job Title': texts
    })

    # Capture fields and values into a separate dataframe
    trs = [t for t in soup.find_all('tr')]
    dfs = []
    for tr in trs:
        fields = [div.text.strip().replace(':','') for div in tr.find_all('div',class_="col-sm-3 col-form-label")]
        values = [div.text.strip() for div in tr.find_all('div',class_="col-sm-9 JobAdAlignRight")]
        idf = pd.DataFrame([values], columns=fields)
        dfs.append(idf)
    dfs = pd.concat(dfs).reset_index(drop=True)

    # Merge the two dataframe into output_df
    output_dfi = df_ID_TITLE.drop_duplicates(subset='Job ID').reset_index(drop=True).join(dfs)
    output_df.append(output_dfi)

output_df = pd.concat(output_df).reset_index(drop=True)
output_df.to_csv(os.path.join("data", f"{folder}_{posting_type}.csv"), index=False)
print(f"Saved {len(output_df)} unique job listings to CSV.")
