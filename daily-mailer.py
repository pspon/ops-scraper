import pandas as pd
import requests
import numpy as np
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import pytz
import os

# Function to get CSV filenames dynamically from a GitHub repository
def get_csv_filenames():
    url = "https://api.github.com/repos/pspon/ops-scraper/contents/data/jobs"
    response = requests.get(url)
    if response.status_code != 200:
        print("Failed to load GitHub directory.")
        return []

    files = response.json()
    # Filter to get only CSV files
    csv_files = [file['download_url'] for file in files if file['name'].endswith('scraped_jobs.csv')]
    return csv_files

# Function to download and merge CSV files
def load_data():
    csv_files = get_csv_filenames()
    all_data = []

    for url in csv_files:
        response = requests.get(url)
        if response.status_code == 200:
            df = pd.read_csv(url)
            all_data.append(df)
        else:
            print(f"Failed to load {url}")

    # Combine data and drop duplicates based on 'Job ID'
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        combined_data = combined_data.drop_duplicates(subset='Job ID').reset_index(drop=True)
        return combined_data
    else:
        print("No data available to display.")
        return pd.DataFrame()

# Function to create a styled email body with job postings
def create_styled_email_body_v5(job_ids, df):
    color_palette = [
        "#FFDDC1", "#CFE2F3", "#D9EAD3", "#F9CB9C", "#D9BFD8", "#F6B5A0"
    ]

    email_body = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; margin: 0; padding: 20px; background-color: #f4f4f4; }}
            h2 {{ color: #333; }}
            p {{ margin: 10px 0; padding: 10px; background-color: #fff; border-radius: 5px; box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1); }}
            strong {{ color: #0056b3; }}
            footer {{ margin-top: 20px; font-size: small; color: gray; text-align: center; }}
            .container {{ max-width: 800px; margin: auto; background: #fff; border-radius: 10px; padding: 20px; box-shadow: 0 4px 10px rgba(0, 0, 0, 0.1); }}
            .job {{ margin-bottom: 20px; border-radius: 5px; padding: 15px; }}
            .job a {{ text-decoration: none; color: #0056b3; }}
        </style>
    </head>
    <body>
        <div class="container" id="top">
            <div class="toc">
                <h3>Jobs Postings</h3>
                <ul>
    """

    # Generate the table of contents for job postings
    for job_id in job_ids:
        selected_job = df[df['Job ID'] == job_id]
        if not selected_job.empty:
            job_title = selected_job.iloc[0]['Job Title']
            email_body += f'<li><a href="https://www.gojobs.gov.on.ca/employees/Preview.aspx?JobID={job_id}">{job_title}</a></li>'

    email_body += """
                </ul>
            </div>
    """

    # Append details of each job posting
    for index, job_id in enumerate(job_ids):
        selected_job = df[df['Job ID'] == job_id]
        if not selected_job.empty:
            job = selected_job.iloc[0]
            color = color_palette[index % len(color_palette)]
            email_body += f"""
                <div class="job" style="background-color: {color};">
                    <p><strong>Closing Date:</strong> {job['Closing Date']}</p>
                    <p><strong>Job Title:</strong> {job['Job Title']}</p>
                    <p><strong>Job ID:</strong> {job['Job ID']}</p>
                    <p><strong>Salary:</strong> ${job['Adjusted Minimum Salary']:,.2f} - ${job['Adjusted Maximum Salary']:,.2f}</p>
                    <p><strong>Compensation Group:</strong> {job['Compensation Group']}</p>
                    <p><strong>Job Term:</strong> {job['Job Term']}</p>
                    <p><strong>Job Code:</strong> {job['Job Code']}</p>
                    <p><strong>Category:</strong> {job['Category']}</p>
                    <p><strong>Location:</strong> {job['Location']}</p>
                    <p><strong>Organization:</strong> {job['Organization']}</p>
                    <p><strong>Division:</strong> {job['Division']}</p>
                    <p><strong>Address:</strong> {job['Address']}</p>
                    <p><a href="https://www.gojobs.gov.on.ca/employees/Preview.aspx?JobID={job['Job ID']}">Job Description</a> | <a href="https://www.gojobs.gov.on.ca/employees/PDR.aspx?JobID={job['Job ID']}">Job Specification</a></p>
                    <hr>
                </div>
            """

    email_body += """
            </div>
            <footer>
                <p>These job opportunities are brought to you by Ontario Tax Payers ☺</p>
            </footer>
        </div>
    </body>
    </html>
    """

    return email_body

# Function to send an email with job postings
def send_mail(keyword, dataframe):
    sender_email = os.getenv("SENDER_EMAIL")
    receiver_email = os.getenv("RECEIVER_EMAIL")
    password = os.getenv("EMAIL_PASSWORD")
    subject = f"OPS {keyword} jobs posted today"

    # Create email
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    
    email_body = create_styled_email_body_v5(dataframe['Job ID'], dataframe)
    msg.attach(MIMEText(email_body, 'html'))
    
    try:
        # Set up the SMTP server
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, password)
            server.send_message(msg)
            print("Email sent successfully!")
    except Exception as e:
        print(f"Error: {e}")
        
# Main logic to load data and send emails for job postings
data = load_data()

if not data.empty:
    # Clean and convert relevant columns
    data['Posted on'] = pd.to_datetime(data['Posted on'].str.replace(r'(?i)(pm).*', '', regex=True), errors='coerce')
    data['Closing Date'] = pd.to_datetime(data['Closing Date'].str.replace(r'(?i)(pm).*', '', regex=True), errors='coerce')
    data['Closing Week'] = data['Closing Date'].dt.isocalendar().apply(lambda x: f"{x['year']}-{x['week']:02}", axis=1)

    eastern = pytz.timezone('US/Eastern')
    today = pd.Timestamp(datetime.now(eastern).date())
    thisweek = f"{today.date().isocalendar().year}-{today.date().isocalendar().week:02}"

    # Filter for rows after today's date
    data = data[data['Closing Date'] > today]

    # Regex patterns to extract salaries and pay frequency
    salary_pattern = r'\$([\d,]+\.\d{2})  - \$([\d,]+\.\d{2})'
    frequency_pattern = r'Per (Year|Week|Hour)'
    
    # Extract Minimum and Maximum Salaries
    salary_matches = data['Salary'].str.extract(salary_pattern)
    data['Minimum Salary'] = salary_matches[0].str.replace(',', '').astype(float)
    data['Maximum Salary'] = salary_matches[1].str.replace(',', '').astype(float)

    # Extract Pay Frequency
    data['Pay Frequency'] = data['Salary'].str.extract(frequency_pattern)

    # Adjust Minimum and Maximum Salary to yearly values
    data['Adjusted Minimum Salary'] = np.where(
        data['Pay Frequency'] == 'Hour',
        data['Minimum Salary'] * 36.25 * 52,
        np.where(data['Pay Frequency'] == 'Week', data['Minimum Salary'] * 52, data['Minimum Salary'])
    )
    
    data['Adjusted Maximum Salary'] = np.where(
        data['Pay Frequency'] == 'Hour',
        data['Maximum Salary'] * 36.25 * 52,
        np.where(data['Pay Frequency'] == 'Week', data['Maximum Salary'] * 52, data['Maximum Salary'])
    )

    # Format the Adjusted Salary Range
    data['Adjusted Salary Range'] = data.apply(
        lambda row: f"${row['Adjusted Minimum Salary']:.2f} - ${row['Adjusted Maximum Salary']:.2f}"
        if pd.notna(row['Adjusted Minimum Salary']) and pd.notna(row['Adjusted Maximum Salary'])
        else np.nan,
        axis=1
    )

    # Load salary_cutoff
    salary_cutoff = float(os.getenv("SALARY_CUTOFF"))
    
    # Look for jobs and send email if applicable
    keywords = ['analytic', 'research', 'business intelligence', 'python', 'dashboard', 'machine learning', 'artificial intelligence']
    for keyword in keywords:
        df_jobs = data[(data['Job Description'].str.contains(keyword, case=False)) & (data['Posted on'] == today) & (data['Adjusted Minimum Salary'] >= salary_cutoff)].sort_values('Closing Date')
        if len(df_jobs) > 0:
            send_mail(keyword, df_jobs)