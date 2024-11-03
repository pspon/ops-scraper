import pandas as pd
import streamlit as st
import requests
import matplotlib.pyplot as plt
import pytz
from datetime import datetime
import numpy as np

# Function to get CSV filenames dynamically from GitHub using API
@st.cache_data
def get_csv_filenames():
    url = "https://api.github.com/repos/pspon/ops-scraper/contents/data/jobs"
    response = requests.get(url)
    if response.status_code != 200:
        st.warning("Failed to load GitHub directory.")
        return []

    files = response.json()
    csv_files = [file['download_url'] for file in files if file['name'].endswith('scraped_jobs.csv')]
    return csv_files

# Function to download and merge CSV files
@st.cache_data
def load_data():
    csv_files = get_csv_filenames()
    all_data = []

    for url in csv_files:
        response = requests.get(url)
        if response.status_code == 200:
            df = pd.read_csv(url)
            all_data.append(df)
        else:
            st.warning(f"Failed to load {url}")

    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        combined_data = combined_data.drop_duplicates(subset='Job ID').reset_index(drop=True)
        return combined_data
    else:
        st.warning("No data available to display.")
        return pd.DataFrame()

# Load the data
data = load_data()

# Process data
if not data.empty:
    
    # Clean and convert relevant columns
    data['Closing Date'] = pd.to_datetime(data['Closing Date'].str.replace(r'(?i)(pm).*', '', regex=True), errors='coerce')
    data['Closing Week'] = data['Closing Date'].dt.isocalendar().apply(lambda x: f"{x['year']}-{x['week']:02}", axis=1)
    data['Count'] = 1  # For counting

    # Convert 'Job ID' to string type
    data['Job ID'] = data['Job ID'].astype(str)
    
    # Get today's date in Eastern Time
    eastern = pytz.timezone('US/Eastern')
    today = pd.Timestamp(datetime.now(eastern).date())  # Convert to Timestamp for comparison
    
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
        data['Minimum Salary'] * 36.25 * 52,  # Convert hourly to yearly
        np.where(
            data['Pay Frequency'] == 'Week',
            data['Minimum Salary'] * 52,  # Convert weekly to yearly
            data['Minimum Salary']  # Already yearly
        )
    )
    
    data['Adjusted Maximum Salary'] = np.where(
        data['Pay Frequency'] == 'Hour',
        data['Maximum Salary'] * 36.25 * 52,  # Convert hourly to yearly
        np.where(
            data['Pay Frequency'] == 'Week',
            data['Maximum Salary'] * 52,  # Convert weekly to yearly
            data['Maximum Salary']  # Already yearly
        )
    )
    
    # Format the Adjusted Salary Range
    data['Adjusted Salary Range'] = data.apply(
        lambda row: f"${row['Adjusted Minimum Salary']:.2f} - ${row['Adjusted Maximum Salary']:.2f}"
        if pd.notna(row['Adjusted Minimum Salary']) and pd.notna(row['Adjusted Maximum Salary'])
        else np.nan,
        axis=1
    )
    
    #data = data.fillna("", inplace=True)
    # Convert date columns to datetime format
    #data['Closing Date'] = pd.to_datetime(data['Closing Date'], errors='coerce')
    #data['Posted on'] = pd.to_datetime(data['Posted on'], errors='coerce')
    
    # Set page configuration to wide mode
    st.set_page_config(layout="wide")

    # Streamlit App
    st.title("Job Data Visualization and Interactive DataFrame")
    
    # Sidebar for filtering
    st.sidebar.header("Filter Options")
    
    # Input filters
    job_id_filter = st.sidebar.text_input("Job ID")
    job_title_filter = st.sidebar.text_input("Job Title")
    organization_filter = st.sidebar.text_input("Organization")
    salary_filter = st.sidebar.text_input("Salary")
    location_filter = st.sidebar.text_input("Location")
    #closing_date_filter = st.sidebar.date_input("Closing Date",value=None)
    closing_date_filter = st.sidebar.text_input("Closing Date")
    position_title_filter = st.sidebar.text_input("Position Title")
    job_description_filter = st.sidebar.text_input("Job Description")
    division_filter = st.sidebar.text_input("Division")
    city_filter = st.sidebar.text_input("City")
    language_filter = st.sidebar.text_input("Language of Position(s)")
    job_term_filter = st.sidebar.text_input("Job Term")
    job_code_filter = st.sidebar.text_input("Job Code")
    posting_status_filter = st.sidebar.text_input("Posting Status")
    address_filter = st.sidebar.text_input("Address")
    compensation_group_filter = st.sidebar.text_input("Compensation Group")
    schedule_filter = st.sidebar.text_input("Schedule")
    category_filter = st.sidebar.text_input("Category")
    #posted_on_filter = st.sidebar.date_input("Posted On",value=None)
    posted_on_filter = st.sidebar.text_input("Posted On")
    note_filter = st.sidebar.text_input("Note")
    purpose_of_position_filter = st.sidebar.text_input("Purpose of Position")
    duties_filter = st.sidebar.text_input("Duties and Responsibilities")
    staffing_filter = st.sidebar.text_input("Staffing & Licensing")
    knowledge_filter = st.sidebar.text_input("Knowledge")
    skills_filter = st.sidebar.text_input("Skills")
    freedom_of_action_filter = st.sidebar.text_input("Freedom of Action")

    # Filter the DataFrame based on user inputs
    filtered_data = data.copy()
        
    # Apply filters
    if job_id_filter:
        filtered_data = filtered_data[filtered_data['Job ID'].str.contains(job_id_filter, case=False)]
    if job_title_filter:
        filtered_data = filtered_data[filtered_data['Job Title'].str.contains(job_title_filter, case=False)]
    if organization_filter:
        filtered_data = filtered_data[filtered_data['Organization'].str.contains(organization_filter, case=False)]
    if salary_filter:
        filtered_data = filtered_data[filtered_data['Salary'].str.contains(salary_filter, case=False)]
    if location_filter:
        filtered_data = filtered_data[filtered_data['Location'].str.contains(location_filter, case=False)]
    if closing_date_filter:
        #filtered_data = filtered_data[filtered_data['Closing Date'] == pd.to_datetime(closing_date_filter)]
        filtered_data = filtered_data[filtered_data['Closing Date'].str.contains(closing_date_filter, case=False)]
    if position_title_filter:
        filtered_data = filtered_data[filtered_data['Position Title'].str.contains(position_title_filter, case=False)]
    if job_description_filter:
        filtered_data = filtered_data[filtered_data['Job Description'].str.contains(job_description_filter, case=False)]
    if division_filter:
        filtered_data = filtered_data[filtered_data['Division'].str.contains(division_filter, case=False)]
    if city_filter:
        filtered_data = filtered_data[filtered_data['City'].str.contains(city_filter, case=False)]
    if language_filter:
        filtered_data = filtered_data[filtered_data['Language of Position(s)'].str.contains(language_filter, case=False)]
    if job_term_filter:
        filtered_data = filtered_data[filtered_data['Job Term'].str.contains(job_term_filter, case=False)]
    if job_code_filter:
        filtered_data = filtered_data[filtered_data['Job Code'].str.contains(job_code_filter, case=False)]
    if posting_status_filter:
        filtered_data = filtered_data[filtered_data['Posting Status'].str.contains(posting_status_filter, case=False)]
    if address_filter:
        filtered_data = filtered_data[filtered_data['Address'].str.contains(address_filter, case=False)]
    if compensation_group_filter:
        filtered_data = filtered_data[filtered_data['Compensation Group'].str.contains(compensation_group_filter, case=False)]
    if schedule_filter:
        filtered_data = filtered_data[filtered_data['Schedule'].astype(str).str.contains(schedule_filter)]
    if category_filter:
        filtered_data = filtered_data[filtered_data['Category'].str.contains(category_filter, case=False)]
    if posted_on_filter:
        #filtered_data = filtered_data[filtered_data['Posted on'] == pd.to_datetime(posted_on_filter)]
        filtered_data = filtered_data[filtered_data['Posted on'].str.contains(posted_on_filter, case=False)]
    if note_filter:
        filtered_data = filtered_data[filtered_data['Note'].fillna('').str.contains(note_filter, case=False)]
    if purpose_of_position_filter:
        filtered_data = filtered_data[filtered_data['Purpose of Position'].fillna('').str.contains(purpose_of_position_filter, case=False)]
    if duties_filter:
        filtered_data = filtered_data[filtered_data['Duties and Responsibilities'].fillna('').str.contains(duties_filter, case=False)]
    if staffing_filter:
        filtered_data = filtered_data[filtered_data['Staffing & Licensing'].fillna('').str.contains(staffing_filter, case=False)]
    if knowledge_filter:
        filtered_data = filtered_data[filtered_data['Knowledge'].fillna('').str.contains(knowledge_filter, case=False)]
    if skills_filter:
        filtered_data = filtered_data[filtered_data['Skills'].fillna('').str.contains(skills_filter, case=False)]
    if freedom_of_action_filter:
        filtered_data = filtered_data[filtered_data['Freedom of Action'].fillna('').str.contains(freedom_of_action_filter, case=False)]

    # Display raw data
    with st.expander(f"Show Raw Data"):
        st.subheader("Raw Data")
        st.write(data)

    # Create two columns
    col1, col2 = st.columns(2)

    # Add content to the first column
    with col1:

        # Visualization: Jobs per closing date
        jobs_per_day = data.groupby(data['Closing Date'].dt.date).size()
        
        # Number of Job Postings by Closing Date
        st.subheader('Number of Job Postings by Closing Date')
        fig, ax = plt.subplots()
        jobs_per_day.plot(kind='bar', ax=ax)
        ax.set_title('Number of Job Postings by Closing Date')
        ax.set_xlabel('Closing Date')
        ax.set_ylabel('Number of Job Postings')
        plt.xticks(rotation=90)
        st.pyplot(fig)
    
        # Number of Job Postings by Organization
        jobs_per_org = data.groupby('Organization').size().sort_values(ascending=True)
    
        st.subheader('Number of Job Postings by Organization')
        fig, ax = plt.subplots()
        jobs_per_org.plot(kind='barh', ax=ax)
        ax.set_title('Number of Job Postings by Organization')
        ax.set_xlabel('Number of Job Postings')
        ax.set_ylabel('Organization')
        st.pyplot(fig)
    
        # Number of Job Postings by Closing Week of Year
        jobs_per_week_of_year = data.groupby('Closing Week').size()
    
        st.subheader('Number of Job Postings by Closing Week of Year')
        fig, ax = plt.subplots()
        jobs_per_week_of_year.plot(kind='bar', ax=ax)
        ax.set_title('Number of Job Postings by Closing Week of Year')
        ax.set_xlabel('Closing Week of Year')
        ax.set_ylabel('Number of Job Postings')
        plt.xticks(rotation=90)
        st.pyplot(fig)

    # Add content to the second column
    with col2:
        # Display the interactive DataFrame
        st.subheader("Interactive Job Postings")        
            
        # Use a multiselect to choose job postings
        if not filtered_data.empty:
            job_titles = filtered_data['Job Title'].tolist()
            with st.expander(f"Selected Job Postings"):
                selected_job_titles = st.multiselect("Select Job Postings", job_titles, default = job_titles)
    
            # Display details for each selected job in tabs
            if selected_job_titles:
                for job_title in selected_job_titles:
                    selected_job = filtered_data[filtered_data['Job Title'] == job_title].iloc[0]
                    with st.expander(f"↪   {selected_job['Job Title']}"):
                        st.write("### Job Details")
                        st.write(f"**Job Title:** {selected_job['Job Title']}")
                        st.write(f"**Job ID:** {selected_job['Job ID']}")
                        st.write(f"**Position Title:** {selected_job['Position Title']}")
                        st.write(f"**Closing Date:** {selected_job['Closing Date']}")
                        st.write(f"**Posting Status:** {selected_job['Posting Status']}")
                        st.write(f"**Posted on:** {selected_job['Posted on']}")
                        st.write(f"**Salary:** \${selected_job['Adjusted Minimum Salary']:,.2f} - \${selected_job['Adjusted Maximum Salary']:,.2f}")
                        st.write(f"**Job Term:** {selected_job['Job Term']}")
                        st.write(f"**Job Code:** {selected_job['Job Code']}")
                        st.write(f"**Category:** {selected_job['Category']}")
                        st.write(f"**Compensation Group:** {selected_job['Compensation Group']}")
                        st.write(f"**Organization:** {selected_job['Organization']}")
                        st.write(f"**Division:** {selected_job['Division']}")
                        st.write(f"**Location:** {selected_job['Location']}")
                        st.write(f"**Address:** {selected_job['Address']}")
                        st.write(f"**Purpose of Position:** {selected_job['Purpose of Position']}")
                        st.write(f"**Job Description:** {selected_job['Job Description']}")
                        st.write("---")

