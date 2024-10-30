import pandas as pd
import matplotlib.pyplot as plt
import os

def visualize_data():
    csv_files = sorted([f for f in os.listdir() if f.startswith('job_listings_')])
    latest_file = csv_files[-1] if csv_files else None

    if latest_file:
        df = pd.read_csv(latest_file)
        df['date_posted'] = pd.to_datetime(df['date_posted'])

        df['count'] = 1
        jobs_per_day = df.groupby(df['date_posted'].dt.date)['count'].sum()

        plt.figure(figsize=(10, 5))
        jobs_per_day.plot(kind='bar')
        plt.title('Number of Job Postings by Date')
        plt.xlabel('Date')
        plt.ylabel('Number of Job Postings')
        plt.xticks(rotation=45)
        plt.tight_layout()

        plt.savefig('docs/job_postings_visualization.png')  # Save in docs directory
        plt.show()

if __name__ == "__main__":
    visualize_data()
