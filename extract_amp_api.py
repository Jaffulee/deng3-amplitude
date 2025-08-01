"""
Amplitude Export Script

Downloads event data from Amplitude's Export API for specific dates, extracts and decompresses
the `.zip` and `.gz` files, saves the JSON output, and appends logs to a CSV file.

Documentation:
- Amplitude Export API: https://amplitude.com/docs/apis/analytics/export
- Environment variables must be defined in a .env file:
    AMP_API_KEY, AMP_SECRET_KEY, AMP_DATA_REGION
"""

import datetime as dt
import os
import time
import zipfile
import gzip
import shutil
import tempfile
import pandas as pd
import requests as rq
from dotenv import load_dotenv

# Constants
url = r'https://analytics.eu.amplitude.com/api/2/export'
directoryzip = 'datazip/'
directory = 'data/'
log_path = os.path.join("logs", "amp_extract_logs.csv")
daydiffs = [1, 2]
wait_time = 5
total_wait_time = 10
delete_zip = True

# Load environment variables
load_dotenv()
api_keys = {
    'AMP_API_KEY': os.getenv('AMP_API_KEY'),
    'AMP_SECRET_KEY': os.getenv('AMP_SECRET_KEY'),
    'AMP_DATA_REGION': os.getenv('AMP_DATA_REGION')
}

# Create required directories
os.makedirs("logs", exist_ok=True)
os.makedirs(directory, exist_ok=True)
os.makedirs(directoryzip, exist_ok=True)

# Set logging containers
log_times = []
log_items = []
log_descriptions = []

log_desriptions_dict = {
    'create': 'File created',
    'delete': 'File deleted',
    'copy': 'File copied',
    'extract': 'Zip extracted',
    'get': 'API get',
    'timeout': 'Timeout - total wait time exceeded',
    'wait': 'Waiting before trying again in'
}
log_items_dict = {
    'error': 'Error',
    'api': 'API call',
    'timeout': 'Timeout',
    'wait': 'Wait'
}

# Start of current day
start_current_day = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

# Loop through each relative day to extract
for daydiff in daydiffs:
    start_daydiff_day = start_current_day - dt.timedelta(days=daydiff)
    end_daydiff_day = start_daydiff_day.replace(hour=23)

    print(start_daydiff_day)
    print(end_daydiff_day)

    start_time = dt.datetime.strftime(start_daydiff_day, '%Y%m%dT%H')
    end_time = dt.datetime.strftime(end_daydiff_day, '%Y%m%dT%H')
    params = {'start': start_time, 'end': end_time}
    filenamezip = 'amp' + start_time + '.zip'
    filepathzip = os.path.join(directoryzip, filenamezip)

    waited_time = 0

    # Retry loop for API call
    while waited_time < total_wait_time:
        try:
            response = rq.get(url, params=params, auth=(api_keys['AMP_API_KEY'], api_keys['AMP_SECRET_KEY']))
            log_times.append(dt.datetime.now())
            log_items.append(log_items_dict['api'])
            log_descriptions.append(log_desriptions_dict['get'])

            response.raise_for_status()

            # Save response content to zip file
            with open(filepathzip, 'wb') as file:
                file.write(response.content)
                log_times.append(dt.datetime.now())
                log_items.append(filepathzip)
                log_descriptions.append(log_desriptions_dict['create'])

            print('Written zipped data')

            # Create temp dir and unzip
            temp_dir = tempfile.mkdtemp()

            with zipfile.ZipFile(filepathzip, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
                log_times.append(dt.datetime.now())
                log_items.append(filepathzip)
                log_descriptions.append(log_desriptions_dict['extract'])

            # Find the first subfolder in the temp dir (day folder)
            day_folder = os.listdir(temp_dir)[0]
            day_path = os.path.join(temp_dir, day_folder)

            # Walk through extracted gzip files
            for root, _, files in os.walk(day_path):
                for file in files:
                    if file.endswith('.gz'):
                        print(file)
                        gz_path = os.path.join(root, file)
                        json_filename = file[:-3]  # Remove .gz
                        output_path = os.path.join(directory, json_filename)

                        # Decompress .gz to .json
                        with gzip.open(gz_path, 'rb') as gz_file, open(output_path, 'wb') as out_file:
                            shutil.copyfileobj(gz_file, out_file)
                            log_times.append(dt.datetime.now())
                            log_items.append(output_path)
                            log_descriptions.append(log_desriptions_dict['copy'])

            # Optionally delete zip file
            if delete_zip:
                print('Removing temporary zip file')
                os.remove(filepathzip)
                log_times.append(dt.datetime.now())
                log_items.append(filepathzip)
                log_descriptions.append(log_desriptions_dict['delete'])

            break  # exit retry loop on success

        except Exception as e:
            log_times.append(dt.datetime.now())
            log_items.append(log_items_dict['error'])
            log_descriptions.append(e)
            print(f'Error {response.status_code}, {response.text}\n{e}')
            print(f'Trying again, waiting {wait_time}s, total time waited {waited_time}s.')

            waited_time += wait_time
            time.sleep(wait_time)

            log_times.append(dt.datetime.now())
            log_items.append(log_items_dict['wait'])
            log_descriptions.append(
                f'{log_desriptions_dict["wait"]} {wait_time}s ({waited_time}s out of {total_wait_time}s)'
            )

    # Handle timeout case
    if waited_time >= total_wait_time:
        print(f'Timeout. Total time waited {waited_time}s.')
        log_times.append(dt.datetime.now())
        log_items.append(log_items_dict['timeout'])
        log_descriptions.append(
            f'{log_desriptions_dict["timeout"]} ({waited_time}s out of {total_wait_time}s)'
        )

# Create log DataFrame
log_df = pd.DataFrame({
    'log_time': log_times,
    'log_item': log_items,
    'log_description': log_descriptions
})

# Append new logs to existing file if it exists
if os.path.exists(log_path) and os.path.getsize(log_path) > 0:
    existing_log_df = pd.read_csv(log_path)
else:
    existing_log_df = pd.DataFrame()

combined_log_df = pd.concat([existing_log_df, log_df], ignore_index=True)
combined_log_df.to_csv(log_path, index=False)

print(f"Appended new logs to {log_path}")
