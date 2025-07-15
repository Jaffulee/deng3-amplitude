# Data extraction using Amplitude's Export API
# https://amplitude.com/docs/apis/analytics/export
import datetime as dt
import os
import time #for waiting and trying again
import zipfile
import gzip
import shutil
import tempfile
import json
import pandas as pd
import requests as rq
from dotenv import load_dotenv
url = r'https://analytics.eu.amplitude.com/api/2/export'

# load .env file
load_dotenv()

# read .env file
api_keys = {'AMP_API_KEY' : os.getenv('AMP_API_KEY'),
'AMP_SECRET_KEY' : os.getenv('AMP_SECRET_KEY'),
'AMP_DATA_REGION' : os.getenv('AMP_DATA_REGION')}

# define parameters
start_current_day = ((dt.datetime.now())
                                    .replace(hour=0, minute=0, second=0, microsecond=0))

directoryzip = r'datazip/'
directory = r'data/'

# Ensure the directories exist
os.makedirs("logs", exist_ok=True)
data_dir = 'data'
os.makedirs(data_dir, exist_ok = True)
os.makedirs('datazip', exist_ok = True)

# Define the path to the JSON log file
log_path = os.path.join("logs", "amp_extract_logs.csv")

daydiffs = [1,2]
wait_time = 5
total_wait_time = 10

delete_zip = True

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
    'api' : 'API call',
    'timeout': 'Timeout',
    'wait': 'Wait'
}

for daydiff in daydiffs:
    start_daydiff_day = start_current_day - dt.timedelta(days=daydiff)
    print(start_daydiff_day)
    end_daydiff_day = start_daydiff_day.replace(hour=23)
    print(end_daydiff_day)

    start_time = dt.datetime.strftime(start_daydiff_day,r'%Y%m%dT%H')
    end_time = dt.datetime.strftime(end_daydiff_day,r'%Y%m%dT%H')
    params = {'start' : start_time,
            'end' : end_time}

    filenamezip = 'amp'+ start_time +'.zip'
    filepathzip = directoryzip + filenamezip
    waited_time = 0

    while waited_time<total_wait_time:
        try:
            response = rq.get(url, params=params, auth = (api_keys['AMP_API_KEY'],api_keys['AMP_SECRET_KEY']))
            log_times.append(dt.datetime.now())
            log_items.append(log_items_dict['api'])
            log_descriptions.append(log_desriptions_dict['get'])

            response.raise_for_status()
            # Save data
            data = response.content
            with open(filepathzip,'wb') as file:
                file.write(data)
                log_times.append(dt.datetime.now())
                log_items.append(filepathzip)
                log_descriptions.append(log_desriptions_dict['create'])

            print('Written zipped data')

            # create a temp dir for extraction
            temp_dir = tempfile.mkdtemp()


            # Unpack zip
            with zipfile.ZipFile(filepathzip,'r') as zip_ref:
                zip_ref.extractall(temp_dir)
                log_times.append(dt.datetime.now())
                log_items.append(filepathzip)
                log_descriptions.append(log_desriptions_dict['extract'])
            
            # Locate sub folder one level

            # Option 1: mega python magic with next
            # day_folder = next(f for f in os.listdir(temp_dir) if f.isdigit())

            # Option 2: slightly less mega python magic without next
            # day_folder = [f for f in os.listdir(temp_dir) if f.isdigit()][0]

            # Option 3 no python magic
            # tempthingforlex = []
            # for f in os.listdir(temp_dir):
            #     if f.isdigit():
            #         tempthingforlex.append(f)
            # Option 3.1
            # day_folder = tempthingforlex[0]
            # Option 3.2
            # day_folder = next(tempthingforlex)

            day_folder = os.listdir(temp_dir)[0] #get first folder name from zip subfolder
            # print(day_folder)

            day_path = os.path.join(temp_dir,day_folder)
            for root,_,files in os.walk(day_path):
                for file in files:
                    if file.endswith('.gz'):
                        print(file)
                        # gz > final location
                        gz_path = os.path.join(root,file)
                        json_filename = file[:-3]
                        output_path = os.path.join(data_dir,json_filename)
                        with gzip.open(gz_path, 'rb') as gz_file, open(output_path,'wb') as out_file:
                            shutil.copyfileobj(gz_file,out_file)
                            log_times.append(dt.datetime.now())
                            log_items.append(output_path)
                            log_descriptions.append(log_desriptions_dict['copy'])
            
            if delete_zip:
                print('Removing temporary zip file')
                os.remove(filepathzip)
                log_times.append(dt.datetime.now())
                log_items.append(filepathzip)
                log_descriptions.append(log_desriptions_dict['delete'])
            break
        except Exception as e:
            log_times.append(dt.datetime.now())
            log_items.append(log_items_dict['error'])
            log_descriptions.append(e)
            print(f'Error {response.status_code}, {response.text}\n{e}')
            print(f'Trying again, waiting {wait_time}s, total time waited {waited_time}s.')
            waited_time+=wait_time
            time.sleep(wait_time)
            log_times.append(dt.datetime.now())
            log_items.append(log_items_dict['wait'])
            log_descriptions.append(f'{log_desriptions_dict["wait"]} {wait_time}s ({waited_time}s out of {wait_time}s)')

    if waited_time>= total_wait_time:
        print(f'Timout. Total time waited {waited_time}s.')
        log_times.append(dt.datetime.now())
        log_items.append(log_items_dict['timeout'])
        log_descriptions.append(f'{log_desriptions_dict["timeout"]} ({waited_time}s out of {wait_time}s)')


# Create and combine logs
log_df = pd.DataFrame({
    'log_time': log_times,
    'log_item': log_items,
    'log_description': log_descriptions
})

# Load existing logs if the file exists and is not empty
if os.path.exists(log_path) and os.path.getsize(log_path) > 0:
    existing_log_df = pd.read_csv(log_path)
else:
    existing_log_df = pd.DataFrame()

# Combine the existing logs with the new one
combined_log_df = pd.concat([existing_log_df, log_df], ignore_index=True)

# Save the combined logs back to file
combined_log_df.to_csv(log_path,index=False)

print(f"Appended new logs to {log_path}")