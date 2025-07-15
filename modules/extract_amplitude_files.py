# Unzip and extract from api
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
from . import logginghelper as lgs

def extract_gzip_amplitude(daydiffs,wait_time,total_wait_time,api_keys):
    '''
    Extracts data from export api for amplitude to an output file 

    Args:
        daydiffs (list): list of relative days to get data from
        wait_time (int): time (s) to wait until next retry of api call
        total_wait_time (int): time (s) total to wait
        api_keys (dict): dict of keys

    '''
    url = r'https://analytics.eu.amplitude.com/api/2/export'



    # read .env file


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


    log_times = []
    log_items = []
    log_descriptions = []

    log_desriptions_dict, log_items_dict = lgs.get_log_descs_and_items_dict()

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
    lgs.create_and_combine_logs(log_times,log_items,log_descriptions)

if __name__ == '__main__':

    # load .env file
    load_dotenv()
    api_keys = {'AMP_API_KEY' : os.getenv('AMP_API_KEY'),
    'AMP_SECRET_KEY' : os.getenv('AMP_SECRET_KEY'),
    'AMP_DATA_REGION' : os.getenv('AMP_DATA_REGION')}
    daydiffs = [1,2]
    wait_time = 5
    total_wait_time = 10

    delete_zip = True
    extract_gzip_amplitude(daydiffs,wait_time,total_wait_time,api_keys)

