# Extract Amplitude Export API Data
# https://amplitude.com/docs/apis/analytics/export

import datetime as dt
import os
import time  # for waiting and retrying
import requests as rq
from dotenv import load_dotenv
from . import logginghelper as lgs
from typing import List, Dict


def extract_gzip_amplitude(
    daydiffs: List[int],
    wait_time: int,
    total_wait_time: int,
    api_keys: Dict[str, str]
) -> None:
    """
    Extracts zipped data from the Amplitude Export API for specified days
    and saves the files locally. Logs actions and errors for traceability.

    Args:
        daydiffs (List[int]): List of integers representing how many days back to extract.
        wait_time (int): Seconds to wait between retry attempts.
        total_wait_time (int): Maximum total wait time in seconds before giving up.
        api_keys (Dict[str, str]): A dictionary containing Amplitude API credentials.

    Raises:
        Exception: If max wait time is exceeded or API consistently fails.
    """
    url = r'https://analytics.eu.amplitude.com/api/2/export'

    # Define base time and local directories
    start_current_day = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    directoryzip = 'datazip/'
    directory = 'data/'

    # Ensure required directories exist
    os.makedirs("logs", exist_ok=True)
    os.makedirs(directory, exist_ok=True)
    os.makedirs(directoryzip, exist_ok=True)

    # Setup logging
    log_path = os.path.join("logs", "amp_extract_logs.csv")
    log_times = []
    log_items = []
    log_descriptions = []
    log_desriptions_dict, log_items_dict = lgs.get_log_descs_and_items_dict()

    # Loop through each day offset and extract corresponding zipped data
    for daydiff in daydiffs:
        start_daydiff_day = start_current_day - dt.timedelta(days=daydiff)
        end_daydiff_day = start_daydiff_day.replace(hour=23)

        start_time = dt.datetime.strftime(start_daydiff_day, r'%Y%m%dT%H')
        end_time = dt.datetime.strftime(end_daydiff_day, r'%Y%m%dT%H')
        params = {'start': start_time, 'end': end_time}

        filenamezip = 'amp' + start_time + '.zip'
        filepathzip = directoryzip + filenamezip
        waited_time = 0

        while waited_time < total_wait_time:
            try:
                # Call API with basic auth and get the content
                response = rq.get(url, params=params, auth=(api_keys['AMP_API_KEY'], api_keys['AMP_SECRET_KEY']))
                log_times.append(dt.datetime.now())
                log_items.append(log_items_dict['api'])
                log_descriptions.append(log_desriptions_dict['get'])

                response.raise_for_status()

                # Write binary zip content to file
                with open(filepathzip, 'wb') as file:
                    file.write(response.content)
                    log_times.append(dt.datetime.now())
                    log_items.append(filepathzip)
                    log_descriptions.append(log_desriptions_dict['create'])

                print('Written zipped data')
                break

            except Exception as e:
                # Log error and retry info
                log_times.append(dt.datetime.now())
                log_items.append(log_items_dict['error'])
                log_descriptions.append(e)

                print(f'Error {response.status_code}, {response.text}\n{e}')
                print(f'Trying again, waiting {wait_time}s, total time waited {waited_time}s.')

                waited_time += wait_time
                time.sleep(wait_time)

                log_times.append(dt.datetime.now())
                log_items.append(log_items_dict['wait'])
                log_descriptions.append(f'{log_desriptions_dict["wait"]} {wait_time}s ({waited_time}s out of {wait_time}s)')

        # Timeout warning
        if waited_time >= total_wait_time:
            print(f'Timeout. Total time waited {waited_time}s.')
            log_times.append(dt.datetime.now())
            log_items.append(log_items_dict['timeout'])
            log_descriptions.append(f'{log_desriptions_dict["timeout"]} ({waited_time}s out of {wait_time}s)')

    # Final logging of all actions
    lgs.create_and_combine_logs(log_times, log_items, log_descriptions)


if __name__ == '__main__':
    # Load credentials and call the function
    load_dotenv()
    api_keys = {
        'AMP_API_KEY': os.getenv('AMP_API_KEY'),
        'AMP_SECRET_KEY': os.getenv('AMP_SECRET_KEY'),
        'AMP_DATA_REGION': os.getenv('AMP_DATA_REGION')
    }

    daydiffs = [1, 2]
    wait_time = 5
    total_wait_time = 10

    extract_gzip_amplitude(daydiffs, wait_time, total_wait_time, api_keys)
