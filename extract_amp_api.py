# Data extraction using Amplitude's Export API
# https://amplitude.com/docs/apis/analytics/export
import datetime as dt
import os
import time #for waiting and trying again
import zipfile
import gzip
import shutil
import tempfile
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

daydiffs = [1,2,3]
wait_time = 5
total_wait_time = 10

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
            response.raise_for_status()
            # Save data
            data = response.content
            with open(filepathzip,'wb') as file:
                file.write(data)
            print('Written zipped data')

            # create a temp dir for extraction
            temp_dir = tempfile.mkdtemp()

            # Create local output directory
            data_dir = 'data'
            os.makedirs(data_dir, exist_ok = True)

            # Unpack zip
            with zipfile.ZipFile(filepathzip,'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
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
            break
        except:
            print(f'Error {response.status_code}, {response.text}')
            print(f'Trying again, waiting {wait_time}s, total time waited {waited_time}s.')
            waited_time+=wait_time
            time.sleep(wait_time)

    if waited_time>= wait_time:
        print(f'Timout. Total time waited {waited_time}s.')

