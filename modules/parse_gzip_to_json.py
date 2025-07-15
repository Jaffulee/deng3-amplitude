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

def parse_gzip_amplitude(delete_zip = True):
    '''
    Parses data from export api for amplitude to an output file 

    Args:
        delete_zip (bool): choose to delete zips 

    '''
    url = r'https://analytics.eu.amplitude.com/api/2/export'


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

    waited_time = 0


    # create a temp dir for extraction
    temp_dir = tempfile.mkdtemp()
    filenameszip = os.listdir(directoryzip)
    for filenamezip in filenameszip:
        filepathzip = directoryzip + filenamezip
        # Unpack zip
        with zipfile.ZipFile(filepathzip,'r') as zip_ref:
            zip_ref.extractall(temp_dir)
            log_times.append(dt.datetime.now())
            log_items.append(filepathzip)
            log_descriptions.append(log_desriptions_dict['extract'])

        day_folder = os.listdir(temp_dir)[0] #get first folder name from zip subfolder

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
    



    # Create and combine logs
    lgs.create_and_combine_logs(log_times,log_items,log_descriptions)

if __name__ == '__main__':
    # load .env file
    parse_gzip_amplitude(delete_zip = False)
