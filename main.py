from datetime import datetime
# import datetime
import json
import time
import os
from dotenv import load_dotenv
import requests as rq
from botocore.exceptions import ClientError
import boto3
import modules.load_data_to_s3 as ld
from modules.extract_amplitude_files import extract_gzip_amplitude
from modules.parse_gzip_to_json import parse_gzip_amplitude
import modules.load_smell_checking as lsc
import pandas as pd


load_dotenv()

api_keys = {'AMP_API_KEY' : os.getenv('AMP_API_KEY'),
'AMP_SECRET_KEY' : os.getenv('AMP_SECRET_KEY'),
'AMP_DATA_REGION' : os.getenv('AMP_DATA_REGION')}
daydiffs = [1]
wait_time = 5
total_wait_time = 10

delete_zip = True

# extract_gzip_amplitude(daydiffs,wait_time,total_wait_time,api_keys)

# parse_gzip_amplitude(delete_zip = False)



filepath_base = 'data'
s3filepath_base = 'python-import'
api_keys = {'Access_key_ID' : os.getenv('S3_USER_ACCESS_KEY'),
'Secret_access_key' : os.getenv('S3_USER_SECRET_KEY'),
'AWS_BUCKET_NAME' : os.getenv('AWS_BUCKET_NAME')}

# ld.load_amp_json(s3filepath_base,api_keys)
filepath_base = 'logs'
remove_local = False
# ld.load_logs_csv(s3filepath_base,api_keys,remove_local)

if __name__ == '__main__':
    daydiffs = [1]
    wait_time = 5
    total_wait_time = 10

    delete_zip = True

    listjsonobjs = lsc.get_list_files_s3(api_keys,s3filepath_base+'/','.json')
    listcsvobjs = lsc.get_list_files_s3(api_keys,s3filepath_base+'/','.csv')
    print(listjsonobjs)
    print(listcsvobjs)
    df = lsc.find_uploaded_amplitude_data_datetimes(listjsonobjs,s3filepath_base,daydiffs,endswith='.json')
    print(df)
    # print(listobjs['Contents'])