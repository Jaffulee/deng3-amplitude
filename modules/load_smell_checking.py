import datetime as dt
# import datetime
import json
import time
import re
import os
from dotenv import load_dotenv
import requests as rq
from botocore.exceptions import ClientError
import boto3
import pandas as pd


def get_list_files_s3(api_keys,s3filepath_base,filename_endswith = ''):
    filepath_base = 'data'
    # filenames = os.listdir(filepath_base)
    # filenames = [f for f in filenames if f.endswith('.json') and filenames]
    # print(filenames)
    print(api_keys)

    s3_client = boto3.client(
        's3',
        aws_access_key_id = api_keys['Access_key_ID'],
        aws_secret_access_key = api_keys['Secret_access_key'],
        region_name = 'eu-north-1'
    )

    response = s3_client.list_objects_v2(Bucket=api_keys['AWS_BUCKET_NAME'],
                                         Prefix = s3filepath_base)
    files = [contents['Key'] for contents in response['Contents'] if contents['Key'].endswith(filename_endswith)]
    return files

def find_uploaded_amplitude_data_datetimes(list_files,s3filepath_base,daydiffs,endswith='.json'):
    date_format = '%Y-%m-%d_%H'
    start_current_day = ((dt.datetime.now())
                                    .replace(hour=0, minute=0, second=0, microsecond=0))
    filenames = []
    filedates = []
    filedays = []
    for daydiff in daydiffs:
        start_daydiff_day = start_current_day - dt.timedelta(days=daydiff)
        print(start_daydiff_day)
        # end_daydiff_day = start_daydiff_day.replace(hour=23)
        # print(end_daydiff_day)
        start_time = dt.datetime.strftime(start_daydiff_day,date_format)
        for filename in list_files:
            regexpstr = r'.*?_([0-9]{4}-.*?)#.*'
            group_search = re.search(regexpstr, filename, re.IGNORECASE)
            if group_search:
                filedatestr = group_search.group(1)
                filenames.append(filename)
                filedatetime = dt.datetime.strptime(filedatestr,date_format)
                filedates.append(filedatetime)
                filedays.append(filedatetime.replace(hour=0))
                # print(filedatestr)
    filesdf = pd.DataFrame({
        'filename' : filenames,
        'filedatetime' : filedates,
        'filedate': filedays
    }).sort_values(by='filedatetime')


    return filesdf

def upload_to_s3(ranges):
    return

def cleanup_uploaded_files():
    return


if __name__ == '__main__':
    api_keys = {'AMP_API_KEY' : os.getenv('AMP_API_KEY'),
    'AMP_SECRET_KEY' : os.getenv('AMP_SECRET_KEY'),
    'AMP_DATA_REGION' : os.getenv('AMP_DATA_REGION')}
    get_list_files_s3(api_keys)