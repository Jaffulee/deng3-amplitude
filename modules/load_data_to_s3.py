"""
Module to upload JSON and log CSV files to AWS S3.

This script defines two functions:
- load_amp_json(): uploads JSON data files from the 'data/' directory.
- load_logs_csv(): uploads log CSV files from the 'logs/' directory.

Credentials must be provided via environment variables or passed into the functions.
"""

import os
from typing import Dict
from botocore.exceptions import ClientError
import boto3


def load_amp_json(s3filepath_base: str, api_keys: Dict[str, str]) -> None:
    """
    Uploads all `.json` files from the `data/` directory to a specified S3 path,
    and deletes each local file after a successful upload.

    Args:
        s3filepath_base (str): Base path in the S3 bucket to upload the files.
        api_keys (Dict[str, str]): Dictionary of AWS credentials, including:
            - 'Access_key_ID'
            - 'Secret_access_key'
            - 'AWS_BUCKET_NAME'
    """
    filepath_base = 'data'
    filenames = os.listdir(filepath_base)
    print(filenames)
    print(api_keys)

    # Create S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=api_keys['Access_key_ID'],
        aws_secret_access_key=api_keys['Secret_access_key'],
        region_name='eu-north-1'
    )

    for filename in filenames:
        try:
            filepath = filepath_base + '/' + filename
            s3_path = s3filepath_base + '/' + filename

            print(filepath, s3_path)

            s3_client.upload_file(
                Filename=filepath,
                Bucket=api_keys['AWS_BUCKET_NAME'],
                Key=s3_path,
            )
            os.remove(filepath)
            print("Upload succeeded")
        except ClientError as err:
            print(f"Upload failed: {err}")

    print(s3_client)


def load_logs_csv(s3filepath_base: str, api_keys: Dict[str, str], remove_local: bool = False) -> None:
    """
    Uploads log `.csv` files from the `logs/` directory to a specified S3 path.

    Args:
        s3filepath_base (str): Base path in the S3 bucket to upload the logs.
        api_keys (Dict[str, str]): Dictionary of AWS credentials, including:
            - 'Access_key_ID'
            - 'Secret_access_key'
            - 'AWS_BUCKET_NAME'
        remove_local (bool): If True, deletes local files after successful upload.
    """
    filepath_base = 'logs'
    filenames = os.listdir(filepath_base)
    print(filenames)
    print(api_keys)

    # Create S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=api_keys['Access_key_ID'],
        aws_secret_access_key=api_keys['Secret_access_key'],
        region_name='eu-north-1'
    )

    for filename in filenames:
        try:
            filepath = filepath_base + '/' + filename
            s3_path = s3filepath_base + '/' + filename

            print(filepath, s3_path)

            s3_client.upload_file(
                Filename=filepath,
                Bucket=api_keys['AWS_BUCKET_NAME'],
                Key=s3_path,
            )
            if remove_local:
                os.remove(filepath)
            print("Upload succeeded")
        except ClientError as err:
            print(f"Upload failed: {err}")

    print(s3_client)


if __name__ == '__main__':
    print('hi')
