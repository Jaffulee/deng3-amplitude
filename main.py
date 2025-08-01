"""
Main orchestration script for Amplitude data extraction, parsing, and upload.

Steps:
1. Extract zipped `.gz` event data from Amplitude Export API.
2. Parse extracted `.gz` files to `.json`.
3. Upload final `.json` data and logs to an S3 bucket.

Environment Variables:
- AMP_API_KEY: Amplitude API key
- AMP_SECRET_KEY: Amplitude secret key
- AMP_DATA_REGION: Amplitude data region
- S3_USER_ACCESS_KEY: AWS access key
- S3_USER_SECRET_KEY: AWS secret key
- AWS_BUCKET_NAME: S3 bucket name
"""

from datetime import datetime
import os
from dotenv import load_dotenv
import modules.load_data_to_s3 as ld
from modules.extract_amplitude_files import extract_gzip_amplitude
from modules.parse_gzip_to_json import parse_gzip_amplitude

# Load environment variables from .env file
load_dotenv()

# ----------------------------
# Amplitude API Configuration
# ----------------------------
api_keys = {
    'AMP_API_KEY': os.getenv('AMP_API_KEY'),
    'AMP_SECRET_KEY': os.getenv('AMP_SECRET_KEY'),
    'AMP_DATA_REGION': os.getenv('AMP_DATA_REGION')
}

# Parameters for extraction
daydiffs = [1]                # List of how many days back to pull from
wait_time = 5                # Time between retries (seconds)
total_wait_time = 10         # Max total wait time per API attempt (seconds)
delete_zip = True            # Option to remove zip files after extraction

# Step 1: Extract .zip files from Amplitude Export API
extract_gzip_amplitude(daydiffs, wait_time, total_wait_time, api_keys)

# Step 2: Parse extracted .gz into .json files
parse_gzip_amplitude(delete_zip=False)

# ----------------------------
# S3 Upload Configuration
# ----------------------------
s3filepath_base = 'python-import'
api_keys = {
    'Access_key_ID': os.getenv('S3_USER_ACCESS_KEY'),
    'Secret_access_key': os.getenv('S3_USER_SECRET_KEY'),
    'AWS_BUCKET_NAME': os.getenv('AWS_BUCKET_NAME')
}

# Step 3: Upload JSON data files to S3
ld.load_amp_json(s3filepath_base, api_keys)

# Step 4: Upload logs (CSV format) to S3
filepath_base = 'logs'
remove_local = False
ld.load_logs_csv(s3filepath_base, api_keys, remove_local)
