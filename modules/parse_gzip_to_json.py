"""
Parse and extract Amplitude Export API gzip files.

This script unzips and decompresses the `.zip` and `.gz` files downloaded from
Amplitude's Export API, places them into a structured `data/` directory,
and optionally removes the original zip files after extraction.
"""

import datetime as dt
import os
import time  # for waiting and retrying
import zipfile
import gzip
import shutil
import tempfile
from typing import List
from . import logginghelper as lgs


def parse_gzip_amplitude(delete_zip: bool = True) -> None:
    """
    Parses `.zip` and `.gz` files from Amplitude Export API, extracts JSON content,
    and writes to the `data/` folder. Tracks and logs each file operation.

    Args:
        delete_zip (bool): Whether to delete zip files after extraction. Defaults to True.
    """
    # Static config
    url = r'https://analytics.eu.amplitude.com/api/2/export'
    start_current_day = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # Directory setup
    directoryzip = 'datazip/'
    data_dir = 'data'
    os.makedirs("logs", exist_ok=True)
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(directoryzip, exist_ok=True)

    # Load logging helper metadata
    log_times: List[dt.datetime] = []
    log_items: List[str] = []
    log_descriptions: List[str] = []
    log_descriptions_dict, log_items_dict = lgs.get_log_descs_and_items_dict()

    # Create a temporary directory for unzip/extraction
    temp_dir = tempfile.mkdtemp()

    # Process each ZIP file
    filenameszip = os.listdir(directoryzip)
    for filenamezip in filenameszip:
        filepathzip = os.path.join(directoryzip, filenamezip)

        # Extract ZIP into temp dir
        with zipfile.ZipFile(filepathzip, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
            log_times.append(dt.datetime.now())
            log_items.append(filepathzip)
            log_descriptions.append(log_descriptions_dict['extract'])

        # Assume first-level folder represents the date
        day_folder = os.listdir(temp_dir)[0]
        day_path = os.path.join(temp_dir, day_folder)

        # Walk through folder, process all .gz files
        for root, _, files in os.walk(day_path):
            for file in files:
                if file.endswith('.gz'):
                    print(file)
                    gz_path = os.path.join(root, file)
                    json_filename = file[:-3]  # Remove .gz extension
                    output_path = os.path.join(data_dir, json_filename)

                    # Extract .gz to JSON
                    with gzip.open(gz_path, 'rb') as gz_file, open(output_path, 'wb') as out_file:
                        shutil.copyfileobj(gz_file, out_file)
                        log_times.append(dt.datetime.now())
                        log_items.append(output_path)
                        log_descriptions.append(log_descriptions_dict['copy'])

        # Optionally delete original zip
        if delete_zip:
            print('Removing temporary zip file')
            os.remove(filepathzip)
            log_times.append(dt.datetime.now())
            log_items.append(filepathzip)
            log_descriptions.append(log_descriptions_dict['delete'])

    # Persist logs to disk
    lgs.create_and_combine_logs(log_times, log_items, log_descriptions)


if __name__ == '__main__':
    # Run parse with delete_zip=False (for debugging or preserving files)
    parse_gzip_amplitude(delete_zip=False)
