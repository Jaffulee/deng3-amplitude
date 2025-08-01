"""
Logging helper module for recording file and API operations.

This module provides utility functions to return standard log description dictionaries
and to append new log entries to a CSV file.
"""

import pandas as pd
import os
from typing import List, Tuple, Dict

# Ensure the logs directory exists
os.makedirs("logs", exist_ok=True)

def get_log_descs_and_items_dict() -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Returns predefined dictionaries for log descriptions and log items.

    Returns:
        Tuple containing:
            - log_descriptions_dict (Dict[str, str]): Maps actions to descriptions.
            - log_items_dict (Dict[str, str]): Maps action types to item labels.
    """
    log_descriptions_dict = {
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
        'api': 'API call',
        'timeout': 'Timeout',
        'wait': 'Wait'
    }
    return log_descriptions_dict, log_items_dict


def create_and_combine_logs(
    log_times: List[str],
    log_items: List[str],
    log_descriptions: List[str]
) -> None:
    """
    Combines new log entries with existing log file and saves them as a CSV.

    Args:
        log_times (List[str]): List of timestamps.
        log_items (List[str]): List of item labels.
        log_descriptions (List[str]): List of log descriptions.
    """
    log_path = os.path.join("logs", "amp_extract_logs.csv")

    # Create new log DataFrame
    log_df = pd.DataFrame({
        'log_time': log_times,
        'log_item': log_items,
        'log_description': log_descriptions
    })

    # Load existing logs if present
    if os.path.exists(log_path) and os.path.getsize(log_path) > 0:
        existing_log_df = pd.read_csv(log_path)
    else:
        existing_log_df = pd.DataFrame()

    # Combine new logs with existing
    combined_log_df = pd.concat([existing_log_df, log_df], ignore_index=True)

    # Save combined logs to file
    combined_log_df.to_csv(log_path, index=False)
    print(f"Appended new logs to {log_path}")
