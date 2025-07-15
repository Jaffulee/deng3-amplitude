import pandas as pd
import os


# Ensure the directories exist
os.makedirs("logs", exist_ok=True)

def get_log_descs_and_items_dict():

    log_desriptions_dict = {
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
        'api' : 'API call',
        'timeout': 'Timeout',
        'wait': 'Wait'
    }
    return log_desriptions_dict, log_items_dict

def create_and_combine_logs(log_times,log_items,log_descriptions):
    log_path = os.path.join("logs", "amp_extract_logs.csv")
    # Create and combine logs
    log_df = pd.DataFrame({
        'log_time': log_times,
        'log_item': log_items,
        'log_description': log_descriptions
    })

    # Load existing logs if the file exists and is not empty
    if os.path.exists(log_path) and os.path.getsize(log_path) > 0:
        existing_log_df = pd.read_csv(log_path)
    else:
        existing_log_df = pd.DataFrame()

    # Combine the existing logs with the new one
    combined_log_df = pd.concat([existing_log_df, log_df], ignore_index=True)

    # Save the combined logs back to file
    combined_log_df.to_csv(log_path,index=False)

    print(f"Appended new logs to {log_path}")