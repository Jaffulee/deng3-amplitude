# deng3-amplitude

This project demonstrates an example pipeline to extract, parse, and upload event data from the [Amplitude Export API](https://amplitude.com/docs/apis/analytics/export).

## 📦 Project Structure

- **modules/**

  - `extract_amplitude_files.py`: Extracts zipped event data from Amplitude's API.
  - `parse_gzip_to_json.py`: Unzips and converts the downloaded `.gz` files to JSON.
  - `load_data_to_s3.py`: Uploads extracted files and logs to an S3 bucket.
  - `logginghelper.py`: Assists with structured logging of API activity and file operations.

- **data/**: Destination folder for parsed JSON event files.
- **datazip/**: Temporary folder for downloaded `.zip` archives from Amplitude.
- **logs/**: Stores `.csv` logs of extract/transform actions.

## 🛠 Requirements

- Python 3.8+
- AWS S3 access credentials
- Amplitude API credentials

Install dependencies using:

```bash
pip install -r requirements.txt
```

AMP_API_KEY="your_amplitude_api_key"
AMP_SECRET_KEY="your_amplitude_secret_key"
AMP_DATA_REGION="eu"  # or appropriate region
S3_USER_ACCESS_KEY="your_aws_access_key"
S3_USER_SECRET_KEY="your_aws_secret_key"
AWS_BUCKET_NAME="your_bucket_name"

Example schema

<img width="1393" height="842" alt="image" src="https://github.com/user-attachments/assets/c09bd393-3a74-4ebe-a08e-9d2703e049b8" />



python main.py

```
