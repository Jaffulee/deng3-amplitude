<!-- README.html -->

<p><a href="https://jaffulee.github.io/Jaffulee/">Visit my website</a></p>

<h1>deng3-amplitude</h1>

<p><strong>Amplitude API Extraction Pipeline</strong><br>
Extracting raw product analytics event data from the Amplitude Export API for downstream analytics.</p>

<ul>
  <li>GitHub: <a href="https://github.com/Jaffulee">https://github.com/Jaffulee</a></li>
  <li>LinkedIn: <a href="https://www.linkedin.com/in/jeffrey-brian-thompson/">https://www.linkedin.com/in/jeffrey-brian-thompson/</a></li>
  <li>Writing: <a href="https://www.thedataschool.co.uk/blog/jeffrey-brian-thompson/">https://www.thedataschool.co.uk/blog/jeffrey-brian-thompson/</a></li>
</ul>

<hr>

<h2>About</h2>

<p>
This project extracts <strong>event-level product analytics data</strong> from the
<strong>Amplitude Export API</strong>, parses and normalises the raw files, and uploads them to
<strong>AWS S3</strong> for downstream processing.
</p>

<p>
It forms the <strong>Bronze (raw ingestion) layer</strong> of a wider analytics pipeline, with
transformations and modelling handled downstream in dbt Cloud.
</p>

<hr>

<h2>Downstream Usage</h2>

<p>
The data produced by this project is modelled and transformed in the following dbt Cloud project:
</p>

<ul>
  <li>
    <strong>dbt analytics layer:</strong>
    <a href="https://github.com/Jaffulee/dbt-cloud-deng3">
      https://github.com/Jaffulee/dbt-cloud-deng3
    </a>
  </li>
</ul>

<p>
In dbt, the raw Amplitude event data is:
</p>

<ul>
  <li>Parsed and standardised in <strong>staging (Bronze)</strong> models</li>
  <li>Flattened and enriched in <strong>intermediate (Silver)</strong> models</li>
  <li>Exposed as analytics-ready <strong>mart (Gold)</strong> models for reporting</li>
</ul>

<hr>

<h2>Features</h2>

<ul>
  <li>Extracts event data from the Amplitude Export API</li>
  <li>Handles zipped and gzip-compressed event files</li>
  <li>Parses nested JSON event and user properties</li>
  <li>Uploads raw and processed files to Amazon S3</li>
  <li>CSV-based structured logging of API and file operations</li>
  <li>Designed to support high-volume, append-only event data</li>
</ul>

<hr>

<h2>Project Structure</h2>

<pre>
├── extract_amp_api.py                 # Alternative / exploratory extraction script
├── main.py                            # Main pipeline entry point
├── modules/
│   ├── extract_amplitude_files.py     # Amplitude Export API extraction
│   ├── parse_gzip_to_json.py          # Gzip → JSON parsing
│   ├── load_data_to_s3.py             # S3 upload utilities
│   └── logginghelper.py               # Structured CSV logging
├── kestra_amplitude_github_action_refactor.yml  # Orchestration proof of concept
├── requirements.txt                   # Python dependencies
├── schema_example.sql                 # Example downstream schema
└── README.md
</pre>

<hr>

<h2>How It Works</h2>

<h3>Step 1: Extract Raw Event Data</h3>

<p>
Event data is pulled from the Amplitude Export API as compressed files, using authenticated API requests.
</p>

<h3>Step 2: Parse and Normalise</h3>

<p>
Downloaded gzip files are decompressed and converted into JSON format, preserving the raw event structure
while making the data easier to ingest downstream.
</p>

<h3>Step 3: Upload to S3</h3>

<p>
Extracted and parsed files, along with logs, are uploaded to an S3 bucket for ingestion into the data warehouse.
</p>

<hr>

<h2>Configuration</h2>

<p>
Credentials and configuration are provided via environment variables:
</p>

<pre>
AMP_API_KEY=your_amplitude_api_key
AMP_SECRET_KEY=your_amplitude_secret_key
AMP_DATA_REGION=eu
S3_USER_ACCESS_KEY=your_aws_access_key
S3_USER_SECRET_KEY=your_aws_secret_key
AWS_BUCKET_NAME=your_bucket_name
</pre>

<hr>

<h2>Orchestration & SQL (Proof of Concept)</h2>

<p>
This repository also includes:
</p>

<ul>
  <li>A <strong>Kestra orchestration example</strong> demonstrating how the pipeline could be scheduled and automated</li>
  <li>An <strong>example SQL silver-layer script</strong> illustrating how raw event data might be transformed</li>
</ul>

<p>
These components are included as <strong>conceptual demonstrations only</strong>.  
The Kestra example uses unencrypted key–value stores and is <strong>not intended as production-ready configuration</strong>.
</p>

<hr>

<h2>Notes</h2>

<ul>
  <li>This project focuses on <strong>extraction and light parsing only</strong>.</li>
  <li>No business logic or analytics modelling is applied here.</li>
  <li>All downstream transformations and analytics live in the dbt Cloud project.</li>
</ul>
