-- Check integration with S3 sync
DESC INTEGRATION JT_DENG3_AMPLITUDE_AIRBYTE_SYNC;

-- Set schema context
USE SCHEMA JT_DENG3_STAGING;

-- Define JSON file format for parsing JSON files
CREATE OR REPLACE FILE FORMAT jts_json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

-- Define Parquet file format
CREATE OR REPLACE FILE FORMAT jts_parquet_format
  TYPE = 'PARQUET';

-- Create external stage pointing to Airbyte S3 path with specified format
CREATE OR REPLACE STAGE amplitude_airbyte_stage
  STORAGE_INTEGRATION = JT_DENG3_AMPLITUDE_AIRBYTE_SYNC
  URL = 's3://deng3-jt/airbyte-sync/'
  FILE_FORMAT = jts_parquet_format;

-- List files from external stage
LIST @amplitude_airbyte_stage;

-- Create raw table by inferring schema from events parquet
CREATE OR REPLACE TABLE amplitude_events_raw USING TEMPLATE (
    SELECT
        ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM (
        TABLE (
            INFER_SCHEMA (
                LOCATION => '@amplitude_airbyte_stage/AMPLITUDE/events/2025_07_10_1752162980951_0.parquet',
                FILE_FORMAT => 'jts_parquet_format'
            )
        )
    )
);

-- Load all events files into raw table
COPY INTO amplitude_events_raw 
FROM @amplitude_airbyte_stage/AMPLITUDE/events/
FILE_FORMAT = jts_parquet_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet'
ON_ERROR = CONTINUE;

-- Repeat schema inference and loading for each Amplitude object type:
-- EVENTS_LIST
CREATE OR REPLACE TABLE amplitude_events_list_raw USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE (INFER_SCHEMA(
        LOCATION => '@amplitude_airbyte_stage/AMPLITUDE/events_list/2025_07_20_1753028361744_0.parquet',
        FILE_FORMAT => 'jts_parquet_format')));

COPY INTO amplitude_events_list_raw 
FROM @amplitude_airbyte_stage/AMPLITUDE/events_list/
FILE_FORMAT = jts_parquet_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet'
ON_ERROR = CONTINUE;

-- COHORTS
CREATE OR REPLACE TABLE amplitude_cohorts_raw USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE (INFER_SCHEMA(
        LOCATION => '@amplitude_airbyte_stage/AMPLITUDE/cohorts/2025_07_15_1752588155148_0.parquet',
        FILE_FORMAT => 'jts_parquet_format')));

COPY INTO amplitude_cohorts_raw 
FROM @amplitude_airbyte_stage/AMPLITUDE/cohorts/
FILE_FORMAT = jts_parquet_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet'
ON_ERROR = CONTINUE;

-- AVERAGE_SESSION_LENGTH
CREATE OR REPLACE TABLE amplitude_average_session_length_raw USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE (INFER_SCHEMA(
        LOCATION => '@amplitude_airbyte_stage/AMPLITUDE/average_session_length/2025_07_15_1752588155148_0.parquet',
        FILE_FORMAT => 'jts_parquet_format')));

COPY INTO amplitude_average_session_length_raw 
FROM @amplitude_airbyte_stage/AMPLITUDE/average_session_length/
FILE_FORMAT = jts_parquet_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet'
ON_ERROR = CONTINUE;

-- ANNOTATIONS
CREATE OR REPLACE TABLE amplitude_annotations_raw USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE (INFER_SCHEMA(
        LOCATION => '@amplitude_airbyte_stage/AMPLITUDE/annotations/2025_07_15_1752588155148_0.parquet',
        FILE_FORMAT => 'jts_parquet_format')));

COPY INTO amplitude_annotations_raw 
FROM @amplitude_airbyte_stage/AMPLITUDE/annotations/
FILE_FORMAT = jts_parquet_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet'
ON_ERROR = CONTINUE;

-- ACTIVE_USERS
CREATE OR REPLACE TABLE amplitude_active_users_raw USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE (INFER_SCHEMA(
        LOCATION => '@amplitude_airbyte_stage/AMPLITUDE/active_users/2025_07_10_1752162980951_0.parquet',
        FILE_FORMAT => 'jts_parquet_format')));

-- Preview data
SELECT * FROM amplitude_active_users_raw LIMIT 10;

COPY INTO amplitude_active_users_raw 
FROM @amplitude_airbyte_stage/AMPLITUDE/active_users/
FILE_FORMAT = jts_parquet_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet'
ON_ERROR = CONTINUE;

-- Preview raw layers
SELECT * FROM amplitude_events_raw LIMIT 10;
SELECT * FROM amplitude_events_list_raw LIMIT 10;
SELECT * FROM amplitude_cohorts_raw LIMIT 10;
SELECT * FROM amplitude_average_session_length_raw LIMIT 10;
SELECT * FROM amplitude_annotations_raw LIMIT 10;

-- Create silver layer table: events
CREATE OR REPLACE TABLE amp_events AS (
SELECT
    e."uuid" AS event_id,
    e."event_id" AS session_event_order,
    e."session_id" AS session_id,
    el."id" AS events_list_id,
    e."event_time" AS event_time,
    HASH(e."event_properties") AS event_properties_id,
    HASH(e."user_properties") AS user_properties_id
FROM amplitude_events_raw AS e
LEFT JOIN amplitude_events_list_raw AS el
    ON e."event_type" = el."name"
);

-- Events list dimension
CREATE OR REPLACE TABLE amp_events_list AS (
SELECT
    el."id" AS events_list_id,
    el."name" AS event_name
FROM amplitude_events_list_raw AS el);

-- Parse event properties JSON
CREATE OR REPLACE TABLE amp_event_properties AS (
WITH parse_evp_json_cte AS (
    SELECT
        HASH(e."event_properties") AS event_properties_id,
        PARSE_JSON(e."event_properties") AS json
    FROM amplitude_events_raw AS e)
SELECT
    event_properties_id,
    json:"[Amplitude] Page URL"::STRING AS page_url,
    json:"referrer"::STRING AS referrer,
    json:"[Amplitude] Page Counter"::INT AS page_counter,
    json:"[Amplitude] Page Domain"::STRING AS page_domain,
    json:"[Amplitude] Page Path"::STRING AS page_path,
    json:"[Amplitude] Page Title"::STRING AS page_title,
    json:"[Amplitude] Page Location"::STRING AS page_location,
    json:"referring_domain"::STRING AS referring_domain,
    json:"[Amplitude] Element Text"::STRING AS element_text,
    json:"video_url"::STRING AS video_url
FROM parse_evp_json_cte);

-- Lookup dimension tables
CREATE OR REPLACE TABLE amp_device_family_lookup AS (
SELECT DISTINCT HASH("device_family") AS device_family_id, "device_family" FROM amplitude_events_raw);

CREATE OR REPLACE TABLE amp_device_lookup AS (
SELECT DISTINCT HASH("device_type") AS device_type_id, NVL("device_type", "device_family") AS device_name, HASH("device_family") AS device_family_id FROM amplitude_events_raw);

CREATE OR REPLACE TABLE amp_os_lookup AS (
SELECT DISTINCT HASH("os_name") AS os_id, "os_name" FROM amplitude_events_raw);

CREATE OR REPLACE TABLE amp_device AS (
SELECT DISTINCT HASH("os_name", "device_type", "device_family", "os_version") AS device_id, HASH("os_name") AS os_id, HASH("device_type") AS device_type_id, "os_version" FROM amplitude_events_raw);

-- Location dimensions
CREATE OR REPLACE TABLE amp_event_city AS (SELECT DISTINCT "city" AS city_name, HASH("city") AS city_id FROM amplitude_events_raw);
CREATE OR REPLACE TABLE amp_event_country AS (SELECT DISTINCT "country" AS country_name, HASH("country") AS country_id FROM amplitude_events_raw);
CREATE OR REPLACE TABLE amp_event_region AS (SELECT DISTINCT "region" AS region_name, HASH("region") AS region_id FROM amplitude_events_raw);

-- Parse user properties JSON
CREATE OR REPLACE TABLE amp_event_user_properties AS (
WITH json AS (
    SELECT HASH("user_properties") AS up_id, PARSE_JSON("user_properties") AS parsed_json
    FROM amplitude_events_raw)
SELECT
    up_id,
    parsed_json:initial_utm_medium::STRING,
    parsed_json:initial_referring_domain::STRING,
    parsed_json:initial_utm_campaign::STRING,
    parsed_json:referrer::STRING,
    parsed_json:initial_utm_source::STRING,
    parsed_json:initial_referrer::STRING,
    parsed_json:referring_domain::STRING
FROM json);

-- Location fact
CREATE OR REPLACE TABLE amp_events_location AS (
SELECT DISTINCT
    HASH("ip_address") AS location_id,
    "ip_address",
    HASH("city") AS city_id,
    HASH("country") AS country_id,
    HASH("region") AS region_id
FROM amplitude_events_raw);

-- Sessions table
CREATE OR REPLACE TABLE amp_session AS (
SELECT DISTINCT
    "session_id",
    "user_id",
    MD5(CONCAT("os_version", "device_family", "device_type")) AS device_id,
    HASH("ip_address") AS location_id
FROM amplitude_events_raw);

-- Stored procedure: merge refresh
CREATE OR REPLACE PROCEDURE amp_events_table_refresh_merge()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO amp_events AS tgt
    USING (
        SELECT
            e."uuid" AS event_id,
            e."event_id" AS session_event_order,
            e."session_id" AS session_id,
            el."id" AS events_list_id,
            e."event_time" AS event_time,
            HASH(e."event_properties") AS event_properties_id,
            HASH(e."user_properties") AS user_properties_id
        FROM amplitude_events_raw AS e
        LEFT JOIN amplitude_events_list_raw AS el
            ON e."event_type" = el."name"
    ) AS src
    ON tgt.event_id = src.event_id
    WHEN NOT MATCHED THEN
    INSERT (event_id, session_event_order, session_id, events_list_id, event_time, event_properties_id, user_properties_id)
    VALUES (src.event_id, src.session_event_order, src.session_id, src.events_list_id, src.event_time, src.event_properties_id, src.user_properties_id);
    RETURN 'Insert completed: amp_events_table_refresh';
END;
$$;

-- Alternative insert-only refresh procedure
CREATE OR REPLACE PROCEDURE amp_events_table_refresh()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO amp_events (
        SELECT
            e."uuid" AS event_id,
            e."event_id" AS session_event_order,
            e."session_id" AS session_id,
            el."id" AS events_list_id,
            e."event_time" AS event_time,
            HASH(e."event_properties") AS event_properties_id,
            HASH(e."user_properties") AS user_properties_id
        FROM amplitude_events_raw AS e
        LEFT JOIN amplitude_events_list_raw AS el
            ON e."event_type" = el."name"
        WHERE e."event_time" > (
            SELECT MAX(ec.event_time)
            FROM amp_events AS ec
        )
    );
    RETURN 'Insert completed: amp_events_table_refresh';
END;
$$;

-- Scheduled task to run refresh every 24 hours
CREATE OR REPLACE TASK run_amp_events_refresh
WAREHOUSE = dataschool_wh
SCHEDULE = '24 hours'
AS
CALL amp_events_table_refresh();

-- Alternative task: trigger when stream has new data
CREATE OR REPLACE STREAM amp_raw_table_stream ON TABLE jt_deng3_staging.amplitude_events_raw;

CREATE OR REPLACE TASK run_amp_events_refresh
WAREHOUSE = dataschool_wh
WHEN SYSTEM$STREAM_HAS_DATA('amp_raw_table_stream')
AS
CALL amp_events_table_refresh();

-- Example manual insert
INSERT INTO amplitude_events_raw (
SELECT ... FROM amplitude_events_raw LIMIT 1);

-- Final row count check
CALL amp_events_table_refresh();
SELECT COUNT(*) FROM amp_events;

-- Dynamic table example
CREATE OR REPLACE DYNAMIC TABLE amp_events_list_dyn
    TARGET_LAG = '20 minutes'
    WAREHOUSE = dataschool_wh
AS
SELECT
    el."id" AS events_list_id,
    el."name" AS event_name
FROM events_list AS el;

-- View for user properties
CREATE OR REPLACE VIEW amp_event_user_properties_vw AS
WITH json AS (
    SELECT HASH("user_properties") AS up_id, PARSE_JSON("user_properties") AS parsed_json
    FROM amplitude_events_raw)
SELECT
    up_id,
    parsed_json:initial_utm_medium::STRING,
    parsed_json:initial_referring_domain::STRING,
    parsed_json:initial_utm_campaign::STRING,
    parsed_json:referrer::STRING,
    parsed_json:initial_utm_source::STRING,
    parsed_json:initial_referrer::STRING,
    parsed_json:referring_domain::STRING
FROM json;

-- Final gold-layer view
CREATE OR REPLACE VIEW vw_gold_layer_everything AS
SELECT e.*
FROM amp_events AS e
LEFT JOIN amp_events_list AS el ON e.events_list_id = el.events_list_id;

SELECT * FROM vw_gold_layer_everything;
