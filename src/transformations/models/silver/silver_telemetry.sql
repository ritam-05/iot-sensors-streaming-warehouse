{{ config(
    materialized='table'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('iotstream', 'bronze_telemetry') }}
),

deduplicated AS (
    -- SQS guarantees at-least-once delivery. This removes any accidental duplicates.
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY processing_timestamp DESC) as row_num
    FROM source_data
),

cleaned AS (
    SELECT
        event_id,
        device_id,
        machine_id,
        CAST(event_timestamp AS TIMESTAMP) AS event_ts,
        CAST(processing_timestamp AS TIMESTAMP) AS processing_ts,
        temperature,
        humidity,
        vibration,
        pressure,
        battery,
        UPPER(device_status) AS device_status,
        UPPER(anomaly_status) AS anomaly_status,
        anomaly_reasons,
        schema_version
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM cleaned