{{ config(
    materialized='table'
) }}

WITH silver_data AS (
    SELECT * FROM {{ ref('silver_telemetry') }}
)

SELECT
    machine_id,
    device_id,
    DATE_TRUNC('hour', event_ts) AS metric_hour,
    
    -- Temperature Metrics
    ROUND(AVG(temperature), 2) AS avg_temperature,
    MAX(temperature) AS max_temperature,
    
    -- Vibration Metrics
    ROUND(AVG(vibration), 2) AS avg_vibration,
    MAX(vibration) AS max_vibration,
    
    -- Battery & Status
    MIN(battery) AS min_battery,
    
    -- Anomaly Tracking
    COUNT(CASE WHEN anomaly_status = 'CRITICAL' THEN 1 END) AS critical_anomaly_count,
    COUNT(CASE WHEN anomaly_status = 'WARNING' THEN 1 END) AS warning_anomaly_count,
    
    -- Event Volume
    COUNT(*) AS total_events

FROM silver_data
GROUP BY 
    machine_id,
    device_id,
    DATE_TRUNC('hour', event_ts)
ORDER BY 
    metric_hour DESC, 
    machine_id