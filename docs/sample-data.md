# Sample Data Across Warehouse Layers

This reference illustrates how one simulated telemetry event progresses through the MotherDuck warehouse. The values are representative examples; timestamps and measurements vary on every simulator run.

## Bronze: `bronze_telemetry`

Bronze is the append-oriented landing table. It preserves the enriched event received from the warehouse queue with only the minimum shaping needed for storage.

| event_id | device_id | machine_id | temperature | humidity | vibration | pressure | battery | device_status | anomaly_status | anomaly_reasons | event_timestamp | processing_timestamp | schema_version |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- | --- | ---: |
| `evt-a1b2c3d4` | `sensor-07` | `MACHINE-07` | 92.40 | 48.10 | 5.60 | 101.30 | 8 | `ONLINE` | `CRITICAL` | `["CRITICAL_TEMPERATURE", "CRITICAL_VIBRATION", "CRITICAL_BATTERY"]` | `2026-11-04 10:00:15+00:00` | `2026-11-04 10:00:20+00:00` | 1 |

## Silver: `silver_telemetry`

Silver removes duplicate deliveries by retaining the latest processed copy of each `event_id`, casts timestamps, and normalizes status fields to uppercase.

| event_id | device_id | machine_id | event_ts | processing_ts | temperature | humidity | vibration | pressure | battery | device_status | anomaly_status | anomaly_reasons | schema_version |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | ---: |
| `evt-a1b2c3d4` | `sensor-07` | `MACHINE-07` | `2026-11-04 10:00:15` | `2026-11-04 10:00:20` | 92.40 | 48.10 | 5.60 | 101.30 | 8 | `ONLINE` | `CRITICAL` | `["CRITICAL_TEMPERATURE", "CRITICAL_VIBRATION", "CRITICAL_BATTERY"]` | 1 |

## Gold: `gold_hourly_machine_stats`

Gold aggregates all Silver events for one device and machine within an hour. It is the reporting-ready table used for operational dashboards and analytical queries.

| machine_id | device_id | metric_hour | avg_temperature | max_temperature | avg_vibration | max_vibration | min_battery | critical_anomaly_count | warning_anomaly_count | total_events |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `MACHINE-07` | `sensor-07` | `2026-11-04 10:00:00` | 84.72 | 92.40 | 4.18 | 5.60 | 8 | 2 | 3 | 12 |
