# IoT Streaming Data Warehouse

![Internet of Things illustration](docs/images/iot-streaming-data-warehouse.png)

An end-to-end reference pipeline for generating industrial IoT telemetry, validating and enriching it, maintaining an operational device-state view, and loading analytics-ready events into MotherDuck.

The project is deliberately organized by pipeline responsibility so its components can be run locally during development and deployed independently as the AWS infrastructure is completed.

## Architecture

```text
IoT simulator
    │  validated telemetry events
    ▼
Telemetry SQS queue ──► Telemetry DLQ
    │
    ▼
ETL consumer
    ├──► DynamoDB: latest state per device
    └──► Warehouse SQS queue ──► Warehouse DLQ
                  │
                  ▼
          MotherDuck bronze_telemetry
                  │
                  ▼
            dbt transformations
```

### Data flow

1. The generator produces one measurement for each of ten virtual devices. Readings cover temperature, humidity, vibration, pressure, battery level, and device status.
2. Each event is validated against the `TelemetryEvent` Pydantic schema before being sent to the telemetry queue.
3. The ETL consumer enriches accepted events with a processing timestamp, schema and processing versions, and anomaly classification derived from configurable thresholds.
4. The latest enriched record for each device is upserted into DynamoDB, providing a low-latency operational state store.
5. The same enriched events are routed to the warehouse queue.
6. The warehouse ingestion job batches messages from that queue into MotherDuck's `bronze_telemetry` table, then deletes only successfully handled SQS messages.
7. dbt is the transformation layer for modelling warehouse data beyond bronze. The included models are the generated dbt starter examples and should be replaced with domain models as the project evolves.

## Repository layout

```text
config/                         Telemetry anomaly thresholds
infrastructure/template.yaml    AWS SAM / CloudFormation resources
scripts/preflight.py            AWS credential and service-access check
src/generator/                  Telemetry simulator, schema, and producer handler
src/etl/                        Transformation, anomaly detection, and DynamoDB state writer
src/warehouse/ingestion.py      Warehouse-queue to MotherDuck ingestion job
src/transformations/            dbt project and MotherDuck profile
```

## Prerequisites

- Python 3.10 or later
- AWS CLI configured with credentials for the target account
- AWS SAM CLI, for provisioning the infrastructure
- A MotherDuck account and token for warehouse ingestion and dbt

The deployment configuration targets AWS region `ap-south-1` by default. Adjust `samconfig.toml` if another region is required.

## Quick start

Create and activate a Python virtual environment, then install the direct project dependencies:

```powershell
py -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Create local configuration from the committed template:

```powershell
Copy-Item .env.example .env
```

Set the following values in `.env`. Never commit this file.

| Variable | Purpose |
| --- | --- |
| `TELEMETRY_QUEUE_URL` | SQS endpoint where the generator publishes raw telemetry and the local ETL job reads it. |
| `WAREHOUSE_QUEUE_URL` | SQS endpoint used to buffer enriched events for warehouse loading. |
| `DYNAMODB_TABLE_NAME` | DynamoDB table holding the latest event for each device. Defaults to `iotstream_device_state` in the ETL client. |
| `MOTHERDUCK_TOKEN` | MotherDuck credential used by the DuckDB client and dbt profile. |

## Provision AWS resources

First, verify that the active AWS credentials can access the required services:

```powershell
python scripts/preflight.py
```

Build and deploy the currently defined AWS SAM stack:

```powershell
sam build --template-file infrastructure/template.yaml
sam deploy --template-file infrastructure/template.yaml --config-file samconfig.toml
```

The stack provisions two SQS queues with dedicated dead-letter queues and a provisioned DynamoDB device-state table. Obtain the output values and copy the queue URLs into `.env`:

```powershell
aws cloudformation describe-stacks --stack-name iotstream-stack --query "Stacks[0].Outputs" --output table
```

### Important deployment status

`infrastructure/template.yaml` currently provisions the queues and DynamoDB table only. The Python handlers are implemented, but Lambda functions, the SQS event-source mapping, EventBridge scheduling, Lambda environment variables, and IAM roles have not yet been declared in the SAM template. Until those resources are added, run the generator, ETL consumer, and warehouse ingestion commands manually as described below.

## Run the pipeline locally

With `.env` configured to point to the deployed queues, execute the stages in order:

```powershell
# Generate and publish a batch of raw telemetry.
python src/generator/handler.py

# Read, enrich, persist, and route raw telemetry.
python src/etl/handler.py

# Load routed events into MotherDuck bronze_telemetry.
python src/warehouse/ingestion.py
```

For a simulation without AWS publication, temporarily omit `TELEMETRY_QUEUE_URL`; the generator prints its validated SQS payload instead.

The local ETL and warehouse scripts are queue-draining utilities: they poll up to ten messages, process them, and delete those messages after successful handling. In production, Lambda event-source mappings should own acknowledgement and retry behavior.

## Anomaly rules

Thresholds reside in [config/thresholds.yaml](config/thresholds.yaml). The simulator injects an anomaly into roughly 10% of generated readings, while the ETL layer independently classifies events as `NORMAL`, `WARNING`, or `CRITICAL`.

| Condition | Generated/derived result |
| --- | --- |
| Temperature at or above warning/critical threshold | `WARNING_TEMPERATURE` or `CRITICAL_TEMPERATURE` |
| Vibration at or above critical threshold | `CRITICAL_VIBRATION` |
| Battery at or below critical threshold | `CRITICAL_BATTERY` |
| Device status is `OFFLINE` | `DEVICE_OFFLINE` and critical status |

Update the YAML configuration to tune values without changing application logic.

## Data contracts and storage

Raw telemetry is validated by `src/generator/schema.py`. Key fields include a unique `event_id`, device and machine identifiers, a UTC event timestamp, numeric sensor metrics, battery percentage, and an enumerated device status.

The ETL stage preserves these fields and adds:

- `processing_timestamp`
- `anomaly_status` and `anomaly_reasons`
- `schema_version`
- `processing_version`

DynamoDB is keyed by `device_id`; each write replaces that device's prior state. MotherDuck stores the analytical history in `iotstream_db.bronze_telemetry`.

## dbt transformations

The dbt project is located in `src/transformations` and is configured in `profiles.yml` to use `md:iotstream_db`.

```powershell
dbt debug --project-dir src/transformations --profiles-dir src/transformations
dbt run --project-dir src/transformations --profiles-dir src/transformations
dbt test --project-dir src/transformations --profiles-dir src/transformations
```

The current `models/example` directory contains dbt's starter models. Replace these with models that declare `bronze_telemetry` as a source and build curated silver/gold datasets for monitoring, maintenance, and reporting.

## Reliability and security notes

- Both SQS queues use 20-second long polling, a 60-second visibility timeout, four-day retention, and a DLQ after three receives.
- DLQ retention is fourteen days, allowing failed messages to be inspected and replayed.
- `.env`, private keys, local DuckDB files, dbt build output, and SAM build artifacts are excluded from Git.
- Use least-privilege IAM roles when adding Lambda resources: producer access should be limited to sending telemetry, ETL access to consuming/sending queue messages plus state-table writes, and warehouse access to its queue.
- Rotate MotherDuck and AWS credentials through your organization’s secret-management process; never place their values in source code or documentation.

## Next steps

1. Define the generator and ETL Lambda functions, IAM roles, SQS event mapping, and EventBridge schedule in the SAM template.
2. Add automated tests for schema validation, anomaly boundaries, transformer output, and failure/retry behavior.
3. Replace the example dbt models with bronze-to-silver-to-gold transformations and data-quality tests.
4. Add observability for queue depth, DLQ messages, Lambda failures, ingestion latency, and anomaly rates.

## Pipeline Verification

### AWS SQS message flow

![AWS SQS dashboard showing active message flow](docs/images/aws-sqs-dashboard.png)

The SQS dashboard shows messages moving through the telemetry and warehouse queues, confirming that the cloud-side producer and routing stages are actively exchanging events.

### DynamoDB real-time device state

![DynamoDB table showing real-time device state updates](docs/images/dynamodb-real-time-state.png)

The DynamoDB table contains the latest state for each device, proving that the ETL processor persists enriched operational data as telemetry arrives.

### Local orchestrator and dbt execution

![Local terminal logs showing the Python orchestrator and dbt execution](docs/images/orchestrator-terminal-logs.png)

The terminal output records the Python orchestration stages and dbt run, connecting the queue-driven ingestion process to warehouse transformation execution.

### MotherDuck Bronze and Gold layers

![MotherDuck UI showing populated Bronze and aggregated Gold layer tables](docs/images/motherduck-sql-queries.png)

The MotherDuck results show populated Bronze telemetry alongside aggregated Gold tables, verifying that raw cloud events complete the autonomous flow through dbt into analytics-ready datasets.
