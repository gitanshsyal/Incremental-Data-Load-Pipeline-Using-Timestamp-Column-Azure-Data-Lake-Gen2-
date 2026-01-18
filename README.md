# Incremental-Data-Load-Pipeline-Using-Timestamp-Column-Azure-Data-Lake-Gen2-
This project demonstrates an end-to-end incremental data loading pipeline built using a timestamp-based strategy. The pipeline efficiently processes only new or updated records and maintains execution metadata using a log table.

## Architecture Flow

# Source Ingestion

Raw transaction data is ingested from the Landing zone of Azure Data Lake Gen2.

# Metadata Management (Log Table)

A log table is maintained with the following columns:

source_name

last_success_runtime

last_run_timestamp

status

Incremental Logic

The pipeline reads last_success_runtime into a variable last_success_time.

## Incremental Load Logic
# First Run (No Previous Success)

If last_success_time is NULL:

Fetch MAX(event_timestamp) from the source dataset

Insert a new record into the log table with:

source_name = 'transaction'

last_success_runtime = max(event_timestamp)

last_run_timestamp = current_timestamp

# Subsequent Runs

If last_success_time exists:

Filter source data where event_timestamp > last_success_time

Process incremental records

Update log table with:

last_success_runtime = new max(event_timestamp)

last_run_timestamp = current_timestamp

status = 'SUCCESS'

## Failure Handling

The pipeline is wrapped in a try–except block

In case of failure:

Update log table with:

status = 'FAILURE'

last_run_timestamp = current_timestamp

last_success_runtime remains unchanged to ensure data consistency
status = 'SUCCESS'

