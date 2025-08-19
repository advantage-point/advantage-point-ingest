# AdvantagePoint Data Ingestion Architecture

This ingestion framework implements a lightweight, serverless ingestion pipeline using Google Cloud Platform (GCP). The design prioritizes **simplicity**, **low cost**, and **scalability** for future growth.

---

## Overview

The system ingests data from external sources (e.g., Web, APIs or Excel files) on a regular basis and writes it to BigQuery. The overall flow is:
1. Data is retrieved from each source and sent to Google Cloud Storage
    - Each source may have its own custom data retrieval method depending on the source type (ex. API, SQL, Excel).
    - The ingestion process run may perform a full (grab all records) or incremental (grab new/modified records) load, depending on preferences, optimizations, constraints, and limitations by the source system as well as Google Cloud.
    - Benefits of loading data to Cloud Storage:
        - Central repository of historical data that can be reloaded to BigQuery at any point, especially for backup/failure reasons.
        - Handles semi-structured and unstructured data ingestion.
2. Cloud Storage data files are loaded to a BigQuery 'temporary' table.
    - The temporary table is meant to contain data from the current ingestion process run.
    - Table is dropped and recreated, picking up any column/data type changes.
3. Data in the temporary table is compared to that in the target table.
    - Schema drift is handled.
    - Inserts, updates, deletions are handled.

```mermaid
graph LR

subgraph source_1[Source 1]
    source_1_entity_1[Source 1 Entity 1]
    source_1_entity_M[Source 1 Entity M]
end

subgraph source_X[Source X]
    source_X_entity_1[Source X Entity 1]
    source_X_entity_N[Source X Entity N]
end

subgraph gcs[Google Cloud Storage]
    gcs_source_1_entity_1[Source 1 Entity 1]
    gcs_source_1_entity_M[Source 1 Entity M]
    gcs_source_X_entity_1[Source X Entity 1]
    gcs_source_X_entity_N[Source X Entity N]
end

subgraph bq_temp[BigQuery Temp]
    bq_temp_source_1_entity_1[Source 1 Entity 1]
    bq_temp_source_1_entity_M[Source 1 Entity M]
    bq_temp_source_X_entity_1[Source X Entity 1]
    bq_temp_source_X_entity_N[Source X Entity N]
end

subgraph bq[BigQuery Target]
    bq_source_1_entity_1[Source 1 Entity 1]
    bq_source_1_entity_M[Source 1 Entity M]
    bq_source_X_entity_1[Source X Entity 1]
    bq_source_X_entity_N[Source X Entity N]
end

source_1 -->|custom extract to| gcs -->|loaded to| bq_temp -->|compared with| bq
source_1_entity_1 --> gcs_source_1_entity_1 --> bq_temp_source_1_entity_1 --> bq_source_1_entity_1
source_1_entity_M --> gcs_source_1_entity_M --> bq_temp_source_1_entity_M --> bq_source_1_entity_M
source_X_entity_1 --> gcs_source_X_entity_1 --> bq_temp_source_X_entity_1 --> bq_source_X_entity_1
source_X_entity_N --> gcs_source_X_entity_N --> bq_temp_source_X_entity_N --> bq_source_X_entity_N

```

---

## Setup

### Architecture

| Name | Type | Description |
|---|---|---|
| `advantage-point-ingest` | GitHub Repository | Stores Python-based ingestion code |
| `cb-advpoint-prod-ingest` | Cloud Build Trigger | Builds Docker container on branch push |
| `ar-advpoint-prod-ingest` | Artifact Registry | Stores built Docker container images |
| `cr-advpoint-prod-ingest` | Cloud Run Service | Executes ingestion logic via container | Runs as `sa-advpoint-prod-ingest` | Uses container image from Artifact Registry |
| `sched-advpoint-prod-ingest` | Cloud Scheduler Job | Triggers ingestion via Cloud Run on a schedule |
| `sm-advpoint-prod-api-key` | Secret Manager Secret | Stores sensitive credentials or API keys |
| `gcs-proj-id-prod-ingest` | Cloud Storage Bucket | Temporarily stores raw files during ingestion |
| `bq-advpoint-prod-ingest` | BigQuery Dataset | Final storage for ingested structured data |
| `sa-advpoint-prod-ingest` | Service Account | Identity used by Cloud Run |

1. Developer pushes code to branch in GitHub.
2. Cloud Build detects the push and:
   - Builds a Docker image
   - Pushes it to Artifact Registry
   - Deploys it to Cloud Run
3. Cloud Scheduler triggers the Cloud Run service on a defined schedule.
4. Cloud Run:
   - Reads ingestion metadata from BigQuery (control table)
   - Loads credentials from Secret Manager
   - Ingests data from source into Cloud Storage
   - Loads final data to from Cloud Storage to BigQuery

```mermaid
graph LR

%% === CI/CD Layer ===
subgraph CI_CD[CI/CD & Source Control]
    github_branch[GitHub Repository]
    cb-advpoint-prod-ingest[Cloud Build]
    ar-advpoint-prod-ingest[Artifact Registry]
end

%% === Runtime / Execution Layer ===
subgraph RUNTIME[Runtime & Orchestration]
    sched-advpoint-prod-ingest[Cloud Scheduler]
    cr-advpoint-prod-ingest[Cloud Run]
    sa-advpoint-prod-ingest[Service Account]
    sm-advpoint-prod-api-key[Secret Manager]
end

%% === Data Layer ===
subgraph DATA[Data Targets]
    gcs-proj-id-prod-ingest[Cloud Storage]
    bq-advpoint-prod-ingest[BigQuery]
end

%% === Flows ===
github_branch -->|push triggers| cb-advpoint-prod-ingest
cb-advpoint-prod-ingest -->|build + push image| ar-advpoint-prod-ingest
ar-advpoint-prod-ingest -->|deploy image| cr-advpoint-prod-ingest
sched-advpoint-prod-ingest -->|scheduled trigger| cr-advpoint-prod-ingest

cr-advpoint-prod-ingest -->|uses| sa-advpoint-prod-ingest
cr-advpoint-prod-ingest -->|reads secrets| sm-advpoint-prod-api-key

cr-advpoint-prod-ingest -->|upload files| gcs-proj-id-prod-ingest
gcs-proj-id-prod-ingest -->|load to BQ| bq-advpoint-prod-ingest
cr-advpoint-prod-ingest -->|or write directly| bq-advpoint-prod-ingest

%% === IAM Bindings ===
sa-advpoint-prod-ingest -->|access| sm-advpoint-prod-api-key
sa-advpoint-prod-ingest -->|access| gcs-proj-id-prod-ingest
sa-advpoint-prod-ingest -->|access| bq-advpoint-prod-ingest

```
---

### BigQuery

```mermaid
graph LR

subgraph control_table_source_1[Control Table Source 1]
    control_table_source_1_entity_1[Source 1 Entity 1]
    control_table_source_1_entity_M[Source 1 Entity M]
end

subgraph control_table_view_source_1[Control Table View Source 1]
    control_table_view_source_1_entity_1[Source 1 Entity 1]
    control_table_view_source_1_entity_M[Source 1 Entity M]
end

subgraph control_table_source_X[Control Table Source X]
    control_table_source_X_entity_1[Source 1 Entity 1]
    control_table_source_X_entity_N[Source 1 Entity N]
end

subgraph control_table_view_source_X[Control Table View Source X]
    control_table_view_source_X_entity_1[Source 1 Entity 1]
    control_table_view_source_X_entity_N[Source 1 Entity N]
end


control_object_master[Master Control Object]

control_table_source_1_entity_1 --> control_table_view_source_1_entity_1 --> control_object_master
control_table_source_1_entity_M --> control_table_view_source_1_entity_M --> control_object_master
control_table_source_X_entity_1 --> control_table_view_source_X_entity_1 --> control_object_master
control_table_source_X_entity_N --> control_table_view_source_X_entity_N --> control_object_master
```

The flow/roles of these control objects are:
1. `control table`
    - Contains necessary ingestion information for each source/entity.
    - If calculations/value patterns are observed/needed, then they should go in `control table view` and/or `control object master.`
2. `control table view`
    - Contains additional source-specific logic.
        - `control_table_table_name`: Underlying control table
        - `bigquery_target_table_id`: While this column appears in all `control table view`s, the logic may differ, so it should be constructed in this object
3. `control object master`
    - Contains column logic common among all control table views. Either:
        - Column logic must be the same
        - Column calculations done in `control table view` and column is referenced in `control object master`.
    - Contains same column but different logic:
        - `control_table_view_name`
    - Contains logic common among all `control table view`s:
        - `biquery_temp_project_id`
        - `bigquery_temp_dataset_id`
        - `bigquery_temp_table_id`
        - `is_active`

The general flow is:
1. Create a table to store source/entity information.
2. Create a view on top of the table to create/override column values.
3. Create a view/table on top of the views from step 2 that unions all views.

Some flexibility can be done:
- If _source 1_ and _source X_ contain similar ingestion processes/logic, it may make sense to either create one control table for both _source 1_ and _source X_ entity records or UNION both control tables into a single control table view.
- If the query in the master control object (if view) requires a lot of compute, then it may make sense to CREATE/REPLACE a table using the desired query.
- Building a view on top of a table gives the flexiblity of:
    - Hardcoding a column value in the view rather than adding/updating a column/value in the table's INSERT statement. Note that if values may differ between entities, then either a CASE WHEN statement can be used within the view (if simple enough) or reinserting/updating the table records.
    - If a 'simple' calculation is needed to create a new column OR to overwrite the underlying table column, then the calculation can occur within the view without the need to update the record's table entry.


| Column | control_table__ingest__{source} | vw__control_table__ingest__{source} | control_object__ingest__master |
|---|---|---|---|
| id | created in this object | selected in this object to join to other control objects | selected in this object to join to other control objects |
| source-specific columns | inserted/updated in this object | selected in this object so that additional source-specific columns/logic is applied | 
| biquery target project and dataset columns | inserted/updated in this object | selected in this object |
| bigquery target table columns | N/A | created in this object as it depends on source-specific data | selected in this object |
| bigquery temp project, dataset, table columns | N/A | created in this object as it is based on target table data and logic is same regardless of source |

---
