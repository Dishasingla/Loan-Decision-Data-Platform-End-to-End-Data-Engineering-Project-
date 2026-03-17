# Loan Decision Data Platform

## Overview

This project simulates a real-world **loan decision analytics platform** built using modern data engineering architecture patterns.

The system processes raw loan applications, applies decision rules, builds dimensional models, generates KPI metrics, and visualizes insights through Power BI.

## Architecture

Bronze → Silver → Gold → KPI → BI

Raw CSV → Data Warehouse → Star Schema → KPI MART → Dashboard

## Tech Stack

* Python (ETL & Decision Engine)
* PostgreSQL (Data Warehouse)
* Apache Airflow (Orchestration)
* SQL (Dimensional Modeling & KPI Layer)
* Docker (Environment Setup)
* Power BI (Analytics Dashboard)

## Pipeline Flow

1. Raw ingestion of loan applications
2. Decision engine applies rule-based approvals
3. SCD2 fact table stores decision history
4. Gold MART table built for analytics
5. KPI materialized views refreshed
6. Data quality validations executed
7. Dashboard reflects insights

## Key Features

* Slowly Changing Dimension Type 2 implementation
* Star Schema modeling
* KPI semantic layer using materialized views
* Cross-DAG orchestration in Airflow
* Production-scalable architecture design
* Executive-level BI dashboard

## How to Run

```
docker compose up -d
```

Access:

Airflow → http://localhost:8080
Postgres → localhost:5433
Power BI → Open dashboard file

## Production Scaling

This system can scale to:

* Kafka streaming ingestion
* Spark decision engine
* Snowflake warehouse
* Kubernetes Airflow
* dbt semantic layer

## Author

Disha Singla
Data Engineering Portfolio Project
