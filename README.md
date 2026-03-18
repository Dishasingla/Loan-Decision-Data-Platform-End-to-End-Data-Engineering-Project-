# Loan Decision Data Platform (Production-Grade Data Engineering Project)

## Overview

This project simulates a **production-grade loan decision analytics platform** built using modern data engineering architecture patterns.

The platform ingests raw loan applications, applies rule-based decision logic, maintains historical decision tracking using **Slowly Changing Dimension Type-2 (SCD2)**, builds analytical marts, generates KPI semantic layers, and serves executive insights through **Power BI dashboards**.

This project demonstrates **end-to-end data platform ownership**, covering ingestion, transformation, orchestration, modeling, quality validation, and BI enablement.

---

## Architecture

Bronze → Silver → Quality → Gold → KPI → BI

External Source → Airflow → Data Warehouse → Analytical MART → KPI Layer → Dashboard

---

## Data Layers

### Bronze Layer (Raw Storage)

Stores raw loan applications exactly as received from source systems.

Table:
- raw_loan_applications

Responsibilities:
- Preserve source data integrity
- Enable replay & auditability
- Append-only ingestion design

---

### Silver Layer (Decision Engine + Historical Tracking)

Implements rule-based loan approval logic and maintains **historical decision tracking using SCD2 modeling**.

Table:
- fact_loan_decisions

Responsibilities:
- Apply approval decision rules
- Track versioned decision history
- Maintain temporal validity
- Ensure analytical consistency

---

### Data Quality Layer

Ensures trusted analytics output via validation checks.

Examples:
- Null validation
- Referential integrity checks
- KPI threshold validation
- Duplicate detection

---

### Gold Layer (Analytical MART)

Curated dataset optimized for reporting workloads.

Table:
- mart_loan_decision

Responsibilities:
- Pre-aggregated analytical structure
- Query performance optimization
- BI-ready schema

---

### Platinum Layer (KPI Semantic Layer)

Materialized views serving executive-level KPIs.

Examples:
- Total loan approvals
- Approval rate by property area
- Credit performance analysis
- Monthly approval trends

This layer is refreshed via **Airflow Cross-DAG orchestration**.

---

## Orchestration Design (Airflow)

### Warehouse Bootstrap DAG
- Initializes warehouse schema
- Creates Bronze & Silver tables
- Manual trigger (runs once)

### Core Loan Decision Pipeline DAG
1. Raw ingestion
2. Decision engine processing
3. SCD2 fact update
4. Data quality validation
5. MART build

Scheduled daily.

### KPI Refresh Pipeline DAG
- Dataset‑driven orchestration
- Runs after MART build

---

## Technology Stack

| Layer | Technology |
|------|-----------|
| Orchestration | Apache Airflow |
| Processing | Python |
| Warehouse | PostgreSQL |
| Modeling | SQL (Star Schema + SCD2) |
| Infrastructure | Docker |
| Analytics | Power BI |
| Orchestration Pattern | Dataset‑Driven DAGs |

---

## End‑to‑End Pipeline Flow

External Source → Airflow → Bronze → Silver → Quality → Gold → KPI → Power BI

---

## Key Engineering Concepts Demonstrated

- Medallion Architecture
- Slowly Changing Dimension Type‑2
- Cross‑DAG orchestration
- Production warehouse bootstrap
- Analytical MART optimization
- KPI semantic layer design
- End‑to‑end data platform ownership
- Executive BI enablement

---

## How to Run

docker compose up -d

Access:
Airflow → http://localhost:8080  
Postgres → localhost:5433  
Power BI → Open dashboard file

---

## Production Scaling Strategy

| Current | Enterprise Upgrade |
|--------|-------------------|
| CSV ingestion | Kafka / CDC |
| Python processing | Spark |
| PostgreSQL | Snowflake |
| Docker Airflow | Kubernetes |
| SQL MART | dbt |
| KPI Views | Metrics Layer |
| Local BI | Cloud BI |

---

## Author

Disha Singla  
Data Engineering Portfolio Project
