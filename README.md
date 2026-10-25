# Data Quality & Transformation Engine

## Project Overview
A PySpark-based data engineering pipeline designed to ingest retail data, enforce strict data quality rules, quarantine malformed records, and calculate analytical revenue metrics. Built with a focus on idempotency, testing, and production-ready data reconciliation.

## Business Problem
E-commerce operational systems often produce datasets with missing references, invalid financial figures, and malformed types. This engine acts as a validation layer, ensuring only analytically sound data reaches downstream reporting (Delta Lake / Data Warehouse), while preserving all rejected records for root-cause analysis.

## Architecture & Data Flow
1. **Ingestion**: Permissive CSV parsing with explicit PySpark `StructType` schemas to prevent schema drift.
2. **Cleaning & Deduplication**: Deterministic duplicate resolution using Window functions.
3. **Validation Layer**: Array-based rule evaluation. Records failing business rules are separated into a quarantine path with an appended array of failure reasons.
4. **Relational Integrity**: Left-join validations ensure Orders reference valid Customers and Products.
5. **Analytical Transformations**: Inner-join aggregations to calculate valid net revenue, partitioned by category and date.
6. **Output**: Partitioned Parquet files.

## Technology Stack
* **Language**: Python 3.10
* **Processing**: PySpark 3.5.0 / Spark SQL
* **Testing**: Pytest (Local PySpark Session Fixture)
* **Storage**: Parquet (Clean & Quarantined data)

## Execution Instructions
1. Initialize virtual environment: `python3 -m venv venv && source venv/bin/activate`
2. Install dependencies: `pip install -r requirements.txt`
3. Generate synthetic anomalies: `python3 scripts/generate_data.py`
4. Run the pipeline: `python3 -m src.main`
5. Run the test suite: `python3 -m pytest tests/ -v`

## Data Reconciliation
The pipeline mathematically verifies:
`Raw Input = Valid Records + Quarantined Records + Dropped Duplicates`
