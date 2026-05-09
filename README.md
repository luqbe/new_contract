# sodacl Data Ingestion Pipeline

This repository contains a Databricks-based data ingestion pipeline implementing the medallion architecture for processing customer, orders, and sales data. The pipeline uses Delta Live Tables (DLT) for orchestration, SodaCL for data quality validation, and follows a bronze-to-silver layer progression with upsert capabilities.

---

# Architecture Overview

The pipeline follows the medallion architecture:

- **Bronze Layer** → Raw data ingestion with append-only storage
- **Silver Layer** → Cleaned and validated data with upsert/merge operations
- **Data Contracts** → SodaCL-based validation rules for data quality gates

## Key Components

### Data Contracts (`contract/`)

YAML files defining validation rules for each dataset.

- `datacontract_customer.yml`
  - No missing customer IDs
  - No duplicate customer IDs
  - Valid email formats

- `datacontract_orders.yml`
  - No missing order IDs
  - Valid order status values

- `datacontract_sales.yml`
  - No duplicate sale IDs
  - No missing total amounts

---

### Ingestion Logic (`src/contract/`)

| File | Description |
|------|-------------|
| `common_utils.py` | Core utilities for validation, upsert operations, and metadata logging |
| `run_ingestion.py` | Main ingestion script (Databricks notebook format) |

---

### Pipeline Configuration

| File | Description |
|------|-------------|
| `resources/ingestion_pipeline.yml` | DLT pipeline definition |
| `databricks.yml` | Databricks Asset Bundle setup |

---

# Setup Instructions

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Permission to create catalogs, schemas, and tables
- Python environment with required libraries

---

## Dependencies

Install the following Python packages:

```bash
pip install soda-core-spark soda-core pyspark delta-spark
```

---

# Deployment

1. Clone this repository

```bash
git clone <repository-url>
cd new_contract
```

2. Update `databricks.yml` with your target workspace details

3. Deploy the Databricks bundle

```bash
databricks bundle deploy --target lab
```

4. Start the DLT pipeline from the Databricks workspace

---

# Usage

The pipeline processes CSV files using the following workflow:

1. **Data Validation**
   - SodaCL contracts validate incoming data

2. **Bronze Ingestion**
   - Raw data is appended to bronze tables if validation succeeds

3. **Silver Upsert**
   - Cleaned data is merged into silver tables using merge keys

4. **Metadata Logging**
   - Ingestion statistics and errors are tracked

---

# Pipeline Parameters

The ingestion script expects the following widget parameters:

| Parameter | Description |
|-----------|-------------|
| `source_path` | Path to the input CSV file |
| `contract_path` | Path to the corresponding data contract YAML |
| `bronze_table` | Target bronze table name |
| `silver_table` | Target silver table name |
| `merge_key` | Column name used for upsert operations |

### Example Table Names

```text
contract.raw_data.bronze_customers
contract.raw_data.silver_customers
```

### Example Merge Key

```text
customer_id
```

---

# Project Structure

```text
new_contract/
├── databricks.yml
├── README.md
├── contract/
│   ├── datacontract_customer.yml
│   ├── datacontract_orders.yml
│   └── datacontract_sales.yml
├── resources/
│   └── ingestion_pipeline.yml
└── src/
    └── contract/
        ├── common_utils.py
        └── run_ingestion.py
```

---

# Data Quality Gates

## Customers

- No missing customer IDs
- No duplicate customer IDs
- Valid email formats

## Orders

Allowed status values:

- `Shipped`
- `Processing`
- `Delivered`
- `Cancelled`

Validation Rules:

- No missing order IDs
- Status must contain valid values only

## Sales

- No duplicate sale IDs
- No missing total amounts

> Failed validations prevent data from progressing to the silver layer and log errors for review.

---

# Monitoring

Ingestion metadata is logged to:

```text
contract.raw_data.ingestion_stats
```

The table tracks:

- Run timestamps
- Dataset names
- Processing status (`SUCCESS`, `FAILED`, `ERROR`)
- Row counts
- Target tables
- Error messages

---

# Contributing

When adding a new dataset:

1. Create a new data contract YAML inside the `contract/` directory
2. Update ingestion logic if required
3. Test validation rules thoroughly
4. Update this `README.md` with the new dataset details

---

# Technologies Used

- Databricks
- Delta Live Tables (DLT)
- PySpark
- Delta Lake
- Soda Core
- SodaCL
- Unity Catalog

---
