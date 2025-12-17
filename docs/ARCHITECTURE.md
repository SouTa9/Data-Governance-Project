# Architecture Documentation

This document provides detailed technical architecture documentation for the Data Governance Platform.

## System Architecture

### Component Overview

```
┌────────────────────────────────────────────────────────────────────────────┐
│                           DOCKER COMPOSE NETWORK                            │
│                        (data-governance-network)                            │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    AIRFLOW CLUSTER (Port 8080)                      │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌────────────┐  │   │
│  │  │ API Server  │  │  Scheduler  │  │ DAG Processor│  │ Triggerer  │  │   │
│  │  │   :8080     │  │             │  │              │  │            │  │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └────────────┘  │   │
│  │                         │                                           │   │
│  │                         ▼                                           │   │
│  │                  ┌─────────────┐                                    │   │
│  │                  │  PostgreSQL │                                    │   │
│  │                  │   (5433)    │                                    │   │
│  │                  └─────────────┘                                    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                  OPENMETADATA CLUSTER (Port 8585)                   │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌───────────────┐                │   │
│  │  │   Server    │  │  Ingestion  │  │ Elasticsearch │                │   │
│  │  │   :8585     │  │   :8081     │  │    :9200      │                │   │
│  │  └─────────────┘  └─────────────┘  └───────────────┘                │   │
│  │         │                                     ▲                     │   │
│  │         ▼                                     │                     │   │
│  │  ┌─────────────┐                              │                     │   │
│  │  │    MySQL    │──────────────────────────────┘                     │   │
│  │  │   (3306)    │                                                    │   │
│  │  └─────────────┘                                                    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
                           ┌─────────────────────┐
                           │      SNOWFLAKE      │
                           │   (Cloud Service)   │
                           │                     │
                           │  ┌───────────────┐  │
                           │  │    BRONZE     │  │
                           │  │    SILVER     │  │
                           │  │   SNAPSHOTS   │  │
                           │  │     GOLD      │  │
                           │  └───────────────┘  │
                           └─────────────────────┘
```

## Service Details

### Airflow Services

| Service | Container | Port | Purpose |
|---------|-----------|------|---------|
| API Server | `airflow-apiserver` | 8080 | Web UI and REST API |
| Scheduler | `airflow-scheduler` | - | DAG execution scheduling |
| DAG Processor | `airflow-dag-processor` | - | DAG file parsing |
| Triggerer | `airflow-triggerer` | - | Async task triggers |
| PostgreSQL | `airflow-postgres` | 5433 | Airflow metadata |
| Init | `airflow-init` | - | Database migrations |

### OpenMetadata Services

| Service | Container | Port | Purpose |
|---------|-----------|------|---------|
| Server | `openmetadata-server` | 8585 | Catalog API and UI |
| Ingestion | `openmetadata-ingestion` | 8081 | Metadata ingestion Airflow |
| MySQL | `openmetadata-mysql` | 3306 | Catalog storage |
| Elasticsearch | `openmetadata-elasticsearch` | 9200 | Search index |
| Migrate | `execute-migrate-all` | - | Database migrations |

## Data Flow Architecture

### ETL Pipeline Flow

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│              │     │              │     │              │     │              │
│  PostgreSQL  │────▶│   Airflow    │────▶│   Snowflake  │────▶│     dbt      │
│   (Source)   │     │    (DAG)     │     │   (Bronze)   │     │   (Models)   │
│              │     │              │     │              │     │              │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
                                                │
                                                ▼
                            ┌────────────────────────────────────────┐
                            │                                        │
                            │  ┌──────────┐   ┌──────────┐   ┌─────┐│
                            │  │  SILVER  │──▶│SNAPSHOTS │──▶│GOLD ││
                            │  └──────────┘   └──────────┘   └─────┘│
                            │                                        │
                            └────────────────────────────────────────┘
```

### DAG Dependencies

```
postgres_to_snowflake_bronze
│
├── get_postgres_tables
│   └── create_bronze_schema
│       └── load_table_to_snowflake (8 parallel tasks)
│           └── verify_load

dbt_build_pipeline
│
├── dbt_build_silver
│   └── dbt_snapshots
│       └── dbt_build_gold
│           └── dbt_docs_generate
```

## Medallion Architecture Details

### Bronze Layer

**Purpose**: Raw data preservation with audit trail

**Schema**: `DATA_GOVERNANCE_PROJECT.BRONZE`

| Column | Type | Description |
|--------|------|-------------|
| `_LOADED_AT` | TIMESTAMP_TZ | ETL timestamp |
| `_SOURCE_TABLE` | VARCHAR | Source table name |
| `*` | Various | All source columns preserved |

**Tables**: CUSTOMERS, EMPLOYEES, OFFICES, ORDERS, ORDER_DETAILS, PAYMENTS, PRODUCTS, PRODUCT_LINES

### Silver Layer

**Purpose**: Cleaned, standardized, business-ready data

**Schema**: `DATA_GOVERNANCE_PROJECT.SILVER`

**Transformations Applied**:
- Text normalization (trim, titlecase)
- Derived columns (REGION, PROFIT_MARGIN_PCT, IS_LATE)
- Data quality flags (IS_MISSING_PHONE, IS_MISSING_CREDIT_LIMIT)
- Deduplication (latest record per primary key)

### Snapshots Layer

**Purpose**: SCD Type 2 history tracking

**Schema**: `DATA_GOVERNANCE_PROJECT.SNAPSHOTS`

**SCD2 Columns**:
| Column | Purpose |
|--------|---------|
| `DBT_VALID_FROM` | Version start timestamp |
| `DBT_VALID_TO` | Version end timestamp (NULL if current) |
| `DBT_SCD_ID` | Unique version identifier |
| `DBT_UPDATED_AT` | Last update timestamp |

**Tracked Entities**:
- `customers_snapshot`: Credit limits, address changes, sales rep
- `products_snapshot`: Price changes, inventory levels
- `employees_snapshot`: Job titles, office transfers

### Gold Layer

**Purpose**: Star schema for analytics and BI

**Schema**: `DATA_GOVERNANCE_PROJECT.GOLD`

**Dimension Tables**:
- `dim_customer_scd2` - Customer with CREDIT_TIER derivation
- `dim_product_scd2` - Product with PRICE_CATEGORY
- `dim_employee_scd2` - Employee with JOB_LEVEL, IS_MANAGER
- `dim_date` - Conformed date dimension

**Fact Tables**:
- `fact_order_items` - Order line grain (CUSTOMER_KEY, PRODUCT_KEY, DATE_KEYs)
- `fact_customer_payments` - Payment grain (CUSTOMER_KEY, DATE_KEY)

## Security Architecture

### Credential Management

| Secret | Location | Method |
|--------|----------|--------|
| Snowflake credentials | `.env` | Environment variables |
| Airflow connections | Airflow metadata DB | Connection objects |
| OpenMetadata auth | Container ENV | JWT tokens |

### Network Isolation

- All services on `data-governance-network` (172.16.240.0/24)
- External access only via exposed ports
- Inter-service communication via container names

### PII Classification

| Tag | GDPR Relevance | Example Columns |
|-----|----------------|-----------------|
| `gdpr_personal_data` | Article 4(1) | Names, email, phone |
| `pii` | General PII | Address, postal code |
| `financial` | Sensitive | Credit limit, prices |
| `confidential` | Business | Profit margins |

## Monitoring & Observability

### Health Checks

| Service | Endpoint | Interval |
|---------|----------|----------|
| Airflow API | `/api/v2/version` | 30s |
| Airflow Scheduler | `/health` (port 8974) | 30s |
| OpenMetadata | `/healthcheck` (port 8586) | 30s |
| Elasticsearch | `/_cluster/health` | 15s |
| MySQL | `mysqladmin ping` | 15s |
| PostgreSQL | `pg_isready` | 10s |

### Key Metrics to Monitor

- DAG run duration and success rate
- dbt test pass/fail counts
- Snowflake warehouse credit consumption
- OpenMetadata ingestion job status
