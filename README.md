# Data Governance Project

**Current Progress**  


---

## 🎯 Project Overview

A production-ready data governance platform implementing modern data engineering practices with enterprise-grade tools and industry-standard architectures.

**Core Architecture:**
```
PostgreSQL → Apache Airflow → Snowflake (Bronze/Silver/Gold) → dbt → OpenMetadata
```

---

## 🏗️ Technical Stack

### Orchestration & ETL
- **Apache Airflow 3.0** - Workflow orchestration and scheduling
- **Docker Compose** - Containerized infrastructure management

### Data Warehouse & Transformation
- **Snowflake** - Cloud data warehouse (Bronze/Silver/Gold layers)
- **dbt (Data Build Tool)** - SQL-based transformation framework

### Data Governance
- **OpenMetadata** - Data catalog, lineage tracking, and quality monitoring

### Source Systems
- **PostgreSQL** - Operational database 

---

## 📊 Architecture Highlights

### Medallion Architecture Implementation

**Bronze Layer (Raw Data)**
- Exact copy from source systems
- Audit trail and historical archive
- No transformations applied
- Created by: Airflow ETL pipelines

**Silver Layer (Cleaned Data)**
- Data quality validations
- Deduplication and standardization
- Business rules applied
- Created by: dbt transformations

**Gold Layer (Business Intelligence)**
- Pre-aggregated metrics
- Star schema design
- Dashboard-ready analytics
- Created by: dbt models

---

## 🔄 Data Flow

```
1. Source Data (PostgreSQL Database)
   ├─ 8 operational tables
   └─ ~3,864 rows of business data

2. Extraction & Loading (Airflow)
   ├─ 3 production DAGs
   ├─ Scheduled orchestration
   └─ Automated to Snowflake Bronze

3. Data Warehouse (Snowflake)
   ├─ BRONZE schema: Raw ingestion
   ├─ STAGING schema: Standardization layer
   ├─ SILVER schema: Cleaned & validated
   └─ GOLD schema: Business analytics

4. Transformations (dbt)
   ├─ staging models (views)
   ├─ silver models (tables)
   ├─ gold models (analytics)
   └─ data quality tests

5. Governance (OpenMetadata)
   ├─ Automated data catalog
   ├─ End-to-end lineage tracking
   ├─ PII classification
   └─ Quality profiling
```

---

## 💡 Key Features Implemented

### 1. Enterprise Orchestration
- Dockerized Airflow deployment
- Separation of concerns (scheduler, web server, triggerer)
- Production-grade connection management
- Error handling and retry mechanisms

### 2. Cloud Data Warehouse
- Multi-layer architecture (Bronze/Silver/Gold)
- Efficient data modeling
- Scalable compute resources
- Cost-optimized storage patterns

### 3. SQL-Based Transformations
- Modular dbt models with clear dependencies
- Incremental processing capability
- Automated testing framework
- Documentation as code

### 4. Data Governance
- Searchable data catalog
- Automated lineage visualization
- Data quality metrics
- Compliance-ready PII detection

### 5. Business Intelligence
- Star schema implementation
- Pre-calculated KPIs
- Customer segmentation
- Product performance analytics
- Sales trend analysis
- Employee performance tracking

---


## 🛠️ Technical Skills Demonstrated

### Infrastructure & DevOps
- Docker containerization
- Multi-service orchestration
- Environment configuration
- Network management

### Data Engineering
- ETL pipeline development
- Data warehouse design
- Schema evolution management
- Data quality frameworks

### Programming & Scripting
- Python (Airflow DAGs)
- SQL (dbt transformations)
- YAML configuration
- PowerShell automation

### Cloud & SaaS
- Snowflake cloud warehouse
- Authentication & security
- Resource optimization

### Best Practices
- Version control ready
- Documentation-first approach
- Modular design patterns
- Environment separation

---

## 📁 Project Structure

```
Data-Project/
├── airflow/
│   ├── dags/                    # ETL pipelines (3 DAGs)
│   ├── config/                  # Airflow configuration
│   └── logs/                    # Execution logs
│
├── data_governance/             # dbt project
│   ├── models/
│   │   ├── staging/            # Standardization layer
│   │   ├── silver/             # Cleaned data
│   │   └── gold/               # Analytics 
│   └── dbt_project.yml         # Project configuration
│
├── docker-compose.yml           # Infrastructure as code
│
└── docs/                        # Comprehensive documentation
    ├── State of Art

---

## ✅ Milestones Completed

- [x] Infrastructure setup with Docker
- [x] Airflow 3.0 deployment and configuration
- [x] Source database integration (PostgreSQL)
- [x] Snowflake cloud warehouse setup
- [x] Bronze layer ETL implementation
- [x] dbt project structure and models

---


