# Data Models Documentation

This document provides detailed documentation for all dbt models in the Data Governance Platform.

---

## Table of Contents

- [Bronze Layer (Sources)](#bronze-layer-sources)
- [Silver Layer (Cleaned)](#silver-layer-cleaned)
- [Snapshots (SCD Type 2)](#snapshots-scd-type-2)
- [Gold Layer (Star Schema)](#gold-layer-star-schema)

---

## Bronze Layer (Sources)

**Schema**: `DATA_GOVERNANCE_PROJECT.BRONZE`

The Bronze layer contains raw data ingested from PostgreSQL with minimal transformation—only column naming standardization (SNAKE_CASE) and audit metadata columns.

### Source Tables

| Table | Records | Primary Key | Description |
|-------|---------|-------------|-------------|
| `CUSTOMERS` | 122 | CUSTOMER_NUMBER | Customer master data |
| `EMPLOYEES` | 23 | EMPLOYEE_NUMBER | Employee/sales rep data |
| `OFFICES` | 7 | OFFICE_CODE | Office location data |
| `ORDERS` | 326 | ORDER_NUMBER | Order header data |
| `ORDER_DETAILS` | 2,996 | ORDER_NUMBER + PRODUCT_CODE | Order line items |
| `PAYMENTS` | 273 | CUSTOMER_NUMBER + CHECK_NUMBER | Payment transactions |
| `PRODUCTS` | 110 | PRODUCT_CODE | Product catalog |
| `PRODUCT_LINES` | 7 | PRODUCT_LINE | Product categories |

### Audit Columns (All Bronze Tables)

| Column | Type | Description |
|--------|------|-------------|
| `_LOADED_AT` | TIMESTAMP_TZ | When the record was loaded |
| `_SOURCE_TABLE` | VARCHAR | Source table name in PostgreSQL |

---

## Silver Layer (Cleaned)

**Schema**: `DATA_GOVERNANCE_PROJECT.SILVER`

The Silver layer contains cleaned, standardized data with derived business fields. All models apply deduplication to ensure only the latest version of each record is retained.

### silver_customers

**Purpose**: Cleaned customer master data with region derivation and data quality flags.

| Column | Type | Description | Tags |
|--------|------|-------------|------|
| `CUSTOMER_NUMBER` | NUMBER | Primary key | `primary_key` |
| `CUSTOMER_NAME` | VARCHAR | Company name (titlecase) | `business_name` |
| `CONTACT_FIRST_NAME` | VARCHAR | Contact first name | `pii` |
| `CONTACT_LAST_NAME` | VARCHAR | Contact last name | `pii` |
| `PHONE` | VARCHAR | Phone number | `pii` |
| `ADDRESS_LINE_1` | VARCHAR | Street address | `pii` |
| `ADDRESS_LINE_2` | VARCHAR | Address line 2 | `pii` |
| `CITY` | VARCHAR | City (titlecase) | `geography` |
| `STATE` | VARCHAR | State (uppercase) | `geography` |
| `POSTAL_CODE` | VARCHAR | Postal code | `geography` |
| `COUNTRY` | VARCHAR | Country (uppercase) | `geography` |
| `REGION` | VARCHAR | **Derived**: NA, EMEA, APAC, OTHER | `derived` |
| `SALES_REP_EMPLOYEE_NUMBER` | NUMBER | FK to employees | `foreign_key` |
| `CREDIT_LIMIT` | NUMBER | Credit limit USD | `financial` |
| `IS_MISSING_PHONE` | BOOLEAN | Data quality flag | `data_quality` |
| `IS_MISSING_CREDIT_LIMIT` | BOOLEAN | Data quality flag | `data_quality` |

**Key Transformations**:
- Region derived from country (USA/Canada → NA, European countries → EMEA, etc.)
- Text normalization (trim, titlecase for names)
- Null credit limits set to NULL (not 0)

---

### silver_employees

**Purpose**: Cleaned employee master data with sales role detection.

| Column | Type | Description | Tags |
|--------|------|-------------|------|
| `EMPLOYEE_NUMBER` | NUMBER | Primary key | `primary_key` |
| `FIRST_NAME` | VARCHAR | First name (titlecase) | `pii` |
| `LAST_NAME` | VARCHAR | Last name (titlecase) | `pii` |
| `EMAIL` | VARCHAR | Corporate email | `pii` |
| `PHONE_EXTENSION` | VARCHAR | Office extension | `contact_info` |
| `JOB_TITLE` | VARCHAR | Job title (uppercase) | `hr` |
| `IS_SALES_ROLE` | BOOLEAN | **Derived**: TRUE if title contains "SALES" | `derived` |
| `OFFICE_CODE` | VARCHAR | FK to offices | `foreign_key` |
| `MANAGER_EMPLOYEE_NUMBER` | NUMBER | FK to employees (manager) | `foreign_key` |

---

### silver_orders

**Purpose**: Cleaned order headers with fulfillment metrics and status flags.

| Column | Type | Description | Tags |
|--------|------|-------------|------|
| `ORDER_NUMBER` | NUMBER | Primary key | `primary_key` |
| `ORDER_DATE` | DATE | Order placement date | `temporal` |
| `REQUIRED_DATE` | DATE | Requested delivery date | `temporal` |
| `SHIPPED_DATE` | DATE | Actual shipment date | `temporal` |
| `STATUS` | VARCHAR | Order status (uppercase) | `status` |
| `COMMENTS` | VARCHAR | Order notes | `free_text` |
| `CUSTOMER_NUMBER` | NUMBER | FK to customers | `foreign_key` |
| `DAYS_TO_SHIP` | NUMBER | **Derived**: Days from order to ship | `derived` |
| `DAYS_TO_REQUIRED` | NUMBER | **Derived**: Days from order to required | `derived` |
| `IS_LATE` | BOOLEAN | **Derived**: Shipped after required date | `derived` |
| `IS_CANCELLED` | BOOLEAN | **Derived**: Status = CANCELLED | `derived` |
| `IS_OPEN_ORDER` | BOOLEAN | **Derived**: Status in progress | `derived` |

---

### silver_products

**Purpose**: Cleaned product catalog with profit margin and stock status.

| Column | Type | Description | Tags |
|--------|------|-------------|------|
| `PRODUCT_CODE` | VARCHAR | Primary key (SKU) | `primary_key` |
| `PRODUCT_NAME` | VARCHAR | Display name (titlecase) | `display_name` |
| `PRODUCT_LINE` | VARCHAR | Category FK | `category` |
| `PRODUCT_SCALE` | VARCHAR | Scale (e.g., 1:18) | `specification` |
| `PRODUCT_VENDOR` | VARCHAR | Manufacturer | `vendor` |
| `PRODUCT_DESCRIPTION` | VARCHAR | Detailed description | `description` |
| `QUANTITY_IN_STOCK` | NUMBER | Inventory level | `inventory` |
| `BUY_PRICE` | NUMBER | Cost/COGS | `financial` |
| `MSRP` | NUMBER | List price | `financial` |
| `PROFIT_MARGIN_PCT` | NUMBER | **Derived**: (MSRP - BUY_PRICE) / BUY_PRICE * 100 | `derived` |
| `STOCK_STATUS` | VARCHAR | **Derived**: OUT_OF_STOCK, LOW, MEDIUM, HIGH | `derived` |

---

## Snapshots (SCD Type 2)

**Schema**: `DATA_GOVERNANCE_PROJECT.SNAPSHOTS`

Snapshots capture historical changes using dbt's `check` strategy, comparing all tracked columns on each run.

### customers_snapshot

**Tracked Columns**:
- CUSTOMER_NAME, CONTACT_LAST_NAME, CONTACT_FIRST_NAME
- PHONE, ADDRESS_LINE_1, ADDRESS_LINE_2
- CITY, STATE, POSTAL_CODE, COUNTRY
- SALES_REP_EMPLOYEE_NUMBER, CREDIT_LIMIT

### products_snapshot

**Tracked Columns**:
- PRODUCT_NAME, PRODUCT_LINE, PRODUCT_SCALE
- PRODUCT_VENDOR, PRODUCT_DESCRIPTION
- QUANTITY_IN_STOCK, BUY_PRICE, MSRP

### employees_snapshot

**Tracked Columns**:
- FIRST_NAME, LAST_NAME, EMAIL, PHONE_EXTENSION
- JOB_TITLE, OFFICE_CODE, MANAGER_EMPLOYEE_NUMBER

### Snapshot Columns (All Snapshots)

| Column | Type | Description |
|--------|------|-------------|
| `DBT_VALID_FROM` | TIMESTAMP | Version start date |
| `DBT_VALID_TO` | TIMESTAMP | Version end date (NULL = current) |
| `DBT_SCD_ID` | VARCHAR | Unique version hash |
| `DBT_UPDATED_AT` | TIMESTAMP | Last dbt run timestamp |

---

## Gold Layer (Star Schema)

**Schema**: `DATA_GOVERNANCE_PROJECT.GOLD`

The Gold layer implements a star schema optimized for analytics and BI tools.

### dim_customer_scd2

**Type**: Slowly Changing Dimension Type 2

| Column | Type | Description | Tags |
|--------|------|-------------|------|
| `CUSTOMER_KEY` | VARCHAR | Surrogate key (hash) | `surrogate_key` |
| `CUSTOMER_NUMBER` | NUMBER | Natural key | `natural_key` |
| `CUSTOMER_NAME` | VARCHAR | Company name | `business_name` |
| `CONTACT_FULL_NAME` | VARCHAR | **Derived**: First + Last name | `derived` |
| `CREDIT_LIMIT` | NUMBER | Credit limit USD | `financial` |
| `CREDIT_TIER` | VARCHAR | **Derived**: Platinum/Gold/Silver/Bronze | `derived` |
| `VALID_FROM` | TIMESTAMP | Version start | `scd2` |
| `VALID_TO` | TIMESTAMP | Version end (9999-12-31 if current) | `scd2` |
| `IS_CURRENT` | BOOLEAN | TRUE if active version | `scd2` |
| `VERSION_NUMBER` | NUMBER | Sequence number | `scd2` |

**Credit Tier Logic**:
- Platinum: ≥ $100,000
- Gold: ≥ $50,000
- Silver: ≥ $25,000
- Bronze: < $25,000

---

### dim_product_scd2

**Type**: Slowly Changing Dimension Type 2

| Column | Type | Description | Tags |
|--------|------|-------------|------|
| `PRODUCT_KEY` | VARCHAR | Surrogate key (hash) | `surrogate_key` |
| `PRODUCT_CODE` | VARCHAR | Natural key | `natural_key` |
| `PRODUCT_NAME` | VARCHAR | Display name | `display_name` |
| `PRODUCT_LINE` | VARCHAR | Category | `category` |
| `PROFIT_MARGIN_PCT` | NUMBER | Profit margin | `financial` |
| `PRICE_CATEGORY` | VARCHAR | **Derived**: Budget/Mid-Range/Premium/Luxury | `derived` |
| `STOCK_STATUS` | VARCHAR | Inventory status | `inventory` |
| `VALID_FROM` | TIMESTAMP | Version start | `scd2` |
| `VALID_TO` | TIMESTAMP | Version end | `scd2` |
| `IS_CURRENT` | BOOLEAN | Current version flag | `scd2` |

---

### dim_date

**Type**: Conformed Date Dimension (Role-Playing)

| Column | Type | Description |
|--------|------|-------------|
| `DATE_KEY` | NUMBER | Integer key (YYYYMMDD) |
| `DATE_DAY` | DATE | Calendar date |
| `YEAR_NUMBER` | NUMBER | Four-digit year |
| `MONTH_NUMBER` | NUMBER | Month (1-12) |
| `MONTH_NAME` | VARCHAR | Full month name |
| `DAY_NUMBER` | NUMBER | Day of month |
| `WEEKDAY_NAME` | VARCHAR | Day name |
| `QUARTER_NUMBER` | NUMBER | Quarter (1-4) |
| `WEEK_OF_YEAR` | NUMBER | ISO week |
| `IS_WEEKEND` | BOOLEAN | Saturday or Sunday |

---

### fact_order_items

**Type**: Transactional Fact Table

**Grain**: One row per order line item

| Column | Type | Description | Tags |
|--------|------|-------------|------|
| `ORDER_ITEM_KEY` | VARCHAR | Surrogate key | `surrogate_key` |
| `ORDER_NUMBER` | NUMBER | Degenerate dimension | `degenerate_dim` |
| `ORDER_LINE_NUMBER` | NUMBER | Line sequence | `degenerate_dim` |
| `CUSTOMER_KEY` | VARCHAR | FK to dim_customer_scd2 | `foreign_key` |
| `PRODUCT_KEY` | VARCHAR | FK to dim_product_scd2 | `foreign_key` |
| `ORDER_DATE_KEY` | NUMBER | FK to dim_date | `foreign_key` |
| `REQUIRED_DATE_KEY` | NUMBER | FK to dim_date | `foreign_key` |
| `SHIPPED_DATE_KEY` | NUMBER | FK to dim_date | `foreign_key` |
| `QUANTITY_ORDERED` | NUMBER | Units ordered | `measure` |
| `PRICE_EACH` | NUMBER | Unit selling price | `measure` |
| `LINE_REVENUE` | NUMBER | Quantity × Price | `measure` |
| `BUY_PRICE` | NUMBER | Unit cost | `measure` |
| `LINE_COST` | NUMBER | Quantity × Cost | `measure` |
| `LINE_PROFIT` | NUMBER | Revenue - Cost | `measure` |
| `LINE_MARGIN_PCT` | NUMBER | Profit / Revenue % | `measure` |
| `DAYS_TO_SHIP` | NUMBER | Fulfillment days | `measure` |
| `ORDER_STATUS` | VARCHAR | Current status | `status` |
| `IS_LATE` | BOOLEAN | Late shipment flag | `status` |

---

### fact_customer_payments

**Type**: Transactional Fact Table

**Grain**: One row per payment transaction

| Column | Type | Description | Tags |
|--------|------|-------------|------|
| `PAYMENT_KEY` | VARCHAR | Surrogate key | `surrogate_key` |
| `CHECK_NUMBER` | VARCHAR | Payment reference | `degenerate_dim` |
| `CUSTOMER_KEY` | VARCHAR | FK to dim_customer_scd2 | `foreign_key` |
| `PAYMENT_DATE_KEY` | NUMBER | FK to dim_date | `foreign_key` |
| `AMOUNT` | NUMBER | Payment amount USD | `measure` |
| `PAYMENT_TYPE` | VARCHAR | Payment method | `category` |
| `PAYMENT_DATE` | DATE | Payment date | `temporal` |

---

## Entity Relationship Diagram

<p align="left">
  <img src="./classicmodels-ERD.png" alt="Star Schema Diagram" width="650">
</p>

---
