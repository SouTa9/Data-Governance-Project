"""
PostgreSQL to Snowflake Bronze Layer DAG
Loads tables from local PostgreSQL to Snowflake BRONZE schema
"""

from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
import re

# ═══════════════════════════════════════════════════════════════════════════
# TABLE NAME MAPPING CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════
# For tables where automatic conversion can't detect word boundaries,
# define explicit mappings.
TABLE_NAME_MAPPINGS = {
    "orderdetails": "ORDER_DETAILS",
    "productlines": "PRODUCT_LINES",
}


def to_snake_case(name):
    """
    Converts any naming convention to SNAKE_UPPERCASE using intelligent detection.

    Strategy:
    1. Check explicit mappings first (for concatenated lowercase words)
    2. Use regex to detect CamelCase word boundaries
    3. Convert delimiters to underscores
    4. Normalize to uppercase

    Examples:
    - orderdetails -> ORDER_DETAILS (via mapping)
    - productlines -> PRODUCT_LINES (via mapping)
    - orderDetails -> ORDER_DETAILS (via regex)
    - product_lines -> PRODUCT_LINES (via regex)
    - customers -> CUSTOMERS (via regex)
    """
    # Check explicit mapping first for concatenated lowercase words
    if name.lower() in TABLE_NAME_MAPPINGS:
        return TABLE_NAME_MAPPINGS[name.lower()]

    # Replace common word delimiters with underscores
    name = re.sub(r"[\s\-\.]", "_", name)

    # Insert underscore before uppercase letters that follow lowercase or digits
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)

    # Insert underscore before digits that follow letters
    name = re.sub(r"([a-zA-Z])(\d)", r"\1_\2", name)

    # Handle sequences like XMLParser -> XML_Parser
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)

    # Remove duplicate underscores and convert to uppercase
    name = re.sub(r"_+", "_", name).upper()

    return name.strip("_")


# ═══════════════════════════════════════════════════════════════════════════
# DAG Definition
# ═══════════════════════════════════════════════════════════════════════════


@dag(
    dag_id="postgres_to_snowflake_bronze",
    start_date=datetime(2025, 11, 5),
    schedule=None,
    catchup=False,
    tags=["postgres", "snowflake", "bronze", "etl"],
    description="Load PostgreSQL tables to Snowflake BRONZE schema",
    default_args={
        "owner": "airflow",
        "retries": 1,
    },
)
def postgres_to_snowflake_pipeline():
    """
    Complete ETL pipeline from PostgreSQL to Snowflake Bronze layer
    """

    # ═══════════════════════════════════════════════════════════════════════════
    # Task 1: Get PostgreSQL Tables
    # ═══════════════════════════════════════════════════════════════════════════

    @task
    def get_postgres_tables():
        """Get list of all tables from PostgreSQL"""
        hook = PostgresHook(postgres_conn_id="postgres_local2")

        sql = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """

        tables = hook.get_records(sql)
        table_names = [row[0] for row in tables]

        print(f"Found {len(table_names)} tables in PostgreSQL:")
        for table in table_names:
            print(f"  - {table}")

        return table_names

    # ═══════════════════════════════════════════════════════════════════════════
    # Task 2: Create Bronze Schema
    # ═══════════════════════════════════════════════════════════════════════════

    @task
    def create_bronze_schema():
        """Ensure BRONZE schema exists in the correct database"""
        sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        # Must specify database first before creating schema
        sf_hook.run("USE DATABASE DATA_GOVERNANCE_PROJECT;")
        sf_hook.run("CREATE SCHEMA IF NOT EXISTS BRONZE;")
        print("✓ BRONZE schema ready")
        return True

    # ═══════════════════════════════════════════════════════════════════════════
    # Task 3: Load Data from PostgreSQL to Snowflake
    # ═══════════════════════════════════════════════════════════════════════════

    @task
    def load_table_to_snowflake(table_name: str):
        """
        Load a single table from PostgreSQL to Snowflake using write_pandas

        Args:
            table_name: Name of the table to load
        """
        print(f"\n{'='*60}")
        print(f"Loading table: {table_name}")
        print(f"{'='*60}\n")

        # Get PostgreSQL connection
        pg_hook = PostgresHook(postgres_conn_id="postgres_local2")

        # Extract data from PostgreSQL into DataFrame
        data_sql = f"SELECT * FROM {table_name};"
        df = pg_hook.get_pandas_df(data_sql)
        print(f"✓ Extracted {len(df)} rows from PostgreSQL")

        # Clean column names for Snowflake
        df.columns = [to_snake_case(col) for col in df.columns]

        # Add metadata columns
        df["_LOADED_AT"] = pd.Timestamp.now(tz="UTC")
        df["_SOURCE_TABLE"] = table_name

        # Get Snowflake connection
        sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = sf_hook.get_conn()

        # Import write_pandas
        from snowflake.connector.pandas_tools import write_pandas

        # Convert table name to SNAKE_UPPERCASE for Snowflake
        snowflake_table_name = to_snake_case(table_name)

        # Drop table if exists (full refresh)
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS BRONZE.{snowflake_table_name}")
        cursor.close()

        # Write dataframe to Snowflake (creates table automatically!)
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=snowflake_table_name,
            database="DATA_GOVERNANCE_PROJECT",
            schema="BRONZE",
            auto_create_table=True,
            overwrite=False,
        )

        print(f"✓ Loaded {nrows} rows into BRONZE.{snowflake_table_name}")

        # Verify load
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM BRONZE.{snowflake_table_name}")
        actual_rows = cursor.fetchone()[0]
        cursor.close()

        print(f"✓ Verification: {actual_rows} rows in Snowflake table\n")

        return {
            "table": snowflake_table_name,
            "rows_loaded": actual_rows,
            "status": "success",
        }

    # ═══════════════════════════════════════════════════════════════════════════
    # Task 4: Verify Data Load
    # ═══════════════════════════════════════════════════════════════════════════

    @task
    def verify_load(load_results: list):
        """
        Verify that data was loaded successfully

        Args:
            load_results: List of results from load tasks
        """
        print("\n" + "=" * 60)
        print("LOAD SUMMARY")
        print("=" * 60)

        total_rows = 0
        for result in load_results:
            table = result["table"]
            rows = result["rows_loaded"]
            status = result["status"]
            total_rows += rows
            print(f"✓ {table:30s} - {rows:>6,d} rows - {status}")

        print("=" * 60)
        print(f"TOTAL: {len(load_results)} tables, {total_rows:,} rows")
        print("=" * 60 + "\n")

        return {"tables_loaded": len(load_results), "total_rows": total_rows}

    # ═══════════════════════════════════════════════════════════════════════════
    # Task Dependencies
    # ═══════════════════════════════════════════════════════════════════════════

    # Get list of tables
    tables = get_postgres_tables()

    # Create bronze schema (call the function to get task instance)
    schema_created = create_bronze_schema()

    # Load each table (dynamic task mapping)
    load_results = load_table_to_snowflake.expand(table_name=tables)

    # Verify all loads
    verification = verify_load(load_results)

    # Set dependencies
    tables >> schema_created >> load_results >> verification


# Instantiate the DAG
postgres_to_snowflake_pipeline()
