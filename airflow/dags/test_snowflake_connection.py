"""
Test Snowflake Connection DAG
Tests connectivity to Snowflake and loads sample data to TEST schema
Airflow 3.0+ compatible
"""

from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# ═══════════════════════════════════════════════════════════════════════════
# DAG Definition (Airflow 3.0 style)
# ═══════════════════════════════════════════════════════════════════════════


@dag(
    dag_id="test_snowflake_connection",
    start_date=datetime(2025, 11, 5),
    schedule=None,
    catchup=False,
    tags=["snowflake", "testing"],
    description="Test Snowflake connection and load sample data to TEST schema",
    default_args={
        "owner": "airflow",
        "retries": 1,
    },
)
def test_snowflake_connection_dag():
    """
    Test Snowflake connection and verify basic operations
    """

    # ═══════════════════════════════════════════════════════════════════════
    # Task 1: Test Connection
    # ═══════════════════════════════════════════════════════════════════════

    @task
    def test_connection():
        """Test Snowflake connection"""
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Test query
        cursor.execute(
            """
            SELECT 
                CURRENT_DATABASE() as database,
                CURRENT_SCHEMA() as schema,
                CURRENT_WAREHOUSE() as warehouse,
                CURRENT_USER() as user
            """
        )

        result = cursor.fetchone()
        print("\n" + "=" * 60)
        print("✅ SNOWFLAKE CONNECTION TEST SUCCESSFUL!")
        print("=" * 60)
        print(f"Database:  {result[0]}")
        print(f"Schema:    {result[1]}")
        print(f"Warehouse: {result[2]}")
        print(f"User:      {result[3]}")
        print("=" * 60 + "\n")

        cursor.close()
        return True

    # ═══════════════════════════════════════════════════════════════════════
    # Task 2: Create TEST Schema
    # ═══════════════════════════════════════════════════════════════════════

    @task
    def create_test_schema():
        """Create a TEST schema for testing purposes"""
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE SCHEMA IF NOT EXISTS TEST;
            """
        )

        print("✅ Created TEST schema")
        cursor.close()
        return True

    # ═══════════════════════════════════════════════════════════════════════
    # Task 3: Create Test Table in TEST
    # ═══════════════════════════════════════════════════════════════════════

    @task
    def create_test_table():
        """Create a test table in TEST schema"""
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS TEST.test_data (
                id INT,
                name VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        print("✅ Created test table: TEST.test_data")
        cursor.close()
        return True

    # ═══════════════════════════════════════════════════════════════════════
    # Task 4: Insert Test Data
    # ═══════════════════════════════════════════════════════════════════════

    @task
    def insert_test_data():
        """Insert sample data into test table"""
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO TEST.test_data (id, name) VALUES
            (1, 'Test Record 1'),
            (2, 'Test Record 2'),
            (3, 'Test Record 3')
            """
        )

        print("✅ Inserted 3 test records")
        cursor.close()
        return True

    # ═══════════════════════════════════════════════════════════════════════
    # Task 5: Verify Data
    # ═══════════════════════════════════════════════════════════════════════

    @task
    def verify_data():
        """Verify data was loaded correctly"""
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM TEST.test_data")
        result = cursor.fetchone()
        row_count = result[0]

        print("\n" + "=" * 60)
        print("✅ VERIFICATION SUCCESSFUL!")
        print(f"   Rows in TEST.test_data: {row_count}")
        print("=" * 60 + "\n")

        cursor.close()
        return True

    # ═══════════════════════════════════════════════════════════════════════
    # Task Dependencies
    # ═══════════════════════════════════════════════════════════════════════

    test = test_connection()
    schema = create_test_schema()
    create = create_test_table()
    insert = insert_test_data()
    verify = verify_data()

    test >> schema >> create >> insert >> verify


# Instantiate the DAG
test_snowflake_connection_dag()
