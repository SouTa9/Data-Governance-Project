"""
Test PostgreSQL Connection DAG
Airflow 3.0+ compatible with modern TaskFlow API
Tests connection to local PostgreSQL and reads sample data
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="test_postgres_connection",
    description="Test PostgreSQL connection and read sample data",
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 11, 2),
    catchup=False,
    tags=["test", "postgres", "connection"],
    default_args={
        "owner": "data_engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
)
def test_postgres_connection():
    """Test PostgreSQL connection using Airflow 3.0+ TaskFlow API"""

    # Task 1: Simple SQL test using SQLExecuteQueryOperator
    test_connection = SQLExecuteQueryOperator(
        task_id="test_connection",
        conn_id="postgres_local2",
        sql="""
            SELECT 
                'Connection successful!' as message,
                current_database() as database,
                current_user as user,
                version() as postgres_version;
        """,
    )

    # Task 2: Read sample data using @task decorator
    @task
    def read_sample_data():
        """Read sample data from PostgreSQL using TaskFlow API"""
        # Use PostgresHook to connect
        hook = PostgresHook(postgres_conn_id="postgres_local2")

        # Get customer count
        customer_count = hook.get_first("SELECT COUNT(*) FROM customers")[0]
        print(f"✅ Customer count: {customer_count}")

        # Get employee count
        employee_count = hook.get_first("SELECT COUNT(*) FROM employees")[0]
        print(f"✅ Employee count: {employee_count}")

        # Get sample customers
        customers = hook.get_records(
            'SELECT "customerNumber", "customerName", city, country FROM customers LIMIT 5'
        )
        print("\n📊 Sample Customers:")
        for customer in customers:
            print(
                f"  ID: {customer[0]}, Name: {customer[1]}, City: {customer[2]}, Country: {customer[3]}"
            )

        # Get sample employees
        employees = hook.get_records(
            'SELECT "employeeNumber", "firstName", "lastName", "jobTitle" FROM employees LIMIT 5'
        )
        print("\n👥 Sample Employees:")
        for employee in employees:
            print(
                f"  ID: {employee[0]}, Name: {employee[1]} {employee[2]}, Title: {employee[3]}"
            )

        print("\n✅ PostgreSQL connection test SUCCESSFUL!")
        return {
            "status": "success",
            "customers": customer_count,
            "employees": employee_count,
        }

    # Task dependencies using bit shift operator
    test_connection >> read_sample_data()


# Instantiate the DAG
test_postgres_connection()
