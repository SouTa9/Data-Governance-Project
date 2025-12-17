FROM apache/airflow:3.0.0

# ------------------------------------------------------------------------------
# Install dbt using pipx
# ------------------------------------------------------------------------------


# Install pipx
RUN pip install --no-cache-dir pipx

# Install dbt-core first (this provides the 'dbt' command)
RUN pipx install dbt-core==1.10.15

# Inject the snowflake adapter into the dbt-core environment
RUN pipx inject dbt-core dbt-snowflake==1.10.3

# Ensure pipx binaries are in the PATH
ENV PATH="/home/airflow/.local/bin:${PATH}"

# ------------------------------------------------------------------------------
# Install additional Airflow Providers
# ------------------------------------------------------------------------------
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
