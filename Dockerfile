FROM quay.io/astronomer/astro-runtime:12.7.1

# Install dbt-postgres and other Python packages globally
RUN pip install --no-cache-dir \
    dbt-postgres \
    faker \
    pg8000

RUN airflow db init 