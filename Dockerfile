FROM quay.io/astronomer/astro-runtime:12.7.1

# Install dbt-postgres and other Python packages globally
RUN pip install --no-cache-dir \
    dbt-postgres \
    faker \
    pg8000

RUN pip install --no-cache-dir "pydantic>=2.0,<3.0" "pyiceberg>=0.5.0" mypy-boto3-glue


RUN airflow db init 