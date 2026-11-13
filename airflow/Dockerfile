FROM apache/airflow:3.1.8-python3.12
# The image already ships the Google provider; add DuckDB for compaction.
RUN pip install --no-cache-dir "apache-airflow==3.1.8" "duckdb==2.0.0"
COPY include/ /opt/airflow/include/
ENV PYTHONPATH=/opt/airflow/include
