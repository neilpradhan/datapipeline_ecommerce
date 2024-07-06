FROM apache/airflow:2.2.5

USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    default-libmysqlclient-dev

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dags /opt/airflow/dags


# Create processed_data directories
RUN mkdir -p /opt/airflow/processed_data/cleaned_data /opt/airflow/processed_data/bad_data /opt/airflow/processed_data/combined_data /opt/airflow/processed_data/reports