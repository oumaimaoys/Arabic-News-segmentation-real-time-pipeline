# Use the correct Airflow version base image
FROM apache/airflow:2.3.1

# Copy requirements.txt to the container
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# Run the airflow db upgrade to ensure the database is up-to-date
RUN airflow db upgrade