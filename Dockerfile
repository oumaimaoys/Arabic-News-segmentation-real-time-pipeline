# Use the correct Airflow version base image
FROM apache/airflow:2.3.1

# Switch to root user to install system packages
USER root

# Install procps to provide the 'ps' command for monitoring processes
RUN apt-get update && apt-get install -y procps

# Switch back to airflow user
USER airflow

# Copy requirements.txt to the container
COPY requirements.txt /requirements.txt

# Install required Python packages, including Airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# Ensure the Airflow database is up-to-date
RUN airflow db upgrade

# Set JAVA_HOME for Spark compatibility
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
