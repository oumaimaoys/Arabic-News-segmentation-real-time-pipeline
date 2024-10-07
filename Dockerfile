# Use the correct Airflow version base image
FROM apache/airflow:2.3.1

# Switch to root user to install system packages
USER root

# Set non-interactive mode to avoid prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

# Update package list and install OpenJDK and procps
RUN apt-get update --allow-releaseinfo-change && \
    apt-get install -y openjdk-11-jdk procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$PATH:$JAVA_HOME/bin"


# Switch back to airflow user
USER airflow

# Copy requirements.txt to the container
COPY requirements.txt /requirements.txt

# Install required Python packages, including Airflow
RUN pip install --no-cache-dir -r /requirements.txt

# Ensure the Airflow database is up-to-date
RUN airflow db upgrade
