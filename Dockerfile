# Start from the base image that includes Spark and Hadoop
FROM datamechanics/spark:3.1.3-hadoop-3.2.0-java-11-scala-2.12-python-3.8-latest as spark_base

# Set working directory for Spark
WORKDIR /opt/spark/

# Download AWS SDK and move it to the jars directory
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.227/aws-java-sdk-bundle-1.12.227.jar && \
    mv aws-java-sdk-bundle-1.12.227.jar /opt/spark/jars

# Set PySpark major Python version
ENV PYSPARK_MAJOR_PYTHON_VERSION=3

# Install Python dependencies for Spark
COPY ./requirements.txt .
RUN pip3 install -r requirements.txt

# Copy the Spark ETL script
COPY ./spark_etl_script.py .

# Use a second base image for additional Spark installation
FROM bitnami/spark:3 as spark_additional

# Switch to root to install additional dependencies
USER root

# Download additional required JARs for Spark
RUN curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.231/aws-java-sdk-bundle-1.12.231.jar --output /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.231.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar --output /opt/bitnami/spark/jars/hadoop-aws-3.3.1.jar && \
    curl -L https://repo1.maven.org/maven2/net/java/dev/jets3t/jets3t/0.9.4/jets3t-0.9.4.jar --output /opt/bitnami/spark/jars/jets3t-0.9.4.jar

# Copy the DAGs and Spark ETL script
COPY ./dags/spark_etl_script_docker.py /opt/bitnami/spark

# Install additional Python dependencies if needed
COPY ./requirements_copy.txt /
RUN pip install -r /requirements_copy.txt

# Final base image for Apache Airflow
FROM apache/airflow:2.10.1 as airflow_base

# Switch to root to install Java and other dependencies
USER root

# Install OpenJDK-11 and other necessary tools
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/

# Switch back to the airflow user
USER airflow

# Install Python dependencies for Airflow
COPY ./requirements.txt /
RUN pip install -r /requirements.txt

# Copy Airflow DAGs to the appropriate directory
COPY --chown=airflow:root ./dags /opt/airflow/dags

# Set the working directory for Airflow (optional)
WORKDIR /opt/airflow
