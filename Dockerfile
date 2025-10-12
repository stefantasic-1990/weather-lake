FROM apache/airflow:3.1.0

# Install Java and set JAVA_HOME
USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
USER airflow

# Install Python dependencies via requirements.txt in the repo
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt