FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (wget for JDBC driver, Java for PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends wget default-jdk procps && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download PostgreSQL JDBC driver
RUN mkdir -p /app/jars && \
    wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -P /app/jars/

COPY . .

CMD ["python", "main.py"]
