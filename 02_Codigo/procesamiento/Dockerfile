# Entorno de procesamiento: PySpark 3.5 + Java 17 + driver JDBC de PostgreSQL (arm64 y amd64)
FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless curl procps \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
RUN pip install --no-cache-dir pyspark==3.5.3 psycopg2-binary==2.9.9 numpy==1.26.4
RUN curl -fsSL -o /opt/postgresql.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
WORKDIR /app
