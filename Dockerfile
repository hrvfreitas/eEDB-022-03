FROM python:3.11-slim

# Java é necessário para rodar o Spark (PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV SPARK_MASTER=local[*]

WORKDIR /opt

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY setup_database.sql .

CMD ["python3", "src/run_all.py"]
