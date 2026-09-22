FROM apache/spark:4.0.4-scala2.13-java21-python3-ubuntu

USER root

ARG SPARK_KAFKA_VERSION=4.0.4
ARG KAFKA_CLIENTS_VERSION=3.9.0
ARG COMMONS_POOL2_VERSION=2.12.0
ARG DELTA_VERSION=4.0.0
ARG POSTGRES_JDBC_VERSION=42.7.4
ARG MAVEN_BASE=https://repo1.maven.org/maven2

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Bake the streaming connectors, Delta Lake, and the Postgres JDBC driver into
# $SPARK_HOME/jars so the job starts with no runtime package resolution.
RUN set -eux; \
    fetch() { curl -fsSL --retry 3 -o "${SPARK_HOME}/jars/$1" "${MAVEN_BASE}/$2"; }; \
    fetch "spark-sql-kafka-0-10_2.13-${SPARK_KAFKA_VERSION}.jar" \
      "org/apache/spark/spark-sql-kafka-0-10_2.13/${SPARK_KAFKA_VERSION}/spark-sql-kafka-0-10_2.13-${SPARK_KAFKA_VERSION}.jar"; \
    fetch "spark-token-provider-kafka-0-10_2.13-${SPARK_KAFKA_VERSION}.jar" \
      "org/apache/spark/spark-token-provider-kafka-0-10_2.13/${SPARK_KAFKA_VERSION}/spark-token-provider-kafka-0-10_2.13-${SPARK_KAFKA_VERSION}.jar"; \
    fetch "kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    fetch "commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
      "org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"; \
    fetch "delta-spark_2.13-${DELTA_VERSION}.jar" \
      "io/delta/delta-spark_2.13/${DELTA_VERSION}/delta-spark_2.13-${DELTA_VERSION}.jar"; \
    fetch "delta-storage-${DELTA_VERSION}.jar" \
      "io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar"; \
    fetch "postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      "org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

WORKDIR /app

# Install the project (config/validation deps), psycopg for the transactional
# upsert, and the Delta Python API. The Delta jar is baked above; --no-deps keeps
# pip from pulling its own pyspark and shadowing the bundled one.
COPY pyproject.toml README.md ./
COPY src ./src
RUN python3 -m pip install --no-cache-dir . "psycopg[binary]>=3.2" \
 && python3 -m pip install --no-cache-dir --no-deps "delta-spark==${DELTA_VERSION}"

COPY config ./config
COPY data ./data

# Writable locations for checkpoints and the Delta lake, owned by the spark user.
RUN mkdir -p /opt/tickstream/checkpoints /opt/tickstream/delta \
 && chown -R spark:spark /opt/tickstream

ENV PYTHONPATH=/app/src \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

USER spark
