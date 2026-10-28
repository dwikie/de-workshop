FROM flink:2.2.1-scala_2.12-java21

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    python3 python3-venv python3-pip python3-dev \
    build-essential curl \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/pyflink-venv \
    && /opt/pyflink-venv/bin/pip install --no-cache-dir apache-flink==2.2.1 \
    && chown -R flink:flink /opt/pyflink-venv

# Kafka connector for Flink 2.2
RUN curl -fsSL \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/5.0.0-2.2/flink-connector-kafka-5.0.0-2.2.jar \
    -o /opt/flink/lib/flink-connector-kafka-5.0.0-2.2.jar \
    && curl -fsSL \
    https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/4.2.0/kafka-clients-4.2.0.jar \
    -o /opt/flink/lib/kafka-clients-4.2.0.jar

# Parquet connector
RUN curl -fsSL \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-parquet/2.2.1/flink-parquet-2.2.1.jar \
    -o /opt/flink/lib/flink-parquet-2.2.1.jar \
    && curl -fsSL \
    https://repo.maven.apache.org/maven2/org/apache/parquet/parquet-hadoop/1.15.2/parquet-hadoop-1.15.2.jar \
    -o /opt/flink/lib/parquet-hadoop-1.15.2.jar \
    && curl -fsSL \
    https://repo.maven.apache.org/maven2/org/apache/parquet/parquet-column/1.15.2/parquet-column-1.15.2.jar \
    -o /opt/flink/lib/parquet-column-1.15.2.jar \
    && curl -fsSL \
    https://repo.maven.apache.org/maven2/org/apache/parquet/parquet-common/1.15.2/parquet-common-1.15.2.jar \
    -o /opt/flink/lib/parquet-common-1.15.2.jar \
    && curl -fsSL \
    https://repo.maven.apache.org/maven2/org/apache/parquet/parquet-encoding/1.15.2/parquet-encoding-1.15.2.jar \
    -o /opt/flink/lib/parquet-encoding-1.15.2.jar \
    && curl -fsSL \
    https://repo.maven.apache.org/maven2/org/apache/parquet/parquet-format-structures/1.15.2/parquet-format-structures-1.15.2.jar \
    -o /opt/flink/lib/parquet-format-structures-1.15.2.jar \
    && curl -fsSL \
    https://repo.maven.apache.org/maven2/org/apache/parquet/parquet-jackson/1.15.2/parquet-jackson-1.15.2.jar \
    -o /opt/flink/lib/parquet-jackson-1.15.2.jar \
    && curl -fsSL \
    https://repo.maven.apache.org/maven2/io/airlift/aircompressor/2.0.2/aircompressor-2.0.2.jar \
    -o /opt/flink/lib/aircompressor-2.0.2.jar \
    && curl -fsSL \
    https://repo.maven.apache.org/maven2/com/google/protobuf/protobuf-java/4.32.1/protobuf-java-4.32.1.jar \
    -o /opt/flink/lib/protobuf-java-4.32.1.jar \
    && curl -fsSL \
    https://repo.maven.apache.org/maven2/com/github/luben/zstd-jni/1.5.6-6/zstd-jni-1.5.6-6.jar \
    -o /opt/flink/lib/zstd-jni-1.5.6-6.jar

# Hadoop dependencies used by the existing Parquet setup
RUN curl -fsSL \
    https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar \
    -o /opt/flink/lib/hadoop-common-3.3.4.jar \
    && curl -fsSL \
    https://repo.maven.apache.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.1.1/hadoop-shaded-guava-1.1.1.jar \
    -o /opt/flink/lib/hadoop-shaded-guava-1.1.1.jar \
    && curl -fsSL \
    https://repo.maven.apache.org/maven2/com/fasterxml/woodstox/woodstox-core/5.3.0/woodstox-core-5.3.0.jar \
    -o /opt/flink/lib/woodstox-core-5.3.0.jar \
    && curl -fsSL \
    https://repo.maven.apache.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar \
    -o /opt/flink/lib/stax2-api-4.2.1.jar \
    && curl -fsSL \
    https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/3.3.4/hadoop-mapreduce-client-core-3.3.4.jar \
    -o /opt/flink/lib/hadoop-mapreduce-client-core-3.3.4.jar

# Iceberg connector for Flink 2.2
RUN curl -fsSL \
    https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.2/1.12.0/iceberg-flink-runtime-2.2-1.12.0.jar \
    -o /opt/flink/lib/iceberg-flink-runtime-2.2-1.12.0.jar \
    && curl -fsSL \
    https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar \
    -o /opt/flink/lib/iceberg-aws-bundle-1.12.0.jar

RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
    && cp /opt/flink/opt/flink-s3-fs-hadoop-2.2.1.jar \
    /opt/flink/plugins/s3-fs-hadoop/ \
    && chown -R flink:flink /opt/flink/plugins/s3-fs-hadoop

ENV PATH="/opt/pyflink-venv/bin:${PATH}"

USER flink