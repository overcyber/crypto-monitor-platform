FROM flink:1.20.5-scala_2.12-java17
ARG ICEBERG_VERSION=1.11.0
ARG FLINK_KAFKA_CONNECTOR_VERSION=3.4.0-1.20
ARG HADOOP_VERSION=3.4.1
USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl gettext-base ca-certificates jq \
 && rm -rf /var/lib/apt/lists/* \
 && curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar \
 && curl -fsSL -o /opt/flink/lib/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar \
 && curl -fsSL -o /opt/flink/lib/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
 && curl -fsSL -o /opt/flink/lib/hadoop-client-api-${HADOOP_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar \
 && curl -fsSL -o /opt/flink/lib/hadoop-client-runtime-${HADOOP_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar
USER flink
