# Lifted from: https://github.com/naushadh/hive-metastore/Dockerfile

FROM openjdk:16-slim

ARG HADOOP_VERSION=3.3.6
ARG HIVE_METASTORE_VERSION=3.1.3
ARG POSTGRES_CONNECTOR_VERSION=42.7.1
ARG AWS_JAVA_SDK_BUNDLE_VERSION=1.12.367

ENV HADOOP_HOME="/opt/hadoop"
ENV HIVE_METASTORE_HOME="/opt/hive-metastore"
ENV PATH="/opt/spark/bin:/opt/hadoop/bin:${PATH}"
ENV DATABASE_DRIVER=org.postgresql.Driver
ENV DATABASE_TYPE=postgres
ENV DATABASE_TYPE_JDBC=postgresql
ENV DATABASE_PORT=5432

WORKDIR /app
ENV TEMPLATE_PATH "/app/template"

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      curl \
      gettext \
      net-tools && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Hadoop
# RUN curl "https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz" | tar xvz -C "/opt/" && \
RUN curl "https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" | tar xvz -C "/opt/" && \
    ln -s "${HADOOP_HOME}-${HADOOP_VERSION}" "${HADOOP_HOME}" && \
    rm -r "${HADOOP_HOME}/share/doc"

# Add S3a jars to the classpath
RUN cp "${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-${HADOOP_VERSION}.jar" "${HADOOP_HOME}/share/hadoop/common/lib/" && \
    cp "${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-${AWS_JAVA_SDK_BUNDLE_VERSION}.jar" "${HADOOP_HOME}/share/hadoop/common/lib/"

# Install Hive Metastore
RUN curl "https://repo1.maven.org/maven2/org/apache/hive/hive-standalone-metastore/${HIVE_METASTORE_VERSION}/hive-standalone-metastore-${HIVE_METASTORE_VERSION}-bin.tar.gz" \
    | tar xvz -C /opt/ && \
    ln -s "/opt/apache-hive-metastore-${HIVE_METASTORE_VERSION}-bin" "${HIVE_METASTORE_HOME}"

# Install PostgreSQL JDBC Driver
RUN curl -L "https://jdbc.postgresql.org/download/postgresql-${POSTGRES_CONNECTOR_VERSION}.jar" --output /opt/postgresql-$POSTGRES_CONNECTOR_VERSION.jar && \
    cp /opt/postgresql-$POSTGRES_CONNECTOR_VERSION.jar "${HADOOP_HOME}/share/hadoop/common/lib/" && \
    cp /opt/postgresql-$POSTGRES_CONNECTOR_VERSION.jar "${HIVE_METASTORE_HOME}/lib/" && \
    rm "/opt/postgresql-${POSTGRES_CONNECTOR_VERSION}.jar"

# Keep this until this lands: https://issues.apache.org/jira/browse/HIVE-22915
RUN rm "/opt/apache-hive-metastore-${HIVE_METASTORE_VERSION}-bin/lib/guava-19.0.jar" && \
    cp "/opt/hadoop-${HADOOP_VERSION}/share/hadoop/hdfs/lib/guava-27.0-jre.jar" "/opt/apache-hive-metastore-${HIVE_METASTORE_VERSION}-bin/lib/"

# Purge build artifacts
RUN apt-get purge -y --auto-remove curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY config/metastore/template ./template

COPY scripts/metastore/config.sh .

CMD [ "./config.sh" ]
HEALTHCHECK CMD [ "sh", "-c", "netstat -ln | grep 9083" ]