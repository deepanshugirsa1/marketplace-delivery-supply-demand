FROM flink:1.18.1-java11

# PyFlink runtime
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir apache-flink==1.18.1

# SQL connector for Kafka (source and sink)
ARG KAFKA_CONNECTOR=flink-sql-connector-kafka-3.1.0-1.18.jar
RUN wget -q -O /opt/flink/lib/${KAFKA_CONNECTOR} \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/${KAFKA_CONNECTOR}

COPY jobs /opt/jobs
