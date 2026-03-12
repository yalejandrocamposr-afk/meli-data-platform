FROM apache/spark:3.5.1

USER root

RUN pip install google-cloud-pubsub google-cloud-bigquery faker pandas pyspark

# instalar conectores necesarios
RUN wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar \
    -O /opt/spark/jars/gcs-connector-hadoop3-latest.jar

RUN wget https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.36.1.jar \
    -O /opt/spark/jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar

WORKDIR /app