import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, current_timestamp, from_utc_timestamp
from google.cloud import pubsub_v1

project_id = "meli-data-platform"
subscription_id = "spark-orders-subscription"

# Crear SparkSession y configurar autenticación para GCP
spark = (
    SparkSession.builder
    .appName("meli-streaming")
    .config(
        "spark.hadoop.google.cloud.auth.service.account.enable",
        "true"
    )
    .config(
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
        "/root/.config/gcloud/application_default_credentials.json"
    )
    .config(
        "spark.hadoop.fs.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    )
    .config(
        "spark.hadoop.fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
    )
    .getOrCreate()
)

# crea un cliente que escucha mensajes de Pub/Sub
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)


# Buffer de eventos
messages = [] 

# Callback que se ejecuta cada vez que llega un evento desde Pub/Sub
def callback(message):
    data = json.loads(message.data.decode("utf-8"))
    messages.append(data)
    # confirmar consumo del mensaje
    message.ack()  

# Cada vez que llegue un mensaje ejecutar callback
subscriber.subscribe(subscription_path, callback=callback) 


print("Listening for messages...")

def process_batch(batch_messages):
    # Convertir lista de eventos en DataFrame de Spark
    df = spark.createDataFrame(batch_messages)

    # Calcular valor total de cada orden
    df = df.withColumn(
        "total_value",
        col("price") * col("quantity")
    )

    # Guardar RAW events
    df.write \
        .format("bigquery") \
        .option("table", "meli-data-platform.meli_raw.orders_stream") \
        .option("temporaryGcsBucket", "meli-data-platform-temp") \
        .mode("append") \
        .save()
    
    ## Transformación de datos

    # Agregación: ventas totales por categoría
    sales_by_category = df.groupBy("category").agg(
    sum("total_value").alias("total_sales")
)
    # Agregar timestamp del momento en que se procesa el batch
    sales_by_category = sales_by_category.withColumn(
        "processed_at",
        from_utc_timestamp(current_timestamp(), "America/Bogota")
)

    sales_by_category.show()

    # Escribir resultado de transformación BigQuery usando GCS como staging
    sales_by_category.write \
        .format("bigquery") \
        .option("table", "meli-data-platform.meli_pipeline.sales_by_category") \
        .option("temporaryGcsBucket", "meli-data-platform-temp") \
        .mode("append") \
        .save()


# Loop continuo que procesa eventos cada cierto intervalo (micro-batch)
while True:

    print("Collecting events for batch...")

    time.sleep(30)  # ventana de micro-batch (30 segundos)

    if len(messages) > 0:
        batch = messages.copy()
        messages.clear()  # limpiar buffer para el siguiente batch

        process_batch(batch)
    else:
        print("No events received in this batch.")