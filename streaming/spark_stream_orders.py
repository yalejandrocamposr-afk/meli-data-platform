import json
import time

from google.cloud import pubsub_v1
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, current_timestamp, from_utc_timestamp

from utils.logger import get_logger

logger = get_logger("spark-stream")

logger.info("Starting streaming job")

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
    logger.info("Received new event from Pub/Sub")
    message.ack()  

# Cada vez que llegue un mensaje ejecutar callback
subscriber.subscribe(subscription_path, callback=callback) 


# Calidad de datos
def validate_orders(df):

    df = df.dropna(subset=["order_id"])
    df = df.filter("price > 0")
    df = df.dropDuplicates(["order_id"])
    return df


logger.info("Listening for messages...")

def process_batch(batch_messages):

    start_time = time.time() # Medición tiempo de ingesta - Inicio

    logger.info(f"Processing batch with {len(batch_messages)} records")

    # Convertir lista de eventos en DataFrame de Spark
    df = spark.createDataFrame(batch_messages)
    
    original_count = df.count() # Conteo de registros crudos
    
    df = validate_orders(df)

    valid_count = df.count() # Conteo de registros validos 
    invalid_count = original_count - valid_count # Conteo de registros invalidos

    logger.info(f"Valid records: {valid_count}")
    logger.info(f"Invalid records removed: {invalid_count}")

    processing_time = round(time.time() - start_time, 2) # Medición tiempo de ingesta - Final
    logger.info(f"Batch processing time: {processing_time} seconds")

    # Calcular valor total de cada orden
    df = df.withColumn(
        "total_value",
        col("price") * col("quantity")
    )

    logger.info("Writing RAW events table orders_stream")
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

    logger.info("Writing analytics table sales_by_category")
    # Guardar analitycs
    sales_by_category.write \
        .format("bigquery") \
        .option("table", "meli-data-platform.meli_analytics.sales_by_category") \
        .option("temporaryGcsBucket", "meli-data-platform-temp") \
        .mode("append") \
        .save()


# Loop continuo que procesa eventos cada cierto intervalo (micro-batch)
while True:

    logger.info("Collecting events for next micro-batch")

    time.sleep(30)  # ventana de micro-batch (30 segundos)

    if len(messages) > 0:
        batch = messages.copy()
        messages.clear()  # limpiar buffer para el siguiente batch

        process_batch(batch)
    else:
        logger.info("No events received in this batch.")