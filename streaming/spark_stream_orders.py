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



messages = [] # Buffer de eventos

# Callback que se ejecuta cada vez que llega un evento desde Pub/Sub
def callback(message):
    data = json.loads(message.data.decode("utf-8"))
    messages.append(data)
    # confirmar consumo del mensaje
    logger.info("Received new event from Pub/Sub")
    message.ack()  

subscriber.subscribe(subscription_path, callback=callback) # Cada vez que llegue un mensaje ejecutar callback


# Calidad de datos
def validate_orders(df):

    df = df.dropna(subset=["order_id"])
    df = df.filter("price > 0")
    df = df.dropDuplicates(["order_id"])
    return df

def write_with_retry(df, table_name, max_retries=3): 
    attempt = 1
    while attempt <= max_retries:
        try:
            logger.info(f"Writing to {table_name} (attempt {attempt})")

            df.write \
                .format("bigquery") \
                .option("table", table_name) \
                .option("temporaryGcsBucket", "meli-data-platform-temp") \
                .mode("append") \
                .save()
            
            logger.info(f"Write successful to {table_name}")
            return

        except Exception as e:

            logger.error(f"Write failed on attempt {attempt}: {str(e)}")
            attempt += 1
            time.sleep(5)

    logger.error(f"All retries failed for table {table_name}")



logger.info("Listening for messages...")

def process_batch(batch_messages):

    start_time = time.time() # Medición tiempo de ingesta - Inicio

    logger.info(f"Processing batch with {len(batch_messages)} records")


    df = spark.createDataFrame(batch_messages) # Convertir lista de eventos en DataFrame de Spark
    
    original_count = df.count() # Conteo de registros crudos
    
    df = validate_orders(df)

    valid_count = df.count() # Conteo de registros validos 
    invalid_count = original_count - valid_count # Conteo de registros invalidos

    logger.info(f"Valid records: {valid_count}")
    logger.info(f"Invalid records removed: {invalid_count}")

    processing_time = round(time.time() - start_time, 2) # Medición tiempo de ingesta - Final
    logger.info(f"Batch processing time: {processing_time} seconds")

    # Calcular valor total de cada orden
    df_raw = df.withColumn(
        "total_value",
        col("price") * col("quantity")
    )

    write_with_retry(df_raw, "meli-data-platform.meli_raw.orders_stream") # Guardar RAW events
    
    #### Transformación de datos ####

    # Agregación: ventas totales por categoría
    df_sales_by_category = df_raw.groupBy("category").agg(
    sum("total_value").alias("total_sales")
)
    # Agregar timestamp del momento en que se procesa el batch
    df_sales_by_category = df_sales_by_category.withColumn(
        "processed_at",
        from_utc_timestamp(current_timestamp(), "America/Bogota")
)
    write_with_retry(df_sales_by_category, "meli-data-platform.meli_analytics.sales_by_category") # Guardar analitycs

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