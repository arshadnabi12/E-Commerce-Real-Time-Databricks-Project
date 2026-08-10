import dlt
from pyspark.sql.functions import current_timestamp

BUCKET_NAME=spark.conf.get("BUCKET_NAME")
@dlt.table(
    name="bronze_orders",
    comment="Raw e-commerce orders events ingested from S3 via Auto Loader"
)

def bronze_orders():
    return(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","json")
        .option("cloudFiles.schemaLocation",f's3a://{BUCKET_NAME}/checkpoints/bronze_orders/')
        .load(f's3a://{BUCKET_NAME}/raw/orders/')
        .withColumn("_ingested_at",current_timestamp())
    )
