import dlt
from pyspark.sql.functions import current_timestamp

BUCKET_NAME=spark.conf.get("BUCKET_NAME")

@dlt.table(
    name="bronze_customers",
    comment="Raw customers data ingested fom S3 via Auto Loader"
)

def bronze_customers():
    return(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","json")
        .option("cloudFiles.schemaLocation",f"s3a://{BUCKET_NAME}/checkpoints/bronze_customers/")
        .load(f"s3a://{BUCKET_NAME}/raw/customers/")
        .withColumn("_ingested_at",current_timestamp())
    )