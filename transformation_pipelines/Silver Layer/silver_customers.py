from pyspark.sql.functions import col
import dlt

@dlt.table(
    name="silver_customers",
    comment="Cleansed customers dimension data"
)

def silver_customers():
    return(
        dlt.readStream("bronze_customers")
        .dropDuplicates(["customer_id"])
        .select(
            "customer_id",
            col("name").alias("customer_name"),
            col("email").alias("customer_email"),
            col("country").alias("customer_country"),
            col("age").alias("customer_age"),
            col("signup_date").cast("date").alias("signup_date"),
            "_ingested_at"
        )
    )