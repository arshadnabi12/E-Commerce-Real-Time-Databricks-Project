import dlt
from pyspark.sql.functions import date_trunc,count,sum,round

@dlt.table(
    name="gold_hourly_order_volumne"
)

def gold_hourly_order_volume():
    return (
        dlt.read("silver_orders")
        .withColumn("order_hour", date_trunc("hour","order_timestamp"))
        .groupBy("order_hour")
        .agg(
            round(sum("amount"),2).alias("total_revenue_in_$"),
            count("order_id").alias("total_orders")
        )
    )