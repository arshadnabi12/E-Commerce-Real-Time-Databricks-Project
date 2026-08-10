import dlt
from pyspark.sql.functions import count,sum

@dlt.table(
    name="gold_orders_status_breakdown"
)

def gold_order_status_breakdown():
    return(
        dlt.read("silver_orders")
        .groupBy("status")
        .agg(
            count("order_id").alias("orders_count"),
            sum("amount").alias("total_revenue_in_$")
        )
    )