import dlt
from pyspark.sql.functions import avg,round

@dlt.table(
    name="gold_avg_order_value"
)

def gold_avg_order_value():
    return(
        dlt.read("silver_orders")
        .groupBy("status")
        .agg(round(avg("amount"),2).alias("average_order_value_in_$"))
    )
    
    