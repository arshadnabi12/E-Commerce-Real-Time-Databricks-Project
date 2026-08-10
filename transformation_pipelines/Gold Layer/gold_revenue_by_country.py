import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="gold_revenue_by_country"
)

def gold_revenue_by_country():
    customers=dlt.read("silver_customers")
    orders=dlt.read("silver_orders")
    return(
         orders.join(customers,on="customer_id",how="inner")
        .groupBy("customer_country")
        .agg(
            count("order_id").alias("total_orders"),
            round(sum("amount"),2).alias("total_revenue_in_$")
        )
    )