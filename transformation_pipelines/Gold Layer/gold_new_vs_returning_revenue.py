from pyspark.sql.functions import *
import dlt

@dlt.table(
    name="gold_new_vs_returning_revenue"
)

def gold_new_vs_returning_revenue():
    customers=dlt.read("silver_customers")
    orders=dlt.read("silver_orders")
    return(
        orders.join(customers,on="customer_id",how="inner")
        .withColumn(
            "customer_type",
            when(datediff(current_date(),col("signup_date"))<=30,"New")
            .otherwise("Existing")
        )
        .groupBy("customer_type")
        .agg(
            count("order_id").alias("total_orders"),
            round(sum("amount"),2).alias("total_revenue_in_$")
        )
    )