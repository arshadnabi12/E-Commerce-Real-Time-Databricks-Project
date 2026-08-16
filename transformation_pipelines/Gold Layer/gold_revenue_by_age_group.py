import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="gold_revenue_by_age_group"
)

def gold_revenue_by_age_group():
    customers=dlt.read("silver_customers")
    orders=dlt.read("silver_orders").filter(col('status')=='delivered')
    return(
        orders.join(customers,on="customer_id",how="inner")
        .withColumn(
            "age_group",
            when(col("customer_age")<=25, "18-25")
            .when((col("customer_age")>=26) & (col("customer_age")<=35),"26-35")
            .when((col("customer_age")>=36) & (col("customer_age")<=50),"36-50")
            .otherwise(">50")
        )
        .groupBy("age_group")
        .agg(
            count("order_id").alias("total_orders"),
            round(sum("amount"),2).alias("total_revenue_in_$")
        )
    )