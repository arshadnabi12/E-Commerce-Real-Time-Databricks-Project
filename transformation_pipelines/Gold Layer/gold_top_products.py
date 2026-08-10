import dlt
from pyspark.sql.functions import sum,col,round

@dlt.table(
    name="gold_top_products"
)

def gold_top_products():
    return(
        dlt.read("silver_order_items")
        .withColumn("line_revenue",col("quantity")*col("unit_price"))
        .groupBy("product")
        .agg(
            sum("quantity").alias("total_quantity"),
            round(sum("line_revenue"),2).alias("total_revenue_in_$")
        )
        .orderBy(col("total_revenue_in_$").desc())
    )