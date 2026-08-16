from pyspark.sql.functions import sum,to_date,count,col,round
import dlt

@dlt.table(
    name="gold_daily_revenue"
)

def gold_daily_revenue():
    return(
        dlt.read("silver_orders")
        .filter(col('status')=='delivered')
        .withColumn("order_date",to_date("order_timestamp"))        
        .groupBy("order_date")
        .agg(
            round(sum("amount"),2).alias("total_revenue_in_$"),
            count("order_id").alias("total_orders")
        )        
        
    )