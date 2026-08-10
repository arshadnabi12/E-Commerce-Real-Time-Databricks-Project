import dlt
from pyspark.sql.functions import col,count,explode,from_json
from pyspark.sql.types import StringType,ArrayType,StructField,StructType,IntegerType,DoubleType


@dlt.table(
    name="silver_orders",
    comment="Cleaned, validated orders - deduplicated,bad rows removed"
)
@dlt.expect_or_drop("valid_amount","amount>0")
@dlt.expect_or_drop("valid_order_id","order_id is NOT NULL")
@dlt.expect_or_drop("valid_status","status IN ('placed','confirmed','shipped','delivered','cancelled')")

def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
        .dropDuplicates(['order_id'])
        .select(
            "order_id",
            "customer_id",
            "amount",
            "status",
            col('order_timestamp').cast('timestamp').alias('order_timestamp'),"_ingested_at"
        )
    )

@dlt.table(
    name="orders_bad_records",
    comment="Orders that failed data quality checks - kept for visibility, not deleted"
)

def orders_bad_records():
    return (
        dlt.readStream("bronze_orders")
        .filter(
            (col("amount")<=0)|
            col("order_id").isNull()|
            (~col("status").isin('placed','confirmed','shipped','delivered','cancelled'))
        )
    )

items_schema=ArrayType(StructType([
    StructField("product",StringType()),
    StructField("quantity",IntegerType()),
    StructField("unit_price",DoubleType())
]))
@dlt.table(
    name="silver_order_items",
    comment="Flattened order line items - one row per product per order"
)

@dlt.expect_or_drop("valid_product","product IS NOT NULL")
@dlt.expect_or_drop("valid_quantity","quantity>0")
@dlt.expect_or_drop("valid_unit_price","unit_price>0")

def silver_order_items():
    items=(
        dlt.readStream('bronze_orders')
        .dropDuplicates(['order_id'])
        .withColumn("items_parsed",from_json(col("items"),items_schema))
        .select(
            "order_id",
            explode("items_parsed").alias("item")
        )
        .select(
            "order_id",
            col("item.product").alias("product"),
            col("item.quantity").alias("quantity"),
            col("item.unit_price").alias("unit_price")
        )
    )
    valid_orders=dlt.read("silver_orders").select("order_id")
    return  items.join(valid_orders,on="order_id",how="inner")
    
