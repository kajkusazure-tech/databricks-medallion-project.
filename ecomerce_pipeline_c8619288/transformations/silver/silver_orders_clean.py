from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="project.ecomerce_pipeline.silver_orders_clean",
    comment="Standardized order events with parsed timestamps and reusable quality fields."
)
def silver_orders_clean():
    bronze = spark.read.table("project.ecomerce_pipeline.bronze_orders_raw")

    return (
        bronze.select(
            F.col("InvoiceNo").alias("invoice_no"),
            F.col("StockCode").alias("stock_code"),
            F.trim(F.col("Description")).alias("product_description"),
            F.col("Quantity").cast("int").alias("quantity"),
            F.col("InvoiceDate").alias("invoice_date_raw"),
            F.col("UnitPrice").cast("double").alias("unit_price"),
            F.col("CustomerID").cast("bigint").alias("customer_id"),
            F.trim(F.col("Country")).alias("country"),
            F.lower(F.trim(F.col("order_status"))).alias("order_status"),
            F.lower(F.trim(F.col("event_source"))).alias("event_source"),
            F.col("ingest_batch_id"),
            F.col("ingest_ts"),
            F.trim(F.col("promo_code")).alias("promo_code"),
            F.col("_rescued_data").alias("rescued_data")
        )
        .withColumn("invoice_timestamp", F.to_timestamp(F.col("invoice_date_raw"), "M/d/yyyy H:mm"))
        .withColumn("order_date", F.to_date(F.col("invoice_timestamp")))
        .withColumn("has_rescued_data", F.col("rescued_data").isNotNull())
        .withColumn("is_return", F.col("invoice_no").startswith("C") | (F.col("quantity") < 0))
        .withColumn("line_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    )
