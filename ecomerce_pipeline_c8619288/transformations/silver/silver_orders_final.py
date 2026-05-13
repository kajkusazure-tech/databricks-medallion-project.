from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


@dp.materialized_view(
    name="project.ecomerce_pipeline.silver_orders_final",
    comment="Validated and deduplicated order events ready for downstream analytics.",
    cluster_by=["customer_id", "order_date"]
)
@dp.expect_all({
    "order_date_present": "order_date IS NOT NULL",
    "unit_price_non_negative": "unit_price >= 0",
    "quantity_non_zero": "quantity <> 0"
})
def silver_orders_final():
    clean = spark.read.table("project.ecomerce_pipeline.silver_orders_clean")

    valid = clean.filter(
        (~F.col("has_rescued_data"))
        & F.col("invoice_no").isNotNull()
        & (F.trim(F.col("invoice_no")) != "")
        & F.col("stock_code").isNotNull()
        & (F.trim(F.col("stock_code")) != "")
        & F.col("product_description").isNotNull()
        & (F.trim(F.col("product_description")) != "")
        & (F.col("product_description") != "?")
        & F.col("invoice_timestamp").isNotNull()
        & F.col("quantity").isNotNull()
        & (F.col("quantity") != 0)
        & F.col("unit_price").isNotNull()
        & (F.col("unit_price") >= 0)
    )

    dedup_window = Window.partitionBy(
        "invoice_no", "stock_code", "customer_id", "invoice_timestamp", "quantity", "unit_price", "order_status", "event_source", "promo_code"
    ).orderBy(F.col("ingest_ts").desc_nulls_last(), F.col("ingest_batch_id").desc_nulls_last())

    return valid.withColumn("row_num", F.row_number().over(dedup_window)).filter(F.col("row_num") == 1).drop("row_num")
