from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="project.ecomerce_pipeline.silver_orders_cdc_source",
    comment="CDC-ready streaming order events with stable keys, sequencing metadata, and operation flags for latest-state processing.",
    cluster_by=["customer_id", "order_date"]
)
@dp.expect_all({
    "order_line_key_present": "order_line_key IS NOT NULL",
    "cdc_sequence_present": "cdc_sequence_ts IS NOT NULL",
    "valid_cdc_operation": "cdc_operation IN ('UPSERT', 'DELETE')"
})
def silver_orders_cdc_source():
    bronze = spark.readStream.table("project.ecomerce_pipeline.bronze_orders_raw")

    cleaned = (
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

    valid = cleaned.filter(
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

    return valid.select(
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("invoice_no"), F.lit("")),
                F.coalesce(F.col("stock_code"), F.lit("")),
                F.coalesce(F.col("customer_id").cast("string"), F.lit(""))
            ),
            256
        ).alias("order_line_key"),
        F.coalesce(F.col("ingest_ts"), F.col("invoice_timestamp")).alias("cdc_sequence_ts"),
        F.coalesce(F.col("ingest_batch_id"), F.lit("unknown")).alias("cdc_sequence_batch_id"),
        F.when(
            F.coalesce(F.col("order_status"), F.lit(""))
            .isin("cancelled", "canceled", "deleted"),
            F.lit("DELETE")
        )
        .when(F.col("is_return"), F.lit("DELETE"))
        .otherwise(F.lit("UPSERT"))
        .alias("cdc_operation"),
        F.col("invoice_no"),
        F.col("stock_code"),
        F.col("customer_id"),
        F.col("product_description"),
        F.col("quantity"),
        F.col("unit_price"),
        F.col("line_amount"),
        F.col("invoice_date_raw"),
        F.col("invoice_timestamp"),
        F.col("order_date"),
        F.col("country"),
        F.col("order_status"),
        F.col("event_source"),
        F.col("promo_code"),
        F.col("ingest_batch_id"),
        F.col("ingest_ts"),
        F.col("rescued_data"),
        F.col("has_rescued_data"),
        F.col("is_return")
    )
