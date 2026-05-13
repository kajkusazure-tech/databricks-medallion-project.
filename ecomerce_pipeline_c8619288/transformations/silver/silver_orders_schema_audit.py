from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="project.ecomerce_pipeline.silver_orders_schema_audit",
    comment="Audit view for rescued data, schema-sensitive fields, and source-to-silver column coverage.",
    cluster_by=["source_layer", "column_name"]
)
def silver_orders_schema_audit():
    bronze = spark.read.table("project.ecomerce_pipeline.bronze_orders_raw")
    clean = spark.read.table("project.ecomerce_pipeline.silver_orders_clean")

    bronze_columns = [
        "InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice",
        "CustomerID", "Country", "order_status", "event_source", "ingest_batch_id", "ingest_ts",
        "promo_code", "_rescued_data"
    ]
    clean_columns = [
        "invoice_no", "stock_code", "product_description", "quantity", "invoice_date_raw", "unit_price",
        "customer_id", "country", "order_status", "event_source", "ingest_batch_id", "ingest_ts",
        "promo_code", "rescued_data", "invoice_timestamp", "order_date", "has_rescued_data",
        "is_return", "line_amount"
    ]

    bronze_summary = bronze.agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("_rescued_data").isNotNull(), 1).otherwise(0)).alias("rescued_row_count")
    ).withColumn("source_layer", F.lit("bronze"))

    clean_summary = clean.agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("has_rescued_data"), 1).otherwise(0)).alias("rescued_row_count")
    ).withColumn("source_layer", F.lit("silver_clean"))

    summary = bronze_summary.unionByName(clean_summary)

    return summary.select(
        F.col("source_layer"),
        F.col("row_count"),
        F.col("rescued_row_count"),
        F.when(F.col("source_layer") == "bronze", F.array(*[F.lit(column_name) for column_name in bronze_columns])).otherwise(F.array(*[F.lit(column_name) for column_name in clean_columns])).alias("known_columns")
    ).select(
        F.col("source_layer"),
        F.col("row_count"),
        F.col("rescued_row_count"),
        F.explode(F.col("known_columns")).alias("column_name")
    )
