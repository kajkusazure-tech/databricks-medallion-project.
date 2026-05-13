from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="project.ecomerce_pipeline.silver_orders_rejected",
    comment="Rejected order events that fail the silver validation rules."
)
def silver_orders_rejected():
    clean = spark.read.table("project.ecomerce_pipeline.silver_orders_clean")

    rejected = (
        clean.withColumn(
            "rejection_reason",
            F.when(F.col("has_rescued_data"), F.lit("rescued_data_present"))
            .when(F.col("invoice_no").isNull() | (F.trim(F.col("invoice_no")) == ""), F.lit("missing_invoice_no"))
            .when(F.col("stock_code").isNull() | (F.trim(F.col("stock_code")) == ""), F.lit("missing_stock_code"))
            .when(F.col("product_description").isNull() | (F.trim(F.col("product_description")) == "") | (F.col("product_description") == "?"), F.lit("invalid_description"))
            .when(F.col("invoice_timestamp").isNull(), F.lit("invalid_invoice_timestamp"))
            .when(F.col("quantity").isNull() | (F.col("quantity") == 0), F.lit("invalid_quantity"))
            .when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), F.lit("invalid_unit_price"))
        )
    )

    return rejected.filter(F.col("rejection_reason").isNotNull())
