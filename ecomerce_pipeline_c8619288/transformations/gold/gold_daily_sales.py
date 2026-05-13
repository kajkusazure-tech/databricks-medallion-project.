from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="project.ecomerce_pipeline.gold_daily_sales",
    comment="Gold daily sales aggregate by order date and country from the latest-state silver orders table.",
    cluster_by=["order_date", "country"],
    table_properties={
        "delta.enableChangeDataFeed": "true"
    }
)
@dp.expect_or_fail("order_date_present", "order_date IS NOT NULL")
@dp.expect_or_drop("country_present", "country IS NOT NULL AND trim(country) <> ''")
@dp.expect("non_negative_sales", "gross_sales_amount >= 0")
def gold_daily_sales():
    latest = spark.read.table("project.ecomerce_pipeline.silver_orders_latest")

    return (
        latest.filter(
            F.col("order_date").isNotNull()
            & F.col("country").isNotNull()
            & (F.trim(F.col("country")) != "")
        )
        .groupBy("order_date", "country")
        .agg(
            F.countDistinct("invoice_no").alias("orders_count"),
            F.count("*").alias("order_lines_count"),
            F.countDistinct("customer_id").alias("customers_count"),
            F.sum("quantity").cast("bigint").alias("units_sold"),
            F.round(F.sum("line_amount"), 2).alias("gross_sales_amount"),
            F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
            F.sum(
                F.when(
                    F.col("promo_code").isNotNull() & (F.trim(F.col("promo_code")) != ""),
                    1
                ).otherwise(0)
            ).cast("bigint").alias("promo_order_lines_count"),
            F.max("invoice_timestamp").alias("latest_invoice_ts")
        )
        .withColumn(
            "avg_order_value",
            F.round(F.col("gross_sales_amount") / F.col("orders_count"), 2)
        )
    )
