from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="project.ecomerce_pipeline.ops_orders_quality_summary",
    comment="Operational summary of order quality, rescued data, and CDC action volumes.",
    cluster_by=["metric_group", "metric_name"]
)
def ops_orders_quality_summary():
    clean = spark.read.table("project.ecomerce_pipeline.silver_orders_clean")
    rejected = spark.read.table("project.ecomerce_pipeline.silver_orders_rejected")
    final = spark.read.table("project.ecomerce_pipeline.silver_orders_final")
    cdc_source = spark.read.table("project.ecomerce_pipeline.silver_orders_cdc_source")
    latest = spark.read.table("project.ecomerce_pipeline.silver_orders_latest")

    clean_metrics = clean.agg(
        F.count("*").alias("clean_rows"),
        F.sum(F.when(F.col("has_rescued_data"), 1).otherwise(0)).alias("clean_rescued_rows")
    )

    rejected_metrics = rejected.agg(F.count("*").alias("rejected_rows"))
    final_metrics = final.agg(F.count("*").alias("valid_final_rows"))
    latest_metrics = latest.agg(F.count("*").alias("latest_state_rows"))
    cdc_metrics = cdc_source.agg(
        F.sum(F.when(F.col("cdc_operation") == "UPSERT", 1).otherwise(0)).alias("cdc_upsert_rows"),
        F.sum(F.when(F.col("cdc_operation") == "DELETE", 1).otherwise(0)).alias("cdc_delete_rows")
    )

    counts = clean_metrics.crossJoin(rejected_metrics).crossJoin(final_metrics).crossJoin(latest_metrics).crossJoin(cdc_metrics)

    return counts.select(
        F.array(
            F.struct(F.lit("quality").alias("metric_group"), F.lit("clean_rows").alias("metric_name"), F.col("clean_rows").cast("bigint").alias("metric_value")),
            F.struct(F.lit("quality").alias("metric_group"), F.lit("clean_rescued_rows").alias("metric_name"), F.col("clean_rescued_rows").cast("bigint").alias("metric_value")),
            F.struct(F.lit("quality").alias("metric_group"), F.lit("rejected_rows").alias("metric_name"), F.col("rejected_rows").cast("bigint").alias("metric_value")),
            F.struct(F.lit("quality").alias("metric_group"), F.lit("valid_final_rows").alias("metric_name"), F.col("valid_final_rows").cast("bigint").alias("metric_value")),
            F.struct(F.lit("cdc").alias("metric_group"), F.lit("cdc_upsert_rows").alias("metric_name"), F.col("cdc_upsert_rows").cast("bigint").alias("metric_value")),
            F.struct(F.lit("cdc").alias("metric_group"), F.lit("cdc_delete_rows").alias("metric_name"), F.col("cdc_delete_rows").cast("bigint").alias("metric_value")),
            F.struct(F.lit("latest_state").alias("metric_group"), F.lit("latest_state_rows").alias("metric_name"), F.col("latest_state_rows").cast("bigint").alias("metric_value"))
        ).alias("metrics")
    ).select(F.explode(F.col("metrics")).alias("metric")).select("metric.*")
