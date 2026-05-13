from pyspark import pipelines as dp

from ecomerce_pipeline_c8619288.transformations.silver.orders_cdc_logic import build_silver_orders_cdc_source


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
    return build_silver_orders_cdc_source(bronze)
