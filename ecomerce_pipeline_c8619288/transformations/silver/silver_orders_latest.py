from pyspark import pipelines as dp
from pyspark.sql.functions import expr, struct


dp.create_streaming_table(
    name="project.ecomerce_pipeline.silver_orders_latest",
    comment="Latest-state SCD1 orders table maintained from CDC-friendly silver order events.",
    cluster_by=["customer_id", "order_date"]
)


dp.create_auto_cdc_flow(
    target="project.ecomerce_pipeline.silver_orders_latest",
    source="project.ecomerce_pipeline.silver_orders_cdc_source",
    keys=["order_line_key"],
    sequence_by=struct("cdc_sequence_ts", "cdc_sequence_batch_id"),
    apply_as_deletes=expr("cdc_operation = 'DELETE'"),
    ignore_null_updates=True,
    stored_as_scd_type=1,
    name="silver_orders_latest_cdc"
)
