from pyspark import pipelines as dp


@dp.table(
    name="project.ecomerce_pipeline.bronze_orders_raw",
    comment="Raw order events ingested incrementally from the landing volume with Auto Loader."
)
def bronze_orders_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/project/ecomerce/data/order-events-landing/incremental")
    )
