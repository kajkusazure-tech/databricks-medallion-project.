import os
import sys
import unittest
from pathlib import Path

spark_home = Path(os.environ.get("SPARK_HOME", str(Path.home() / "spark-3.4.1-bin-hadoop3")))
spark_python = spark_home / "python"
spark_python_lib = spark_python / "lib"

if spark_python.exists():
    os.environ.setdefault("SPARK_HOME", str(spark_home))
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    sys.path.insert(0, str(spark_python))
    for zip_path in sorted(spark_python_lib.glob("*.zip")):
        sys.path.insert(0, str(zip_path))

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType, DoubleType
from ecomerce_pipeline_c8619288.transformations.silver.orders_cdc_logic import build_silver_orders_cdc_source


class SilverTransformationsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("ecomerce-pipeline-tests")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_silver_orders_cdc_source_filters_invalid_data(self):
        schema = StructType(
            [
                StructField("InvoiceNo", StringType(), True),
                StructField("StockCode", StringType(), True),
                StructField("Description", StringType(), True),
                StructField("Quantity", IntegerType(), True),
                StructField("InvoiceDate", StringType(), True),
                StructField("UnitPrice", DoubleType(), True),
                StructField("CustomerID", IntegerType(), True),
                StructField("Country", StringType(), True),
                StructField("order_status", StringType(), True),
                StructField("event_source", StringType(), True),
                StructField("ingest_batch_id", StringType(), True),
                StructField("ingest_ts", TimestampType(), True),
                StructField("promo_code", StringType(), True),
                StructField("_rescued_data", StringType(), True),
            ]
        )

        source_data = [
            Row(
                InvoiceNo="123",
                StockCode="ABC",
                Description="Valid",
                Quantity=1,
                InvoiceDate="5/13/2026 0:00",
                UnitPrice=10.0,
                CustomerID=101,
                Country="Serbia",
                order_status="Normal",
                event_source="api",
                ingest_batch_id="batch-1",
                ingest_ts=None,
                promo_code=None,
                _rescued_data=None,
            ),
            Row(
                InvoiceNo=None,
                StockCode="XYZ",
                Description="Invalid",
                Quantity=1,
                InvoiceDate="5/13/2026 0:00",
                UnitPrice=10.0,
                CustomerID=202,
                Country="Serbia",
                order_status="Normal",
                event_source="api",
                ingest_batch_id="batch-1",
                ingest_ts=None,
                promo_code=None,
                _rescued_data=None,
            ),
        ]

        source_df = self.spark.createDataFrame(source_data, schema=schema)
        rows = build_silver_orders_cdc_source(source_df).collect()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["invoice_no"], "123")
        self.assertEqual(rows[0]["cdc_operation"], "UPSERT")
