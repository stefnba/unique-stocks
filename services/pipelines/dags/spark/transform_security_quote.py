"""
Pre-transformation to update security quote. This is not used to get historical data, but to update the current data.
Sink format is csv and the acutal transformation is done in the sink_security_quote.py file.
"""

import os

from pyspark.sql import SparkSession
from spark_utils.path import convert_to_adls_uri

spark = SparkSession.builder.appName("Pre-transform security_quote update").getOrCreate()

path = convert_to_adls_uri(spark.conf.get("spark.datasetPath"), os.getenv("ADLS_STORAGE_ACCOUNT_NAME"))

raw = (
    spark.read.option("header", True)
    .csv(path)
    .withColumnRenamed("Code", "security")
    .select("Date", "exchange", "security", "Open", "High", "Low", "Close", "Adjusted_close", "Volume")
)


sink_path = convert_to_adls_uri(spark.conf.get("spark.sink"), os.getenv("ADLS_STORAGE_ACCOUNT_NAME"))
raw.write.option("header", True).csv(sink_path)
spark.stop()
