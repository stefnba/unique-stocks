"""
Pre-transformation to update security quote. This is not used to get historical data, but to update the current data.
Sink format is csv and the acutal transformation is done in the sink_security_quote.py file.
"""

import os

from pyspark.sql import SparkSession
from spark_utils.path import convert_to_adls_uri

spark = SparkSession.builder.appName("Pre-transform security_quote update").getOrCreate()


ADLS_STORAGE_ACCOUNT_NAME = os.getenv("ADLS_STORAGE_ACCOUNT_NAME")
PATH = spark.conf.get("spark.datasetPath")

path = convert_to_adls_uri(PATH, ADLS_STORAGE_ACCOUNT_NAME)

print(path)

# data = [[295, "South Bend", "Indiana", "IN", 101190, 112.9]]
# columns = ["rank", "city", "state", "code", "population", "price"]

# df1 = spark.createDataFrame(
#     data, schema="rank LONG, city STRING, state STRING, code STRING, population LONG, price DOUBLE"
# )
# df1.show()


# spark.stop()
