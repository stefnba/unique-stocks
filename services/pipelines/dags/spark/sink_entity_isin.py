from pyspark.sql import SparkSession
from pyspark.sql import types as t

spark = SparkSession.builder.appName("Sink entity_isin").getOrCreate()


PATH = spark.conf.get("spark.datasetPath")


if not PATH:
    raise Exception("Missing spark.datasetPath configuration")

PATH = PATH.replace("s3://", "s3a://")


schema = t.StructType(
    [
        t.StructField("LEI", t.StringType(), True),
        t.StructField("ISIN", t.StringType(), True),
    ]
)


spark.read.schema(schema).option("header", True).option("compression", "gzip").csv(PATH).createOrReplaceTempView("raw")

spark.sql(
    """
SELECT
    LEI as lei,
    ISIN as isin
FROM
    raw
"""
).createOrReplaceTempView("transformed")


spark.sql(
    """
INSERT OVERWRITE 
    curated.entity_isin
    (lei, isin, created_at, updated_at)
SELECT
    lei, isin, current_timestamp(), NULL
FROM transformed;
"""
)


spark.stop()
