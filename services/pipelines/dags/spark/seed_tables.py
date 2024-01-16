from pyspark.sql import SparkSession
from pyspark.sql import types as t

spark = SparkSession.builder.appName("Seed Iceberg tables").getOrCreate()


PATH = spark.conf.get("spark.datasetPath")


if not PATH:
    raise Exception("Path is not set.")

schema = t.StructType(
    [
        t.StructField("product", t.StringType(), True),
        t.StructField("source", t.StringType(), True),
        t.StructField("field", t.StringType(), True),
        t.StructField("source_value", t.StringType(), True),
        t.StructField("source_description", t.StringType(), True),
        t.StructField("mapping_value", t.StringType(), True),
        t.StructField("uid_description", t.StringType(), True),
        t.StructField("active_from", t.DateType(), True),
        t.StructField("active_until", t.DateType(), True),
        t.StructField("is_active", t.BooleanType(), True),
    ]
)


data = spark.read.options(header=True).schema(schema).csv(PATH).createOrReplaceTempView("data")

spark.sql(
    """
    INSERT OVERWRITE mapping SELECT * FROM data;
    """
)
