from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Insert into Iceberg table").getOrCreate()

PATH = spark.conf.get("spark.datasetPath")


if not PATH:
    raise Exception("Path is not set.")


PATH = PATH.replace("s3://", "s3a://")


data = spark.read.parquet(PATH).createOrReplaceTempView("data")

spark.sql(
    """
    INSERT OVERWRITE
        fundamental (
            category, metric, value, currency, period, period_type, published_at, 
            exchange_code, security_code, created_at, updated_at
            )
    SELECT
        category, metric, value, currency, period, period_type, published_at, 
        exchange_code, security_code, current_timestamp(), NULL
    FROM data;
    """
)

spark.stop()
