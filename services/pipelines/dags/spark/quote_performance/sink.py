from pyspark.sql import SparkSession
from spark_utils.path import convert_to_s3_uri

spark = SparkSession.builder.appName("Sink quote performance").getOrCreate()

reference_path = convert_to_s3_uri(spark.conf.get("spark.referencePath"))
current_path = convert_to_s3_uri(spark.conf.get("spark.currentPath"))

if not reference_path:
    raise Exception("Reference path is missing.")

if not current_path:
    raise Exception("Current path is missing.")


spark.read.parquet(reference_path).createOrReplaceTempView("reference")
spark.read.parquet(current_path).createOrReplaceTempView("current")

spark.sql(
    """
SELECT
    current.security_code,
    current.exchange_code,
    current.date AS current_date,
    reference.date as reference_date,
    reference.period,
    current.quote AS current_quote,
    reference.quote AS reference_quote,
    current.quote / reference.quote - 1 AS performance
FROM current
LEFT JOIN reference USING (security_code, exchange_code)
"""
).createOrReplaceTempView("performance")


spark.sql(
    """
    INSERT OVERWRITE
        curated.security_quote_performance (
            security_code, 
            exchange_code, 
            current_date, 
            reference_date, 
            period, 
            current_quote,
            reference_quote,
            performance,
            created_at, 
            updated_at)
    SELECT
        security_code, 
        exchange_code, 
        current_date, 
        reference_date, 
        period, 
        current_quote,
        reference_quote,
        performance,
        current_timestamp(),
        NULL
    FROM performance;
    """
)

spark.stop()
