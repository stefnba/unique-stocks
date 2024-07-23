from pyspark.sql import SparkSession
from spark_utils.path import convert_to_s3_uri

spark = SparkSession.builder.appName("Extract latest quote by date").getOrCreate()

sink_path = convert_to_s3_uri(spark.conf.get("spark.datasetPath"))
reference_date = spark.conf.get("spark.referenceDate")

period = spark.sparkContext.getConf().get("spark.period") or "current"
period = f"'{period}'"

if not reference_date:
    raise Exception("Reference date is missing.")

spark.sql(
    f"""   
SELECT
    last.*, 
    adjusted_close AS quote,
    {period} as period
FROM (
    SELECT
        security_code,
        exchange_code,
        MAX(date) AS date
    FROM curated.security_quote 
    WHERE date BETWEEN date_sub('{reference_date}', 14) AND '{reference_date}'
    GROUP BY 1, 2
    HAVING date = '{reference_date}'
) AS last 
LEFT JOIN (
    SELECT
        security_code,
        exchange_code,
        date,
        adjusted_close
    FROM curated.security_quote
) AS quote 
USING (security_code,exchange_code, date)
"""
).write.mode("append").parquet(sink_path)


spark.stop()
