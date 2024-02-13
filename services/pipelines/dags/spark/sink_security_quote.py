import os
import typing

from pyspark.sql import SparkSession
from pyspark.sql import types as t
from spark_utils.path import convert_to_adls_uri

spark = SparkSession.builder.appName("Sink security_quote").getOrCreate()


path = convert_to_adls_uri(spark.conf.get("spark.datasetPath"), os.getenv("ADLS_STORAGE_ACCOUNT_NAME"))


schema = t.StructType(
    [
        t.StructField("Date", t.DateType(), True),
        t.StructField("exchange", t.StringType(), True),
        t.StructField("security", t.StringType(), True),
        t.StructField("Open", t.DoubleType(), True),
        t.StructField("High", t.DoubleType(), True),
        t.StructField("Low", t.DoubleType(), True),
        t.StructField("Close", t.DoubleType(), True),
        t.StructField("Adjusted_close", t.DoubleType(), True),
        t.StructField("Volume", t.LongType(), True),
    ]
)


spark.read.schema(schema).option("header", True).csv(path).createOrReplaceTempView("data")

spark.sql(
    """
SELECT
    Date as date,
    Open as open,
    High AS high,
    Low as low,
    Close as close,
    Adjusted_close as adjusted_close,
    Volume as volume,
    security AS security_code,
    exchange AS exchange_code,
    current_timestamp() AS created_at,
    NULL AS updated_at
FROM
    data
WHERE
    date < current_date();
"""
).createOrReplaceTempView("transformed")


mode: typing.Literal["APPEND", "OVERWRITE"] = "APPEND"  # default is APPEND

if mode == "APPEND":
    # Add new records
    spark.sql(
        """
    MERGE INTO curated.security_quote AS t 
    USING (SELECT * FROM transformed) AS s  
    ON (t.date = s.date AND t.security_code = s.security_code AND t.exchange_code = s.exchange_code)
    WHEN NOT MATCHED THEN 
            INSERT 
                (date, open, high, low, close, adjusted_close, volume, 
                security_code, exchange_code, created_at, updated_at) 
            VALUES
                (s.date, s.open, s.high, s.low, s.close, s.adjusted_close, s.volume, 
                s.security_code, s.exchange_code, s.created_at, NULL)
    """
    )

elif mode == "OVERWRITE":
    # Overwrite the table
    spark.sql(
        """
    INSERT OVERWRITE 
        curated.security_quote
        (date, open, high, low, close, adjusted_close, volume, security_code, exchange_code, created_at, updated_at)
    SELECT
        date, open, high, low, close, adjusted_close, volume, security_code, exchange_code, created_at, updated_at
    FROM transformed;
    """
    )


spark.stop()
