from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Create Iceberg Tables").getOrCreate()  # type: ignore

spark.sql("USE uniquestocks_dev.uniquestocks_dev;")

# spark.sql("DROP TABLE IF EXISTS exchange PURGE;")
# spark.sql("DROP TABLE IF EXISTS security PURGE;")
# spark.sql("DROP TABLE IF EXISTS security_quote PURGE;")
# spark.sql("DROP TABLE IF EXISTS fundamental PURGE;")

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS exchange (
        code STRING,
        name STRING,
        operating_mic STRING,
        currency STRING,
        country STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    ) USING iceberg;
"""
)


spark.sql(
    """
    CREATE TABLE IF NOT EXISTS security (
        code STRING,
        name STRING,
        isin STRING,
        country STRING,
        currency STRING,
        exchange_code STRING,
        type STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP     
    ) USING iceberg
    PARTITIONED BY (exchange_code, type);
"""
)


spark.sql(
    """
    CREATE TABLE IF NOT EXISTS security_quote (
        security_code STRING,
        exchange_code STRING,
        date DATE,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        adjusted_close FLOAT,
        volume LONG,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (exchange_code, year(date));
    """
)

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS mapping (
        product STRING,
        source STRING,
        field STRING,
        source_value STRING,
        source_description STRING,
        mapping_value STRING,
        uid_description STRING,
        active_from TIMESTAMP,
        active_until TIMESTAMP,
        is_active BOOLEAN
    )
    USING iceberg
    PARTITIONED BY (product, source);
    """
)


spark.sql(
    """
    CREATE TABLE IF NOT EXISTS fundamental (
        category STRING,
        metric STRING,
        value STRING,
        currency STRING,
        period DATE,
        period_type STRING,
        published_at TIMESTAMP,
        exchange_code STRING,
        security_code STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (exchange_code, security_code, year(period));
    """
)

spark.stop()
