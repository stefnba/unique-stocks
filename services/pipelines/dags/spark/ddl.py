from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Create Iceberg Tables").getOrCreate()  # type: ignore

spark.sql("USE uniquestocks;")

# spark.sql("DROP TABLE IF EXISTS curated.exchange PURGE;")
# spark.sql("DROP TABLE IF EXISTS curated.security PURGE;")
# spark.sql("DROP TABLE IF EXISTS curated.security_quote PURGE;")
# spark.sql("DROP TABLE IF EXISTS curated.fundamental PURGE;")
# spark.sql("DROP TABLE IF EXISTS curated.entity_isin PURGE;")
# spark.sql("DROP TABLE IF EXISTS curated.entity PURGE;")
# spark.sql("DROP TABLE IF EXISTS mapping.mapping PURGE;")

spark.sql("CREATE namespace IF NOT EXISTS curated;")
spark.sql("CREATE namespace IF NOT EXISTS mapping;")
spark.sql("CREATE namespace IF NOT EXISTS transformed;")

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS curated.exchange (
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
    CREATE TABLE IF NOT EXISTS curated.security (
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
    CREATE TABLE IF NOT EXISTS curated.security_quote (
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
    CREATE TABLE IF NOT EXISTS mapping.mapping (
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
    CREATE TABLE IF NOT EXISTS curated.fundamental (
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


spark.sql(
    """
    CREATE TABLE IF NOT EXISTS curated.entity_isin (
        lei STRING,
        isin STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg;
    """
)


spark.sql(
    """
    CREATE TABLE curated.entity (
        lei STRING,
        name STRING,
        legal_form_id STRING,
        jurisdiction STRING,
        legal_address_street STRING,
        legal_address_street_number STRING,
        legal_address_zip_code STRING,
        legal_address_city STRING,
        legal_address_country STRING,
        headquarter_address_street STRING,
        headquarter_address_street_number STRING,
        headquarter_address_city STRING,
        headquarter_address_zip_code STRING,
        headquarter_address_country STRING,
        status STRING,
        creation_date STRING,
        expiration_date STRING,
        expiration_reason STRING,
        registration_date STRING,
        registration_status STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (legal_address_country);
    """
)

spark.stop()
