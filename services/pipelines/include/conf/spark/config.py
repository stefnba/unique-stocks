CATALOG_NAME = "uniquestocks"

WAREHOUSE_PATH = "s3a://uniquestocks/data-lake/lakehouse/"


iceberg_glue_catalog = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    f"spark.sql.catalog.{CATALOG_NAME}": "org.apache.iceberg.spark.SparkCatalog",
    f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    f"spark.sql.catalog.{CATALOG_NAME}.warehouse": WAREHOUSE_PATH,
    f"spark.sql.catalog.{CATALOG_NAME}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.defaultCatalog": CATALOG_NAME,
}


iceberg_hive_catalog = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    f"spark.sql.catalog.{CATALOG_NAME}": "org.apache.iceberg.spark.SparkCatalog",
    f"spark.sql.catalog.{CATALOG_NAME}.type": "hive",
    f"spark.sql.catalog.{CATALOG_NAME}.uri": "thrift://metastore:9083",
    f"spark.sql.catalog.{CATALOG_NAME}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.defaultCatalog": CATALOG_NAME,
    f"spark.sql.catalog.{CATALOG_NAME}.warehouse": WAREHOUSE_PATH,
    "spark.hadoop.fs.s3a.access.key": "$AWS_ACCESS_KEY_ID",
    "spark.hadoop.fs.s3a.secret.key": "$AWS_SECRET_ACCESS_KEY",
    "spark.hadoop.fs.s3a.endoint.region": "$AWS_REGION",
}


iceberg_jdbc_catalog = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    f"spark.sql.catalog.{CATALOG_NAME}": "org.apache.iceberg.spark.SparkCatalog",
    f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
    f"spark.sql.catalog.{CATALOG_NAME}.uri": "jdbc:postgresql://uniquestocks-lakehouse-catalog:5432/iceberg",
    f"spark.sql.catalog.{CATALOG_NAME}.jdbc.user": "admin",  # todo env variable
    f"spark.sql.catalog.{CATALOG_NAME}.jdbc.password": "password",  # todo env variable
    f"spark.sql.catalog.{CATALOG_NAME}.warehouse": WAREHOUSE_PATH,
    f"spark.sql.catalog.{CATALOG_NAME}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.hadoop.fs.s3a.access.key": "$AWS_ACCESS_KEY_ID",
    "spark.hadoop.fs.s3a.secret.key": "$AWS_SECRET_ACCESS_KEY",
    "spark.sql.defaultCatalog": CATALOG_NAME,
}


adls = {
    "spark.hadoop.fs.azure.account.auth.type.${ADLS_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net": "OAuth",
    "spark.hadoop.fs.azure.account.oauth.provider.type.${ADLS_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net": "org.apache."
    "hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "spark.hadoop.fs.azure.account.oauth2.client.id."
    "${ADLS_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net": "${ADLS_CLIENT_ID}",
    "spark.hadoop.fs.azure.account.oauth2.client.secret.${ADLS_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net": ""
    "${ADLS_CLIENT_SECRET}",
    "spark.hadoop.fs.azure.account.oauth2.client.endpoint.${ADLS_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net": "https://login.microsoftonline.com/${ADLS_TENANT_ID}/oauth2/token",
}


aws = {
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.hadoop.fs.s3a.access.key": "${AWS__LOGIN}",
    "spark.hadoop.fs.s3a.secret.key": "${AWS__PASSWORD}",
}
