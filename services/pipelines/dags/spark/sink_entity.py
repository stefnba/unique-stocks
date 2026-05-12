from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Sink entity").getOrCreate()


PATH = spark.conf.get("spark.datasetPath")


if not PATH:
    raise Exception("Missing spark.datasetPath configuration")

PATH = PATH.replace("s3://", "s3a://")

data = spark.read.option("header", True).option("compression", "gzip").csv(PATH)


data = data.select(
    col("LEI"),
    col("`Entity.LegalName`"),
    col("`Entity.LegalForm.EntityLegalFormCode`"),
    col("`Entity.LegalJurisdiction`"),
    col("`Entity.LegalAddress.FirstAddressLine`"),
    col("`Entity.LegalAddress.AddressNumber`"),
    col("`Entity.LegalAddress.PostalCode`"),
    col("`Entity.LegalAddress.City`"),
    col("`Entity.LegalAddress.Country`"),
    col("`Entity.HeadquartersAddress.FirstAddressLine`"),
    col("`Entity.HeadquartersAddress.AddressNumber`"),
    col("`Entity.HeadquartersAddress.City`"),
    col("`Entity.HeadquartersAddress.PostalCode`"),
    col("`Entity.HeadquartersAddress.Country`"),
    col("`Entity.EntityStatus`"),
    col("`Entity.EntityCreationDate`"),
    col("`Entity.EntityExpirationDate`"),
    col("`Entity.EntityExpirationReason`"),
    col("`Registration.InitialRegistrationDate`"),
    col("`Registration.RegistrationStatus`"),
)

data = (
    data.withColumnRenamed("LEI", "lei")
    .withColumnRenamed("Entity.LegalName", "name")
    .withColumnRenamed("Entity.LegalForm.EntityLegalFormCode", "legal_form_id")
    .withColumnRenamed("Entity.LegalJurisdiction", "jurisdiction")
    .withColumnRenamed("Entity.LegalAddress.FirstAddressLine", "legal_address_street")
    .withColumnRenamed("Entity.LegalAddress.AddressNumber", "legal_address_street_number")
    .withColumnRenamed("Entity.LegalAddress.PostalCode", "legal_address_zip_code")
    .withColumnRenamed("Entity.LegalAddress.City", "legal_address_city")
    .withColumnRenamed("Entity.LegalAddress.Country", "legal_address_country")
    .withColumnRenamed("Entity.HeadquartersAddress.FirstAddressLine", "headquarter_address_street")
    .withColumnRenamed("Entity.HeadquartersAddress.AddressNumber", "headquarter_address_street_number")
    .withColumnRenamed("Entity.HeadquartersAddress.City", "headquarter_address_city")
    .withColumnRenamed("Entity.HeadquartersAddress.PostalCode", "headquarter_address_zip_code")
    .withColumnRenamed("Entity.HeadquartersAddress.Country", "headquarter_address_country")
    .withColumnRenamed("Entity.EntityStatus", "status")
    .withColumnRenamed("Entity.EntityCreationDate", "creation_date")
    .withColumnRenamed("Entity.EntityExpirationDate", "expiration_date")
    .withColumnRenamed("Entity.EntityExpirationReason", "expiration_reason")
    .withColumnRenamed("Registration.InitialRegistrationDate", "registration_date")
    .withColumnRenamed("Registration.RegistrationStatus", "registration_status")
)


data = (
    data.withColumn("creation_date", (col("creation_date").cast("timestamp")))
    .withColumn("expiration_date", (col("expiration_date").cast("timestamp")))
    .withColumn("registration_date", (col("registration_date").cast("timestamp")))
)


data.createOrReplaceTempView("transformed")


spark.sql(
    """
    INSERT OVERWRITE 
        curated.entity
        (
        lei,
        name,
        legal_form_id,
        jurisdiction,
        legal_address_street,
        legal_address_street_number,
        legal_address_zip_code,
        legal_address_city,
        legal_address_country,
        headquarter_address_street,
        headquarter_address_street_number,
        headquarter_address_city,
        headquarter_address_zip_code,
        headquarter_address_country,
        status,
        creation_date,
        expiration_date,
        expiration_reason,
        registration_date,
        registration_status, 
        created_at, 
        updated_at
        )
    SELECT
        lei,
        name,
        legal_form_id,
        jurisdiction,
        legal_address_street,
        legal_address_street_number,
        legal_address_zip_code,
        legal_address_city,
        legal_address_country,
        headquarter_address_street,
        headquarter_address_street_number,
        headquarter_address_city,
        headquarter_address_zip_code,
        headquarter_address_country,
        status,
        creation_date,
        expiration_date,
        expiration_reason,
        registration_date,
        registration_status, 
        current_timestamp(),
        NULL
    FROM transformed;
"""
)


spark.stop()
