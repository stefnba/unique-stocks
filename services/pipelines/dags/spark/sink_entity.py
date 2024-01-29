from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Sink entity").getOrCreate()


PATH = spark.conf.get("spark.datasetPath")


if not PATH:
    raise Exception("Missing spark.datasetPath configuration")

PATH = PATH.replace("s3://", "s3a://")


spark.read.parquet(PATH).createOrReplaceTempView("raw")


spark.sql(
    """
INSERT OVERWRITE 
    curated.entity
    (
        lei ,
        name ,
        legal_form_id ,
        jurisdiction ,
        legal_address_street ,
        legal_address_street_number ,
        legal_address_zip_code ,
        legal_address_city ,
        legal_address_country ,
        headquarter_address_street ,
        headquarter_address_street_number ,
        headquarter_address_city ,
        headquarter_address_zip_code ,
        headquarter_address_country ,
        status ,
        creation_date ,
        expiration_date ,
        expiration_reason ,
        registration_date ,
        registration_status, 
        created_at, 
        updated_at
    )
SELECT
    lei ,
    name ,
    legal_form_id ,
    jurisdiction ,
    legal_address_street ,
    legal_address_street_number ,
    legal_address_zip_code ,
    legal_address_city ,
    legal_address_country ,
    headquarter_address_street ,
    headquarter_address_street_number ,
    headquarter_address_city ,
    headquarter_address_zip_code ,
    headquarter_address_country ,
    status ,
    creation_date ,
    expiration_date ,
    expiration_reason ,
    registration_date ,
    registration_status, 
    current_timestamp(),
    NULL
FROM transformed;
"""
)


spark.stop()
