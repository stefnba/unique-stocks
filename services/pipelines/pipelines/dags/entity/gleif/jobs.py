import polars as pl


ASSET_SOURCE = ""


def ingest():
    from shared.clients.data_lake.azure.azure_data_lake import dl_client

    url = "https://leidata-preview.gleif.org/storage/golden-copy-files/2023/05/29/789258/20230529-0800-gleif-goldencopy-lei2-golden-copy.csv.zip"

    return dl_client.stream_from_url(url=url)


def uncompress(zipped_file: bytes):
    import zipfile
    import io

    with zipfile.ZipFile(io.BytesIO(zipped_file)) as zip_archive:
        file_to_unzip = zip_archive.filelist[0]
        print(file_to_unzip.filename, file_to_unzip.file_size)

        unzipped_file = zip_archive.read(file_to_unzip)

    return unzipped_file


def transform(df_raw: pl.DataFrame):
    data = pl.DataFrame()
    data = data.with_columns(
        [
            df_raw["LEI"].alias("lei"),
            df_raw["Entity.LegalName"].alias("name"),
            df_raw["Entity.LegalForm.EntityLegalFormCode"].alias("legal_form_id"),
            df_raw["Entity.LegalJurisdiction"].alias("jurisdiction"),
            df_raw["Entity.LegalAddress.FirstAddressLine"].alias("legal_address_street"),
            df_raw["Entity.LegalAddress.AddressNumber"].alias("legal_address_street_number"),
            df_raw["Entity.LegalAddress.PostalCode"].alias("legal_address_zip_code"),
            df_raw["Entity.LegalAddress.City"].alias("legal_address_city"),
            df_raw["Entity.LegalAddress.Country"].alias("legal_address_country"),
            df_raw["Entity.HeadquartersAddress.FirstAddressLine"].alias("headquarter_address_street"),
            df_raw["Entity.HeadquartersAddress.AddressNumber"].alias("headquarter_address_street_number"),
            df_raw["Entity.HeadquartersAddress.City"].alias("headquarter_address_city"),
            df_raw["Entity.HeadquartersAddress.PostalCode"].alias("headquarter_address_zip_code"),
            df_raw["Entity.HeadquartersAddress.Country"].alias("headquarter_address_country"),
            df_raw["Entity.EntityStatus"].alias("status"),
            df_raw["Entity.EntityCreationDate"].alias("creation_date"),
            df_raw["Entity.EntityExpirationDate"].alias("expiration_date"),
            df_raw["Entity.EntityExpirationReason"].alias("expiration_reason"),
            df_raw["Registration.InitialRegistrationDate"].alias("registration_date"),
            df_raw["Registration.RegistrationStatus"].alias("registration_status"),
            # pl.concat_list(df_raw.select("^Entity.OtherEntityNames.OtherEntityName.[0-9]$")),
        ]
    )

    """
    #  'Entity.LegalAddress.AddressNumberWithinBuilding',
    #  'Entity.LegalAddress.MailRouting',
    #  'Entity.LegalAddress.AdditionalAddressLine.1',
    #  'Entity.LegalAddress.AdditionalAddressLine.2',
    #  'Entity.LegalAddress.AdditionalAddressLine.3',


    #  'Entity.HeadquartersAddress.AdditionalAddressLine.1',
    #  'Entity.HeadquartersAddress.AdditionalAddressLine.2',
    #  'Entity.HeadquartersAddress.AdditionalAddressLine.3',
    
    """

    data = data.with_columns(
        [pl.when(pl.col(pl.Utf8).str.lengths() == 0).then(None).otherwise(pl.col(pl.Utf8)).keep_name()]
    )

    return data


def load_into_db():
    pass


def ingest_isin_to_lei_relationship():
    pass


def transform_isin_to_lei_relationship(data: pl.DataFrame):
    data = data.rename({"LEI": "uid", "ISIN": "source_value"})

    data = data.with_columns(
        [
            pl.lit("entity").alias("product"),
            pl.lit("Gleif").alias("source"),
            pl.lit("isin_to_lei").alias("field"),
            pl.lit("ISIN").alias("source_description"),
            pl.lit("LEI").alias("uid_description"),
        ]
    )

    return data
