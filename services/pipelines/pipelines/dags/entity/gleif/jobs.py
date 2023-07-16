import polars as pl

# from shared.loggers import logger
from shared.clients.api.gleif.client import GleifApiClient

ASSET_SOURCE = GleifApiClient.client_key


def transform(df_raw: pl.DataFrame):
    from shared.jobs.surrogate_keys.jobs import map_surrogate_keys

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

    data = map_surrogate_keys(data=data, product="entity", id_col_name="id", uid_col_name="lei")

    return data
