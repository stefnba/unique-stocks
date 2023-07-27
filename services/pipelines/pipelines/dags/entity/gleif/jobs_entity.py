import polars as pl

# from shared.loggers import logger
from shared.clients.api.gleif.client import GleifApiClient

ASSET_SOURCE = GleifApiClient.client_key


def transform(data: pl.LazyFrame) -> pl.LazyFrame:
    from shared.jobs.surrogate_keys.jobs import map_surrogate_keys

    data = data.select(
        [
            "LEI",
            "Entity.LegalName",
            "Entity.LegalForm.EntityLegalFormCode",
            "Entity.LegalJurisdiction",
            "Entity.LegalAddress.FirstAddressLine",
            "Entity.LegalAddress.AddressNumber",
            "Entity.LegalAddress.PostalCode",
            "Entity.LegalAddress.City",
            "Entity.LegalAddress.Country",
            "Entity.HeadquartersAddress.FirstAddressLine",
            "Entity.HeadquartersAddress.AddressNumber",
            "Entity.HeadquartersAddress.City",
            "Entity.HeadquartersAddress.PostalCode",
            "Entity.HeadquartersAddress.Country",
            "Entity.EntityStatus",
            "Entity.EntityCreationDate",
            "Entity.EntityExpirationDate",
            "Entity.EntityExpirationReason",
            "Registration.InitialRegistrationDate",
            "Registration.RegistrationStatus",
        ]
    )

    data = data.rename(
        {
            "LEI": "lei",
            "Entity.LegalName": "name",
            "Entity.LegalForm.EntityLegalFormCode": "legal_form_id",
            "Entity.LegalJurisdiction": "jurisdiction",
            "Entity.LegalAddress.FirstAddressLine": "legal_address_street",
            "Entity.LegalAddress.AddressNumber": "legal_address_street_number",
            "Entity.LegalAddress.PostalCode": "legal_address_zip_code",
            "Entity.LegalAddress.City": "legal_address_city",
            "Entity.LegalAddress.Country": "legal_address_country",
            "Entity.HeadquartersAddress.FirstAddressLine": "headquarter_address_street",
            "Entity.HeadquartersAddress.AddressNumber": "headquarter_address_street_number",
            "Entity.HeadquartersAddress.City": "headquarter_address_city",
            "Entity.HeadquartersAddress.PostalCode": "headquarter_address_zip_code",
            "Entity.HeadquartersAddress.Country": "headquarter_address_country",
            "Entity.EntityStatus": "status",
            "Entity.EntityCreationDate": "creation_date",
            "Entity.EntityExpirationDate": "expiration_date",
            "Entity.EntityExpirationReason": "expiration_reason",
            "Registration.InitialRegistrationDate": "registration_date",
            "Registration.RegistrationStatus": "registration_status",
        }
    )

    # data = data.with_columns(
    #     [pl.when(pl.col(pl.Utf8).str.lengths() == 0).then(None).otherwise(pl.col(pl.Utf8)).keep_name()]
    # )

    # return data
    return map_surrogate_keys(data=data, product="entity", id_col_name="id", uid_col_name="lei", collect=False)
