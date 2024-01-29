SELECT
    "LEI" AS lei,
    "Entity.LegalName" AS name,
    "Entity.LegalForm.EntityLegalFormCode" AS legal_form_id,
    "Entity.LegalJurisdiction" AS jurisdiction,
    "Entity.LegalAddress.FirstAddressLine" AS legal_address_street,
    "Entity.LegalAddress.AddressNumber" AS legal_address_street_number,
    "Entity.LegalAddress.PostalCode" AS legal_address_zip_code,
    "Entity.LegalAddress.City" AS legal_address_city,
    "Entity.LegalAddress.Country" AS legal_address_country,
    "Entity.HeadquartersAddress.FirstAddressLine" AS headquarter_address_street,
    "Entity.HeadquartersAddress.AddressNumber" AS headquarter_address_street_number,
    "Entity.HeadquartersAddress.City" AS headquarter_address_city,
    "Entity.HeadquartersAddress.PostalCode" AS headquarter_address_zip_code,
    "Entity.HeadquartersAddress.Country" AS headquarter_address_country,
    "Entity.EntityStatus" AS status,
    "Entity.EntityCreationDate" AS creation_date,
    "Entity.EntityExpirationDate" AS expiration_date,
    "Entity.EntityExpirationReason" AS expiration_reason,
    "Registration.InitialRegistrationDate" AS registration_date,
    "Registration.RegistrationStatus" AS registration_status
FROM read_csv_auto('${data_path}', header=True)

        