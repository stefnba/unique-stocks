import pyarrow as pa

EntityIsin = pa.schema(
    [
        pa.field("lei", pa.string()),
        pa.field("isin", pa.string()),
    ]
)

Entity = pa.schema(
    [
        pa.field("lei", pa.string()),
        pa.field("name", pa.string()),
        pa.field("legal_form_id", pa.string()),
        pa.field("jurisdiction", pa.string()),
        pa.field("legal_address_street", pa.string()),
        pa.field("legal_address_street_number", pa.string()),
        pa.field("legal_address_zip_code", pa.string()),
        pa.field("legal_address_city", pa.string()),
        pa.field("legal_address_country", pa.string()),
        pa.field("headquarter_address_street", pa.string()),
        pa.field("headquarter_address_street_number", pa.string()),
        pa.field("headquarter_address_city", pa.string()),
        pa.field("headquarter_address_zip_code", pa.string()),
        pa.field("headquarter_address_country", pa.string()),
        pa.field("status", pa.string()),
        pa.field("creation_date", pa.string()),
        pa.field("expiration_date", pa.string()),
        pa.field("expiration_reason", pa.string()),
        pa.field("registration_date", pa.string()),
        pa.field("registration_status", pa.string()),
    ]
)
