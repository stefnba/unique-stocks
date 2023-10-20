import pyarrow as pa

EntityIsin = pa.schema(
    [
        pa.field("lei", pa.string()),
        pa.field("isin", pa.string()),
    ]
)


SecurityQuote = pa.schema(
    [
        pa.field("date", pa.date32()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("adjusted_close", pa.float64()),
        pa.field("volume", pa.int64()),
        pa.field("exchange_code", pa.string()),
        pa.field("security_code", pa.string()),
        pa.field("created_at", pa.date32()),
    ]
)

QuotePerformance = pa.schema(
    [
        pa.field("security_code", pa.string()),
        pa.field("exchange_code", pa.string()),
        pa.field("base_date", pa.date32()),
        pa.field("base_close", pa.float32()),
        pa.field("reference_date", pa.date32()),
        pa.field("reference_close", pa.float32()),
        pa.field("performance", pa.float32()),
        pa.field("period", pa.string()),
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


Mapping = pa.schema(
    [
        pa.field("source", pa.string()),
        pa.field("product", pa.string()),
        pa.field("field", pa.string()),
        pa.field("source_value", pa.string()),
        pa.field("source_description", pa.string()),
        pa.field("mapping_value", pa.string()),
        pa.field("uid_description", pa.string()),
        pa.field("active_from", pa.date64()),
        pa.field("active_until", pa.date32()),
        pa.field("is_active", pa.bool_()),
    ]
)


Fundamental = pa.schema(
    [
        pa.field("exchange_code", pa.string()),
        pa.field("security_code", pa.string()),
        pa.field("security_type", pa.string()),
        pa.field("category", pa.string()),
        pa.field("metric", pa.string()),
        pa.field("value", pa.string()),
        pa.field("currency", pa.string()),
        pa.field("period", pa.date32()),
        pa.field("period_type", pa.string()),
        pa.field("published_at", pa.date32()),
    ]
)


IndexMember = pa.schema(
    [
        pa.field("security_code", pa.string()),
        pa.field("security_name", pa.string()),
        pa.field("exchange_code", pa.string()),
        pa.field("country", pa.string()),
        pa.field("currency", pa.string()),
        pa.field("index_code", pa.string()),
        pa.field("index_name", pa.string()),
    ]
)
