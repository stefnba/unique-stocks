CREATE TABLE IF NOT EXISTS mapping_figi(
    isin text,
    wkn int,
    ticker text,
    ticker_figi text,
    exchange_mic text,
    --exchange_code_figi text,
    --exchange_operating_mic text,
    currency varchar(3),
    country varchar(2),
    figi text NOT NULL,
    share_class_figi text,
    composite_figi text,
    security_type_id int,
    created_at timestamp without time zone DEFAULT (now() at time zone 'utc') NOT NULL,
    updated_at timestamp without time zone,
    valid_from timestamp without time zone,
    valid_until timestamp without time zone,
    is_active boolean DEFAULT TRUE
);

CREATE INDEX ON mapping_figi(isin);

CREATE INDEX ON mapping_figi(wkn);

CREATE INDEX ON mapping_figi(ticker);

CREATE INDEX ON mapping_figi(ticker_figi);

--CREATE INDEX ON mapping_figi(exchange_code);

--CREATE INDEX ON mapping_figi(exchange_code_figi);

CREATE INDEX ON mapping_figi(exchange_mic);

CREATE UNIQUE INDEX ON mapping_figi(figi);

