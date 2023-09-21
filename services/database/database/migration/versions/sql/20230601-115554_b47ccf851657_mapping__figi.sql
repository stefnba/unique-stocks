/* UP */
CREATE TABLE IF NOT EXISTS "mapping"."figi"(
    isin_source text,
    wkn_source int,
    ticker_source text,
    ticker_figi text,
    name_figi text,
    figi text NOT NULL,
    share_class_figi text,
    composite_figi text,
    exchange_code_figi text,
    security_type_figi text,
    security_type2_figi text,
    market_sector_figi text,
    security_description_figi text,
    created_at timestamp without time zone DEFAULT (now() at time zone 'utc') NOT NULL,
    updated_at timestamp without time zone,
    active_from timestamp without time zone,
    active_until timestamp without time zone,
    is_active boolean DEFAULT TRUE
);

CREATE INDEX ON "mapping"."figi"(isin_source);

CREATE INDEX ON "mapping"."figi"(wkn_source);

CREATE INDEX ON "mapping"."figi"(ticker_source);

CREATE INDEX ON "mapping"."figi"(ticker_figi);

CREATE UNIQUE INDEX ON "mapping"."figi"(figi);


/* DOWN */
DROP TABLE IF EXISTS "mapping"."figi" CASCADE;

