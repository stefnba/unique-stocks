/* UP */
CREATE TABLE IF NOT EXISTS "mapping"."figi"(
    isin text,
    wkn int,
    ticker text,
    ticker_figi text,
    name_figi text,
    exchange_mic text,
    currency varchar(3),
    country varchar(2),
    figi text NOT NULL,
    share_class_figi text,
    composite_figi text,
    security_type_id int,
    created_at timestamp without time zone DEFAULT (now() at time zone 'utc') NOT NULL,
    updated_at timestamp without time zone,
    active_from timestamp without time zone,
    active_until timestamp without time zone,
    is_active boolean DEFAULT TRUE
);

CREATE INDEX ON "mapping"."figi"(isin);

CREATE INDEX ON "mapping"."figi"(wkn);

CREATE INDEX ON "mapping"."figi"(ticker);

CREATE INDEX ON "mapping"."figi"(ticker_figi);

CREATE INDEX ON "mapping"."figi"(exchange_mic);

CREATE UNIQUE INDEX ON "mapping"."figi"(figi);


/* DOWN */
DROP TABLE IF EXISTS "mapping"."figi" CASCADE;

