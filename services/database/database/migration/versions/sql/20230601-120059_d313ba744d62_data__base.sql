/* UP */
CREATE TABLE IF NOT EXISTS "data"."timezone"(
    jd int PRIMARY KEY,
    name text,
    created_at timestamp without time zone DEFAULT (now() at time zone 'utc'),
    updated_at timestamp without time zone
);

CREATE TABLE IF NOT EXISTS "data"."currency"(
    id char(3) PRIMARY KEY, -- iso code
    currency_name varchar NOT NULL,
    created_at timestamp without time zone DEFAULT (now() at time zone 'utc'),
    updated_at timestamp without time zone
);

CREATE TABLE IF NOT EXISTS "data"."country"(
    id char(2) PRIMARY KEY, -- iso code
    name varchar NOT NULL,
    region varchar NOT NULL,
    currency char(2) NOT NULL,
    timezone int NOT NULL,
    created_at timestamp without time zone DEFAULT (now() at time zone 'utc'),
    updated_at timestamp without time zone
);

CREATE TABLE "data"."entity"(
    "id" integer PRIMARY KEY,
    "lei" text UNIQUE,
    "type_id" integer,
    "parent_id" integer,
    "name" text NOT NULL,
    "description" text,
    "legal_address_street" text,
    "legal_address_street_number" text,
    "legal_address_zip_code" text,
    "legal_address_city" text,
    "legal_address_country" text,
    "headquarter_address_street" text,
    "headquarter_address_street_number" text,
    "headquarter_address_city" text,
    "headquarter_address_zip_code" text,
    "headquarter_address_country" text,
    "jurisdiction" text,
    "industry_id" integer,
    "sector_id" integer,
    "country_id" text,
    "website" text,
    "email" text,
    "telephone" text,
    "is_active" boolean DEFAULT TRUE,
    "active_from" timestamp DEFAULT (now()),
    "active_until" timestamp,
    "created_at" timestamp DEFAULT (now()),
    "updated_at" timestamp
);

CREATE INDEX ON "data"."entity"("lei");

ALTER TABLE "data"."entity"
    ADD FOREIGN KEY ("parent_id") REFERENCES "data"."entity"("id");

CREATE TABLE "data"."entity_isin"(
    "id" integer PRIMARY KEY,
    "entity_id" integer,
    "isin" text UNIQUE,
    "is_active" boolean DEFAULT TRUE,
    "active_from" timestamp DEFAULT (now()),
    "active_until" timestamp,
    "created_at" timestamp DEFAULT (now()),
    "updated_at" timestamp
);

CREATE INDEX ON "data"."entity_isin"("isin");

ALTER TABLE "data"."entity_isin"
    ADD FOREIGN KEY ("entity_id") REFERENCES "data"."entity"("id");

CREATE TABLE "data"."entity_type"(
    "id" integer PRIMARY KEY,
    "name" text,
    "created_at" timestamp DEFAULT (now()),
    "updated_at" timestamp
);

ALTER TABLE "data"."entity"
    ADD FOREIGN KEY ("type_id") REFERENCES "data"."entity_type"("id");

CREATE TABLE "data"."security_type"(
    "id" integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    "parent_id" integer,
    "name" text NOT NULL,
    "name_figi" text,
    "name_figi2" text,
    "market_sector_figi" text,
    "is_leaf" boolean,
    "created_at" timestamp DEFAULT (now()),
    "updated_at" timestamp
);

COMMENT ON COLUMN "data"."security_type"."name_figi" IS 'OpenFigi Security type of the desired instrument';

COMMENT ON COLUMN "data"."security_type"."name_figi2" IS 'OpenFigi securityType2 is typically less specific than securityType';

COMMENT ON COLUMN "data"."security_type"."market_sector_figi" IS 'OpenFigi Market sector description of the desired instrument';

CREATE TABLE "data"."security"(
    "id" integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    "security_type_id" integer,
    "country_id" text,
    "name" text,
    "isin" text,
    "entity_isin_id" integer,
    "figi" text,
    "is_active" boolean DEFAULT TRUE,
    "active_from" timestamp DEFAULT (now()),
    "active_until" timestamp,
    "created_at" timestamp DEFAULT (now()),
    "updated_at" timestamp,
    "name_figi_alias" _text,
    "name_figi_count" int4
);

ALTER TABLE "data"."security"
    ADD UNIQUE ("figi", "isin");

ALTER TABLE "data"."security"
    ADD FOREIGN KEY ("entity_isin_id") REFERENCES "data"."entity_isin"("id");

CREATE INDEX ON "data"."security"("figi");

CREATE INDEX ON "data"."security"("isin");

CREATE TABLE "data"."security_ticker"(
    "id" integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    "security_id" integer,
    "ticker" text NOT NULL,
    "is_active" boolean DEFAULT TRUE,
    "active_from" timestamp DEFAULT (now()),
    "active_until" timestamp,
    "created_at" timestamp DEFAULT (now()),
    "updated_at" timestamp
);

ALTER TABLE "data"."security_ticker"
    ADD FOREIGN KEY ("security_id") REFERENCES "data"."security"("id");

ALTER TABLE "data"."security_ticker"
    ADD UNIQUE ("ticker", "security_id");

CREATE TABLE "data"."exchange"(
    "id" integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    "operating_exchange_id" integer,
    "mic" text UNIQUE,
    "name" text NOT NULL,
    "country_id" text,
    "currency" text,
    "website" text,
    "timezone" text,
    "comment" text,
    "acronym" text,
    "status" text,
    "source" text,
    "is_active" boolean DEFAULT TRUE,
    "is_virtual" boolean,
    "active_from" timestamp DEFAULT (now()),
    "active_until" timestamp,
    "created_at" timestamp DEFAULT (now()),
    "updated_at" timestamp
);

ALTER TABLE "data"."exchange"
    ADD FOREIGN KEY ("operating_exchange_id") REFERENCES "data"."exchange"("id");

CREATE TABLE "data"."index"(
    "id" integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    "name" text,
    "description" text,
    "created_at" timestamp DEFAULT (now()),
    "updated_at" timestamp
);

CREATE TABLE "data"."security_listing"(
    "id" integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    "exchange_id" integer,
    "security_ticker_id" integer,
    -- "index_id" integer,
    "currency" text,
    "figi" text UNIQUE,
    "quote_source" text,
    "is_primary" boolean,
    "is_active" boolean DEFAULT TRUE,
    "active_from" timestamp DEFAULT (now()),
    "active_until" timestamp,
    "created_at" timestamp DEFAULT (now()),
    "updated_at" timestamp
);

ALTER TABLE "data"."security_listing"
    ADD FOREIGN KEY ("security_ticker_id") REFERENCES "data"."security_ticker"("id");

ALTER TABLE "data"."security_listing"
    ADD FOREIGN KEY ("exchange_id") REFERENCES "data"."exchange"("id");

CREATE TABLE "data"."security_quote"(
    "security_listing_id" integer,
    "interval_id" integer,
    "timestamp" timestamp,
    "open" DECIMAL,
    "high" DECIMAL,
    "low" DECIMAL,
    "close" DECIMAL,
    "adj_open" DECIMAL,
    "adj_high" DECIMAL,
    "adj_low" DECIMAL,
    "adj_close" DECIMAL,
    "volume" DECIMAL,
    "created_at" timestamp DEFAULT (now()),
    "updated_at" timestamp,
    PRIMARY KEY ("security_listing_id", "interval_id", "timestamp")
);

ALTER TABLE "data"."security_quote"
    ADD FOREIGN KEY ("security_listing_id") REFERENCES "data"."security_listing"("id");

CREATE TABLE "data"."security_quote_interval"(
    "id" integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    "name" text,
    "min" integer,
    "created_at" timestamp DEFAULT (now()),
    "updated_at" timestamp
);

ALTER TABLE "data"."security_quote"
    ADD FOREIGN KEY ("interval_id") REFERENCES "data"."security_quote_interval"("id");


/* DOWN */
DROP TABLE IF EXISTS "data"."country" CASCADE;

DROP TABLE IF EXISTS "data"."currency" CASCADE;

DROP TABLE IF EXISTS "data"."timezone" CASCADE;

DROP TABLE IF EXISTS "data"."exchange" CASCADE;

DROP TABLE IF EXISTS "data"."security_type" CASCADE;

DROP TABLE IF EXISTS "data"."security" CASCADE;

DROP TABLE IF EXISTS "data"."index" CASCADE;

DROP TABLE IF EXISTS "data"."security_listing" CASCADE;

DROP TABLE IF EXISTS "data"."security_ticker" CASCADE;

DROP TABLE IF EXISTS "data"."entity" CASCADE;

DROP TABLE IF EXISTS "data"."entity_isin" CASCADE;

DROP TABLE IF EXISTS "data"."entity_type" CASCADE;

DROP TABLE IF EXISTS "data"."security_quote" CASCADE;

DROP TABLE IF EXISTS "data"."security_quote_interval" CASCADE;

