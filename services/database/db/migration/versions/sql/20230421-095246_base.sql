-- CREATE TABLE "country"(
--     "id" text PRIMARY KEY,
--     "name" text
-- );
CREATE TABLE "security_type"(
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

COMMENT ON COLUMN "public"."security_type"."name_figi" IS 'OpenFigi Security type of the desired instrument';

COMMENT ON COLUMN "public"."security_type"."name_figi2" IS 'OpenFigi securityType2 is typically less specific than securityType';

COMMENT ON COLUMN "public"."security_type"."market_sector_figi" IS 'OpenFigi Market sector description of the desired instrument';

CREATE TABLE "security"(
    "id" integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    "security_type_id" integer,
    "country_id" text,
    "name" text,
    "isin" text,
    "figi" text UNIQUE, -- share class figi
    "created_at" timestamp DEFAULT (now()),
    "updated_at" timestamp
);

CREATE TABLE "security_ticker"(
    "id" integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    "security_id" integer,
    "ticker" text NOT NULL UNIQUE,
    "created_at" timestamp DEFAULT (now()),
    "updated_at" timestamp
);

ALTER TABLE "security_ticker"
    ADD FOREIGN KEY ("security_id") REFERENCES "security"("id");

CREATE TABLE "exchange"(
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

ALTER TABLE "exchange"
    ADD FOREIGN KEY ("operating_exchange_id") REFERENCES "exchange"("id");

CREATE TABLE "index"(
    "id" integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    "name" text,
    "description" text,
    "created_at" timestamp DEFAULT (now()),
    "updated_at" timestamp
);

CREATE TABLE "security_listing"(
    "id" integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    "exchange_id" integer,
    "security_ticker_id" integer,
    -- "index_id" integer,
    "currency" text,
    "figi" text UNIQUE,
    "quote_source" text,
    "is_primary" boolean,
    "created_at" timestamp DEFAULT (now()),
    "updated_at" timestamp
);

ALTER TABLE "security_listing"
    ADD FOREIGN KEY ("security_ticker_id") REFERENCES "security_ticker"("id");

ALTER TABLE "security_listing"
    ADD FOREIGN KEY ("exchange_id") REFERENCES "exchange"("id");

