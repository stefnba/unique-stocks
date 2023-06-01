/* UP */
CREATE TABLE IF NOT EXISTS "mapping"."mapping"(
    id SERIAL4 PRIMARY KEY,
    source varchar NOT NULL,
    product varchar NOT NULL,
    field varchar,
    source_value varchar NOT NULL,
    source_description varchar,
    uid varchar NOT NULL,
    uid_description varchar,
    is_seed boolean DEFAULT FALSE,
    created_at timestamp without time zone DEFAULT (now() at time zone 'utc') NOT NULL,
    updated_at timestamp without time zone,
    active_from timestamp without time zone,
    active_until timestamp without time zone,
    is_active boolean DEFAULT TRUE
    --,UNIQUE (product, field, uid)
);

ALTER TABLE "mapping"."mapping"
    ADD UNIQUE (source, product, source_value, uid, is_active);

CREATE INDEX ON "mapping"."mapping"(source);

CREATE INDEX ON "mapping"."mapping"(field);

CREATE INDEX ON "mapping"."mapping"(product);

CREATE INDEX ON "mapping"."mapping"(source_value);

CREATE INDEX ON "mapping"."mapping"(uid);


/* DOWN */
DROP TABLE IF EXISTS "mapping"."mapping" CASCADE;

