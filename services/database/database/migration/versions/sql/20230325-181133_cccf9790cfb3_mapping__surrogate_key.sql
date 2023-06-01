/* UP */
CREATE TABLE IF NOT EXISTS "mapping"."surrogate_key"(
    surrogate_key SERIAL4 PRIMARY KEY,
    product varchar NOT NULL,
    uid varchar NOT NULL,
    created_at timestamp without time zone DEFAULT (now() at time zone 'utc'),
    updated_at timestamp without time zone,
    active_until timestamp without time zone,
    active_from timestamp without time zone,
    is_active boolean DEFAULT TRUE,
    UNIQUE (uid, product)
);

CREATE INDEX ON "mapping"."surrogate_key"(product);

CREATE INDEX ON "mapping"."surrogate_key"(uid);

CREATE INDEX ON "mapping"."surrogate_key"(is_active);


/* DOWN */
DROP TABLE IF EXISTS "mapping"."surrogate_key" CASCADE;

