CREATE TABLE IF NOT EXISTS mappings(
    id SERIAL4 PRIMARY KEY,
    source varchar NOT NULL,
    product varchar NOT NULL,
    field varchar,
    source_value varchar NOT NULL,
    uid varchar NOT NULL,
    uid_description varchar,
    is_seed boolean DEFAULT FALSE,
    created_at timestamp without time zone DEFAULT (now() at time zone 'utc') NOT NULL,
    updated_at timestamp without time zone,
    valid_from timestamp without time zone,
    valid_until timestamp without time zone,
    is_active boolean DEFAULT TRUE
    --,UNIQUE (product, field, uid)
);

CREATE INDEX ON mappings(source);

CREATE INDEX ON mappings(field);

CREATE INDEX ON mappings(product);

CREATE INDEX ON mappings(source_value);

CREATE INDEX ON mappings(uid);

