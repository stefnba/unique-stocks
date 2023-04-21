CREATE TABLE IF NOT EXISTS surrogate_keys(
    surrogate_key SERIAL4 PRIMARY KEY,
    product varchar NOT NULL,
    uid varchar NOT NULL,
    created_at timestamp without time zone DEFAULT (now() at time zone 'utc'),
    updated_at timestamp without time zone,
    valid_until timestamp without time zone,
    valid_from timestamp without time zone,
    is_active boolean DEFAULT TRUE,
    UNIQUE (uid, product)
);

CREATE INDEX ON surrogate_keys(product);

CREATE INDEX ON surrogate_keys(uid);

CREATE INDEX ON surrogate_keys(is_active);

