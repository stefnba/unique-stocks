/* UP */
CREATE TABLE IF NOT EXISTS users (
    id serial PRIMARY KEY,
    username varchar NOT NULL,
    email varchar NOT NULL
);


/* DOWN */
DROP TABLE IF EXISTS users CASCADE;

