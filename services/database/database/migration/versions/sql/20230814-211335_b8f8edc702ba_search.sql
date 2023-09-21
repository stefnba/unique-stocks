/* UP */
CREATE EXTENSION IF NOT EXISTS pg_trgm;

ALTER TABLE "data"."exchange"
    ADD "search_token" tsvector GENERATED ALWAYS AS (to_tsvector('simple', name) || ' ' || to_tsvector('simple', mic)) STORED;

CREATE INDEX ON "data"."exchange" USING GIN("search_token");

ALTER TABLE "data"."entity"
    ADD "search_token" tsvector GENERATED ALWAYS AS (to_tsvector('simple', name)) STORED;

CREATE INDEX ON "data"."entity" USING GIN("search_token");

ALTER TABLE "data"."security"
    ADD "search_token" tsvector GENERATED ALWAYS AS (to_tsvector('simple', name)) STORED;

CREATE INDEX ON "data"."security" USING GIN("search_token");


/* DOWN */
