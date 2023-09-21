/* UP */
CREATE TABLE IF NOT EXISTS "data"."fundamental_type"(
    "id" SERIAL4 PRIMARY KEY,
    "name" text,
    "type" text,
    "created_at" timestamp without time zone DEFAULT (now() at time zone 'utc'),
    "updated_at" timestamp without time zone
);

CREATE TABLE IF NOT EXISTS "data"."fundamental_period_type"(
    "id" SERIAL4 PRIMARY KEY,
    "name" text,
    "created_at" timestamp without time zone DEFAULT (now() at time zone 'utc'),
    "updated_at" timestamp without time zone
);

CREATE TABLE IF NOT EXISTS "data"."fundamental"(
    "id" SERIAL4 PRIMARY KEY,
    "entity_id" integer,
    "seucrity_id" integer,
    "type_id" integer,
    "period_id" int,
    "period" date,
    "currency" char(3),
    "value" text,
    "source" text,
    "created_at" timestamp without time zone DEFAULT (now() at time zone 'utc'),
    "updated_at" timestamp without time zone,
    "published_at" timestamp without time zone
);

ALTER TABLE "data"."fundamental"
    ADD FOREIGN KEY ("entity_id") REFERENCES "data"."entity"("id");

ALTER TABLE "data"."fundamental"
    ADD FOREIGN KEY ("type_id") REFERENCES "data"."fundamental_type"("id");

ALTER TABLE "data"."fundamental"
    ADD FOREIGN KEY ("period_id") REFERENCES "data"."fundamental_period_type"("id");


/* DOWN */
DROP TABLE IF EXISTS "data"."fundamental_type" CASCADE;

DROP TABLE IF EXISTS "data"."fundamental_period_type" CASCADE;

DROP TABLE IF EXISTS "data"."fundamental" CASCADE;

