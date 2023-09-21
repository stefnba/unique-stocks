WITH "fts" AS (
    SELECT
        $<joinColumnFtsQuery:name>,
        "query",
        "rank",
        "similarity",
        $<tokenColumn:name>
    FROM
        $<table>,
        websearch_to_tsquery('$<searchQuery:value>') AS "query",
        SIMILARITY('$<searchQuery:value>', $<tokenColumn:name>::text) AS "similarity",
        ts_rank_cd($<tokenColumn:name>, "query") AS "rank"
    WHERE
        $<filter:raw> query @@ $<tokenColumn:name>
        OR similarity >= $<similarityThreshold>
),
"originalQuery" AS (
$<originalQuery:raw>
)
SELECT
    "originalQuery".*,
    "fts".*
FROM
    "fts"
    JOIN "originalQuery" ON "originalQuery".$<joinColumnOriginalQuery:name> = "fts".$<joinColumnFtsQuery:name>
ORDER BY
    "rank",
    "similarity" DESC NULLS LAST
