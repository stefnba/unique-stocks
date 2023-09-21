WITH "members_nested" AS (
UNPIVOT( FROM (
            SELECT
                UNNEST($data.Components)
            FROM $data)) ON COLUMNS(*) INTO NAME "key" VALUE "value"
),
members AS (
    SELECT
        unnest(members_nested."value")
    FROM
        members_nested
)
SELECT
    members.code AS code,
    members.Exchange AS exchange_code,
    members.Name AS name,
    $data.General.Code AS index_code
FROM
    members,
    $data
