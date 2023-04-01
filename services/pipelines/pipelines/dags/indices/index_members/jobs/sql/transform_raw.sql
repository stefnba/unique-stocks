SELECT
    Code AS code,
    Name AS name,
    Exchange AS exchange_source_code,
    Sector AS sector,
    Industry AS industry,
    $source AS source,
    $index_code AS index_code
FROM
    $members AS members
