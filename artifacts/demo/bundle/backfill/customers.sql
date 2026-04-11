-- Backfill script for customers
-- Chunk target: 3000000 rows
INSERT INTO target.customers
SELECT * FROM source.customers
WHERE id > :lower_pk AND id <= :upper_pk
ORDER BY id;
