-- Backfill script for orders
-- Chunk target: 3000000 rows
INSERT INTO target.orders
SELECT * FROM source.orders
WHERE id > :lower_pk AND id <= :upper_pk
ORDER BY id;
