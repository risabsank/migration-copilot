-- Validation pack (postgres)

SELECT 'users' AS table_name, (SELECT COUNT(*) FROM source.users) AS source_count, (SELECT COUNT(*) FROM target.users) AS target_count;
SELECT 'users' AS table_name, (SELECT SUM(hashtext(CAST(id AS STRING))) FROM source.users) AS source_pk_checksum, (SELECT SUM(hashtext(CAST(id AS STRING))) FROM target.users) AS target_pk_checksum;

