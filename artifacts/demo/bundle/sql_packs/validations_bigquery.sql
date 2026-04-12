-- Validation pack (bigquery)

SELECT 'customers' AS table_name, (SELECT COUNT(*) FROM source.customers) AS source_count, (SELECT COUNT(*) FROM target.customers) AS target_count;
SELECT 'customers' AS table_name, (SELECT SUM(farm_fingerprint(CAST(id AS STRING))) FROM source.customers) AS source_pk_checksum, (SELECT SUM(farm_fingerprint(CAST(id AS STRING))) FROM target.customers) AS target_pk_checksum;

SELECT 'orders' AS table_name, (SELECT COUNT(*) FROM source.orders) AS source_count, (SELECT COUNT(*) FROM target.orders) AS target_count;
SELECT 'orders' AS table_name, (SELECT SUM(farm_fingerprint(CAST(id AS STRING))) FROM source.orders) AS source_pk_checksum, (SELECT SUM(farm_fingerprint(CAST(id AS STRING))) FROM target.orders) AS target_pk_checksum;

