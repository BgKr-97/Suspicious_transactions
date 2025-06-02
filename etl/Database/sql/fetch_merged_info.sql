-- ===================================================================
-- 1. Извлекаем дополнительные (справочные) данные из схемы staging, необходимые для их объединения
--    с трансформированными данными перед заливкой в витрину
-- ===================================================================
SELECT
    t.id AS transaction_id,
    cl.full_name AS client_name,
    t.account_id,
    tt.is_receipt,
    src_region.region_name AS sender_region,
    src_city.city_name AS sender_city,
    src_country.country_name AS sender_country,
    dst_region.region_name AS recipient_region,
    dst_city.city_name AS recipient_city,
    dst_country.country_name AS recipient_country,
    dst_city.latitude AS recipient_latitude,
    dst_city.longitude AS recipient_longitude
FROM core.transactions AS t
JOIN core.clients AS cl
    ON t.client_id = cl.client_id
JOIN core.transaction_types AS tt
    ON t.transaction_type_id = tt.id
JOIN core.cities AS src_city
    ON t.source_city_id = src_city.city_id
JOIN core.countries AS src_country
    ON src_country.country_id = src_city.country_id
JOIN core.cities AS dst_city
    ON t.destination_city_id = dst_city.city_id
JOIN core.countries AS dst_country
    ON dst_country.country_id = dst_city.country_id
JOIN core.regions as src_region
    ON src_region.region_id = t.source_region_id
JOIN core.regions as dst_region
    ON dst_region.region_id = t.destination_region_id;