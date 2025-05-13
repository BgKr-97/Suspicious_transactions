-- ===================================================================
-- 1. Извлекаем основные данные из схемы staging, необходимые для их трансформации
-- ===================================================================
SELECT
    t.id AS transaction_id,
    t.client_id,
    cl.birth_date,
    t.date_time,
    t.amount,
    tt.t_type,
    src_city.latitude AS sender_latitude,
    src_city.longitude AS sender_longitude,
    dst_country.blacklist
FROM core.transactions AS t
JOIN core.clients AS cl
    ON t.client_id = cl.client_id
JOIN core.transaction_types AS tt
    ON t.transaction_type_id = tt.id
JOIN core.cities AS src_city
    ON t.source_city_id = src_city.city_id
JOIN core.cities AS dst_city
    ON t.destination_city_id = dst_city.city_id
JOIN core.countries AS dst_country
    ON dst_city.country_id = dst_country.country_id;