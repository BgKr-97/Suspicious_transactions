-- ===================================================================
-- 0. Удаляем схему data_mart со всем содержимым
-- ===================================================================
DROP SCHEMA IF EXISTS data_mart CASCADE;

-- ===================================================================
-- 1. Создаём схему data_mart
-- ===================================================================
CREATE SCHEMA IF NOT EXISTS data_mart;

-- ===================================================================
-- 2. Создаём таблицу витрины data_table
-- ===================================================================
CREATE TABLE IF NOT EXISTS data_mart.data_table (
    -- Клиенты
    transaction_id           INTEGER PRIMARY KEY,
    client_id                INTEGER,
    client_name              VARCHAR,
    client_age               INTEGER,
    account_id               INTEGER,

    -- Временные метки
    date_time                TIMESTAMP,

    -- Финансовые данные
    amount                   NUMERIC(15, 2),
    t_type                   TEXT,
    is_receipt               BOOLEAN,

    -- Геоданные отправителя
    sender_country           TEXT,
    sender_city              TEXT,
    sender_region            TEXT,
    sender_latitude          NUMERIC,
    sender_longitude         NUMERIC,

    -- Геоданные получателя
    recipient_country        TEXT,
    recipient_city           TEXT,
    recipient_region         TEXT,
    recipient_latitude       NUMERIC,
    recipient_longitude      NUMERIC,

    -- Анализ риска
    is_suspicious            BOOLEAN,
    risk_score               INTEGER,
    reason_flags             VARCHAR,
    risk_status              VARCHAR
);