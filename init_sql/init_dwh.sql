-- ============================================================
-- SecureLink - Data Warehouse
-- Este archivo se ejecuta automáticamente cuando se crea
-- el contenedor de postgres-dwh por primera vez.
-- Crea todas las tablas donde el pipeline guarda los resultados.
-- ============================================================

-- -------------------------------------------------------
-- TABLA: transacciones procesadas (resultado del pipeline)
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS transactions_processed (
    transaction_id      TEXT PRIMARY KEY,
    card_id             TEXT,
    user_id             TEXT,
    transaction_date    TIMESTAMP,
    amount              NUMERIC(12, 2),
    merchant_id         TEXT,
    merchant_city       TEXT,
    merchant_state      TEXT,
    mcc                 TEXT,
    mcc_description     TEXT,
    transaction_type    TEXT,       -- 'Online' o 'Swipe'
    errors              TEXT,
    -- Campos del usuario
    user_age            INTEGER,
    user_gender         TEXT,
    user_state          TEXT,
    yearly_income       NUMERIC(12, 2),
    total_debt          NUMERIC(12, 2),
    credit_score        INTEGER,
    -- Campos de la tarjeta
    card_brand          TEXT,       -- Visa, Mastercard, etc.
    card_type           TEXT,       -- Debit, Credit, Prepaid
    has_chip            BOOLEAN,
    card_on_dark_web    BOOLEAN,
    credit_limit        NUMERIC(12, 2),
    -- Indicadores derivados
    debt_income_ratio   NUMERIC(8, 4),
    distance_km         NUMERIC(10, 2),
    -- Etiqueta real de fraude
    is_fraud            BOOLEAN,
    -- Resultado de las reglas del pipeline
    is_suspicious       BOOLEAN,
    suspicion_reasons   TEXT,
    -- Cuándo fue procesado
    processed_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- -------------------------------------------------------
-- TABLA: métricas globales (para el panel general)
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS fraud_metrics_global (
    id                          SERIAL PRIMARY KEY,
    computed_at                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_transactions          INTEGER,
    total_fraud                 INTEGER,
    fraud_rate                  NUMERIC(8, 4),
    total_amount_at_risk        NUMERIC(16, 2),
    avg_fraud_amount            NUMERIC(12, 2),
    false_positive_rate         NUMERIC(8, 4),
    false_negative_rate         NUMERIC(8, 4),
    -- Dataset info
    dataset_start_date          DATE,
    dataset_end_date            DATE
);

-- -------------------------------------------------------
-- TABLA: fraude por categoría de comercio (MCC)
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS fraud_by_mcc (
    id                  SERIAL PRIMARY KEY,
    computed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    mcc                 TEXT,
    mcc_description     TEXT,
    total_transactions  INTEGER,
    total_fraud         INTEGER,
    fraud_rate          NUMERIC(8, 4),
    amount_at_risk      NUMERIC(16, 2)
);

-- -------------------------------------------------------
-- TABLA: fraude por tipo de tarjeta
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS fraud_by_card_type (
    id                  SERIAL PRIMARY KEY,
    computed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    card_brand          TEXT,
    card_type           TEXT,
    has_chip            BOOLEAN,
    total_transactions  INTEGER,
    total_fraud         INTEGER,
    fraud_rate          NUMERIC(8, 4),
    amount_at_risk      NUMERIC(16, 2)
);

-- -------------------------------------------------------
-- TABLA: fraude por estado de EEUU
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS fraud_by_state (
    id                  SERIAL PRIMARY KEY,
    computed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    state               TEXT,
    total_transactions  INTEGER,
    total_fraud         INTEGER,
    fraud_rate          NUMERIC(8, 4),
    amount_at_risk      NUMERIC(16, 2)
);

-- -------------------------------------------------------
-- TABLA: top merchants con más fraude
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS fraud_by_merchant (
    id                  SERIAL PRIMARY KEY,
    computed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    merchant_id         TEXT,
    merchant_city       TEXT,
    merchant_state      TEXT,
    mcc_description     TEXT,
    total_transactions  INTEGER,
    total_fraud         INTEGER,
    fraud_rate          NUMERIC(8, 4),
    amount_at_risk      NUMERIC(16, 2)
);

-- -------------------------------------------------------
-- TABLA: perfil de usuario (para el panel de usuario)
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_profiles (
    user_id             TEXT PRIMARY KEY,
    computed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    age                 INTEGER,
    gender              TEXT,
    state               TEXT,
    yearly_income       NUMERIC(12, 2),
    total_debt          NUMERIC(12, 2),
    debt_income_ratio   NUMERIC(8, 4),
    credit_score        INTEGER,
    num_cards           INTEGER,
    total_transactions  INTEGER,
    total_fraud         INTEGER,
    fraud_rate          NUMERIC(8, 4),
    total_spent         NUMERIC(16, 2),
    avg_transaction     NUMERIC(12, 2),
    amount_at_risk      NUMERIC(16, 2)
);

-- -------------------------------------------------------
-- TABLA: log de ejecuciones del pipeline
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_run_log (
    id              SERIAL PRIMARY KEY,
    run_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status          TEXT,       -- 'success', 'error', 'partial'
    rows_processed  INTEGER,
    rows_fraud      INTEGER,
    duration_secs   NUMERIC(10, 2),
    notes           TEXT
);

-- Mensaje de confirmación
DO $$ BEGIN
    RAISE NOTICE 'SecureLink DWH inicializado correctamente.';
END $$;
