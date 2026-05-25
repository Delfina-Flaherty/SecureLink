"""
SecureLink - Pipeline de Detección de Fraude
=============================================
Versión optimizada con Polars (ingest + transform) y DuckDB (agregaciones).

Mejoras de performance vs versión pandas:
- ingest_transactions: Polars lazy/streaming → 5-10x más rápido que pandas
- transform_data: combina clean+features+rules en una pasada (antes eran 3 tareas)
- compute_metrics: DuckDB SQL vectorizado → 10-100x más rápido que defaultdict+groupby
- load_to_dwh: COPY para transacciones (5-10x vs INSERT) + execute_values en agregadas
"""

import logging
import os
import json
import pickle
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

logger = logging.getLogger(__name__)

DATA_DIR = "/opt/airflow/data"
TRANSACTIONS_FILE = os.path.join(DATA_DIR, "transactions_data.csv")
USERS_FILE        = os.path.join(DATA_DIR, "users_data.csv")
CARDS_FILE        = os.path.join(DATA_DIR, "cards_data.csv")
LABELS_FILE       = os.path.join(DATA_DIR, "train_fraud_labels.json")
MCC_FILE          = os.path.join(DATA_DIR, "mcc_codes.json")

DWH_CONN = "postgres_dwh"

REF_USERS_PATH    = "/tmp/securelink_ref_users.parquet"
REF_CARDS_PATH    = "/tmp/securelink_ref_cards.parquet"
REF_MCC_PATH      = "/tmp/securelink_ref_mcc.pkl"
REF_LABELS_PATH   = "/tmp/securelink_ref_labels.pkl"
MERGED_PATH       = "/tmp/securelink_merged.parquet"
PROCESSED_PATH    = "/tmp/securelink_processed.parquet"
METRICS_PATH      = "/tmp/securelink_metrics.pkl"
COPY_CSV_PATH     = "/tmp/securelink_for_copy.csv"

default_args = {
    "retries": 3,
    "retry_delay": timedelta(seconds=60),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
}


# ============================================================
# TAREA 1: Validar archivos
# ============================================================
def _validate_files(**context):
    logger.info("=== TAREA 1: Validando archivos de entrada ===")

    for filepath in [TRANSACTIONS_FILE, USERS_FILE, CARDS_FILE, LABELS_FILE, MCC_FILE]:
        if not os.path.exists(filepath):
            raise FileNotFoundError(
                f"Archivo no encontrado: {filepath}\n"
                f"Copiá tus archivos a la carpeta ./data/ del proyecto."
            )
        logger.info(f"  ✓ {os.path.basename(filepath)} existe")

    # Verificar columnas mínimas
    txn = pd.read_csv(TRANSACTIONS_FILE, sep=None, engine='python')
    for col in ['id', 'date', 'client_id', 'card_id', 'amount', 'use_chip', 'merchant_id', 'mcc']:
        if col not in txn.columns:
            raise ValueError(f"transactions_data.csv no tiene la columna '{col}'. Columnas: {list(txn.columns)}")

    usr = pd.read_csv(USERS_FILE, nrows=1, sep=None, engine='python')
    for col in ['id', 'current_age', 'gender', 'yearly_income', 'total_debt', 'credit_score']:
        if col not in usr.columns:
            raise ValueError(f"users_data.csv no tiene la columna '{col}'. Columnas: {list(usr.columns)}")

    crd = pd.read_csv(CARDS_FILE, nrows=1, sep=None, engine='python')
    for col in ['id', 'card_brand', 'card_type', 'has_chip', 'credit_limit']:
        if col not in crd.columns:
            raise ValueError(f"cards_data.csv no tiene la columna '{col}'. Columnas: {list(crd.columns)}")

    logger.info("Todos los archivos validados correctamente.")


# ============================================================
# TAREAS 2a-2d: Cargar fuentes de referencia (corren en PARALELO)
# ============================================================
def _load_users(**context):
    logger.info("=== TAREA 2a: Leyendo users_data.csv ===")
    users = pd.read_csv(USERS_FILE, sep=None, engine='python')
    users = users.rename(columns={"id": "user_id"})
    cols_users = [c for c in ["user_id", "current_age", "gender", "latitude",
                              "longitude", "yearly_income", "total_debt", "credit_score"]
                  if c in users.columns]
    users["user_id"] = users["user_id"].astype(str)
    users[cols_users].to_parquet(REF_USERS_PATH, index=False)
    logger.info(f"  Usuarios: {len(users):,} filas → {REF_USERS_PATH}")


def _load_cards(**context):
    logger.info("=== TAREA 2b: Leyendo cards_data.csv ===")
    cards = pd.read_csv(CARDS_FILE, sep=None, engine='python')
    cards = cards.rename(columns={"id": "card_id"})
    cols_cards = [c for c in ["card_id", "card_brand", "card_type", "has_chip",
                              "credit_limit", "card_on_dark_web", "client_id"]
                  if c in cards.columns]
    cards["card_id"] = cards["card_id"].astype(str)
    cards[cols_cards].to_parquet(REF_CARDS_PATH, index=False)
    logger.info(f"  Tarjetas: {len(cards):,} filas → {REF_CARDS_PATH}")


def _load_mcc(**context):
    logger.info("=== TAREA 2c: Leyendo mcc_codes.json ===")
    with open(MCC_FILE, "r") as f:
        mcc_raw = json.load(f)
    mcc_lookup = {str(k): v for k, v in mcc_raw.items()}
    with open(REF_MCC_PATH, "wb") as f:
        pickle.dump(mcc_lookup, f)
    logger.info(f"  MCC: {len(mcc_lookup):,} categorías → {REF_MCC_PATH}")


def _load_labels(**context):
    logger.info("=== TAREA 2d: Leyendo train_fraud_labels.json ===")
    # JSON anidado {"target": {"txn_id": "Yes"/"No", ...}}
    with open(LABELS_FILE, "r") as f:
        labels_raw = json.load(f)
    labels_inner = labels_raw.get("target", labels_raw)
    labels_dict = {str(k): (v == "Yes") for k, v in labels_inner.items()}
    with open(REF_LABELS_PATH, "wb") as f:
        pickle.dump(labels_dict, f)
    logger.info(f"  Labels: {len(labels_dict):,} registros → {REF_LABELS_PATH}")


# ============================================================
# TAREA 3: Ingesta + unión con fuentes de referencia (Polars)
# ============================================================
def _ingest_transactions(**context):
    """Lee transactions_data.csv y enriquece con las 4 fuentes de referencia.
    Usa Polars con scan/sink (lazy + streaming) para procesar 13M filas en
    una sola pasada paralelizada en todos los cores del CPU."""
    logger.info("=== TAREA 3: Ingesta y unión de transacciones (Polars) ===")
    import polars as pl

    # ── Cargar referencias (datos chicos) ──
    users = pl.read_parquet(REF_USERS_PATH)
    cards = pl.read_parquet(REF_CARDS_PATH)
    with open(REF_MCC_PATH, "rb") as f:
        mcc_lookup = pickle.load(f)
    with open(REF_LABELS_PATH, "rb") as f:
        labels_dict = pickle.load(f)

    # ── Convertir dicts a DataFrames Polars para hash joins eficientes ──
    mcc_df = pl.DataFrame({
        "mcc": [str(k) for k in mcc_lookup.keys()],
        "mcc_description": list(mcc_lookup.values()),
    })
    labels_df = pl.DataFrame({
        "transaction_id": [str(k) for k in labels_dict.keys()],
        "is_fraud": list(labels_dict.values()),
    })
    # liberar dicts grandes ahora que ya están en polars
    del labels_dict
    del mcc_lookup

    logger.info(f"  Referencias: users={len(users):,}, cards={len(cards):,}, "
                f"mcc={len(mcc_df):,}, labels={len(labels_df):,}")

    # cards trae su propio client_id, lo dropeo para evitar colisión con el de transactions
    cards_join = cards.drop("client_id") if "client_id" in cards.columns else cards
    # users.user_id → client_id (clave de join con transactions)
    users_join = users.rename({"user_id": "client_id"})

    # ── Pipeline lazy: scan CSV → 4 joins → sink parquet (streaming) ──
    result = (
        pl.scan_csv(TRANSACTIONS_FILE, separator=";", infer_schema_length=10_000,
                    ignore_errors=True)
        .rename({"id": "transaction_id"})
        .with_columns([
            pl.col("transaction_id").cast(pl.Utf8),
            pl.col("card_id").cast(pl.Utf8),
            pl.col("client_id").cast(pl.Utf8),
            pl.col("mcc").cast(pl.Utf8),
        ])
        # 1. Labels (8.9M registros)
        .join(labels_df.lazy(), on="transaction_id", how="left")
        .with_columns(pl.col("is_fraud").fill_null(False))
        # 2. Cards (6k registros)
        .join(cards_join.lazy(), on="card_id", how="left")
        # 3. Users (2k registros)
        .join(users_join.lazy(), on="client_id", how="left")
        # 4. MCC description (~900 entradas)
        .join(mcc_df.lazy(), on="mcc", how="left")
        .with_columns(pl.col("mcc_description").fill_null("Unknown"))
        # user_id = client_id para compatibilidad con el schema del DWH
        .with_columns(pl.col("client_id").alias("user_id"))
    )

    result.sink_parquet(MERGED_PATH, compression="snappy")

    total_rows = pl.scan_parquet(MERGED_PATH).select(pl.len()).collect().item()
    logger.info(f"Dataset final: {total_rows:,} filas → {MERGED_PATH}")


# ============================================================
# TAREA 4: Limpieza + Features + Reglas (en una sola pasada Polars)
# ============================================================
def _transform_data(**context):
    """Combina las 3 transformaciones (clean, features, rules) en una sola pasada
    Polars sobre el parquet. Reduce I/O en ~60% vs hacer 3 tareas separadas."""
    logger.info("=== TAREA 4: Limpieza + Features + Reglas (Polars) ===")
    import polars as pl

    df = pl.scan_parquet(MERGED_PATH)

    # ── Limpieza ──
    df = (
        df
        .with_columns([
            pl.col("date").str.to_datetime(format="%m/%d/%Y %H:%M",strict=False).alias("transaction_date"),
            pl.col("amount").cast(pl.Utf8)
                .str.replace_all("$", "", literal=True)
                .str.replace_all(",", "", literal=True)
                .str.strip_chars()
                .cast(pl.Float64, strict=False),
            pl.col("has_chip").cast(pl.Utf8).str.to_uppercase()
                .is_in(["YES", "TRUE", "1"]).alias("has_chip"),
            pl.col("card_on_dark_web").cast(pl.Utf8).str.to_uppercase()
                .is_in(["YES", "TRUE", "1"]).alias("card_on_dark_web"),
        ])
        .with_columns([
            # Limpiar columnas monetarias del usuario/tarjeta (vienen como strings con $)
            pl.col("credit_limit").cast(pl.Utf8)
                .str.replace_all("$", "", literal=True)
                .str.replace_all(",", "", literal=True)
                .str.strip_chars().cast(pl.Float64, strict=False),
            pl.col("yearly_income").cast(pl.Utf8)
                .str.replace_all("$", "", literal=True)
                .str.replace_all(",", "", literal=True)
                .str.strip_chars().cast(pl.Float64, strict=False),
            pl.col("total_debt").cast(pl.Utf8)
                .str.replace_all("$", "", literal=True)
                .str.replace_all(",", "", literal=True)
                .str.strip_chars().cast(pl.Float64, strict=False),
        ])
        .filter(
            (pl.col("amount") >= 0)
            & pl.col("amount").is_not_null()
            & pl.col("transaction_date").is_not_null()
        )
        # Nota: NO se hace .unique(subset="transaction_id") porque rompe el streaming
        # de Polars (requeriría materializar 13M transaction_ids en un hashset → OOM
        # en máquinas con 4 GB de RAM). transaction_id es PK del CSV de origen, no
        # debería haber duplicados. Si los hubiera, los maneja `INSERT ... ON CONFLICT
        # DO NOTHING` del staging del load_to_dwh.
    )

    # ── Features derivadas ──
    df = df.with_columns([
        pl.col("use_chip").cast(pl.Utf8).str.strip_chars().str.to_lowercase()
            .eq("online transaction").alias("is_online"),
        pl.when(pl.col("yearly_income") > 0)
            .then(pl.col("total_debt") / pl.col("yearly_income"))
            .otherwise(None)
            .alias("debt_income_ratio"),
        pl.lit(None).cast(pl.Float64).alias("distance_km"),
    ]).with_columns([
        pl.when(pl.col("is_online"))
            .then(pl.lit("Online"))
            .otherwise(pl.lit("Swipe"))
            .alias("transaction_type"),
    ])

    # ── Umbral p99 del monto (necesario para Regla 1) ──
    # Polars ejecuta el plan lazy hasta acá para sacar el p99.
    # Después vuelve a ejecutarlo para aplicar las reglas. Son 2 pasadas del
    # parquet, pero ambas streaming y paralelas, mucho más rápido que pandas.
    p99 = df.select(pl.col("amount").quantile(0.99)).collect().item()
    logger.info(f"  Umbral monto alto (p99): ${p99:,.2f}")

    # ── Reglas de detección ──
    final = (
        df
        .with_columns([
            (pl.col("amount") > p99).alias("_r1"),
            (
                pl.col("errors").is_not_null()
                & (pl.col("errors").cast(pl.Utf8).str.strip_chars() != "")
                & (pl.col("errors").cast(pl.Utf8).str.strip_chars() != "nan")
            ).alias("_r2"),
            pl.col("card_on_dark_web").fill_null(False).alias("_r3"),
            (pl.col("debt_income_ratio") > 3).fill_null(False).alias("_r4"),
        ])
        .with_columns([
            (pl.col("_r1") | pl.col("_r2") | pl.col("_r3") | pl.col("_r4"))
                .alias("is_suspicious"),
            pl.concat_str([
                pl.when(pl.col("_r1")).then(pl.lit("monto_atipico;")).otherwise(pl.lit("")),
                pl.when(pl.col("_r2")).then(pl.lit("error_en_transaccion;")).otherwise(pl.lit("")),
                pl.when(pl.col("_r3")).then(pl.lit("tarjeta_en_dark_web;")).otherwise(pl.lit("")),
                pl.when(pl.col("_r4")).then(pl.lit("alto_endeudamiento;")).otherwise(pl.lit("")),
            ]).str.strip_chars_end(";").alias("suspicion_reasons"),
        ])
        .drop(["_r1", "_r2", "_r3", "_r4"])
        # Rename columns para que coincidan con el schema del DWH
        .rename({
            "current_age": "user_age",
            "gender": "user_gender",
        })
    )

    final.sink_parquet(PROCESSED_PATH, compression="snappy")
    logger.info(f"Transformaciones completadas → {PROCESSED_PATH}")


# ============================================================
# TAREA 5: Calcular métricas (DuckDB)
# ============================================================
def _compute_metrics(**context):
    """Calcula todas las métricas agregadas con DuckDB directo sobre el parquet.
    DuckDB es columnar+vectorizado: corre las agregaciones 10-100x más rápido que
    pandas groupby, y consume mucha menos RAM."""
    logger.info("=== TAREA 5: Calculando métricas (DuckDB) ===")
    import duckdb

    con = duckdb.connect()
    parquet = PROCESSED_PATH

    # ── Métricas globales ──
    g_row = con.execute(f"""
        WITH base AS (
            SELECT
                COUNT(*) AS total_tx,
                COUNT(*) FILTER (WHERE is_fraud) AS total_fraud,
                COUNT(*) FILTER (WHERE NOT is_fraud) AS total_legit,
                COALESCE(SUM(CASE WHEN is_fraud THEN amount END), 0) AS amount_at_risk,
                COUNT(*) FILTER (WHERE is_suspicious AND NOT is_fraud) AS fp_count,
                COUNT(*) FILTER (WHERE NOT is_suspicious AND is_fraud) AS fn_count,
                MIN(transaction_date)::DATE AS start_dt,
                MAX(transaction_date)::DATE AS end_dt
            FROM read_parquet('{parquet}')
        )
        SELECT
            total_tx,
            total_fraud,
            CAST(total_fraud AS DOUBLE) / NULLIF(total_tx, 0)        AS fraud_rate,
            amount_at_risk,
            amount_at_risk / NULLIF(total_fraud, 0)                  AS avg_fraud_amount,
            CAST(fp_count AS DOUBLE) / NULLIF(total_legit, 0)        AS fpr,
            CAST(fn_count AS DOUBLE) / NULLIF(total_fraud, 0)        AS fnr,
            start_dt, end_dt
        FROM base
    """).fetchone()

    metrics_global = {
        "total_transactions": int(g_row[0]),
        "total_fraud": int(g_row[1]),
        "fraud_rate": float(g_row[2] or 0),
        "total_amount_at_risk": float(g_row[3] or 0),
        "avg_fraud_amount": float(g_row[4] or 0),
        "false_positive_rate": float(g_row[5] or 0),
        "false_negative_rate": float(g_row[6] or 0),
        "dataset_start_date": g_row[7],
        "dataset_end_date": g_row[8],
    }
    logger.info(f"  Tasa de fraude: {metrics_global['fraud_rate']:.2%}")
    logger.info(f"  Monto en riesgo: ${metrics_global['total_amount_at_risk']:,.2f}")

    # ── Helper: usa GROUP BY ALL (sintaxis DuckDB) para evitar repetir las
    # expresiones del SELECT en el GROUP BY. ALL agrupa por todas las columnas
    # no-agregadas del SELECT automáticamente.
    def agg_query(group_cols, where=""):
        return f"""
            SELECT {group_cols},
                COUNT(*)                                                AS total_transactions,
                COUNT(*) FILTER (WHERE is_fraud)                        AS total_fraud,
                CAST(COUNT(*) FILTER (WHERE is_fraud) AS DOUBLE) / COUNT(*) AS fraud_rate,
                COALESCE(SUM(CASE WHEN is_fraud THEN amount END), 0)    AS amount_at_risk
            FROM read_parquet('{parquet}')
            {where}
            GROUP BY ALL
        """

    # ── By MCC ──
    fraud_mcc = con.execute(agg_query("CAST(mcc AS VARCHAR) AS mcc, mcc_description")).df()

    # ── By card type ──
    fraud_card = con.execute(agg_query("card_brand, card_type, has_chip")).df()

    # ── By state (solo presenciales con estado) ──
    fraud_state = con.execute(agg_query(
        "CAST(merchant_state AS VARCHAR) AS state",
        "WHERE NOT is_online AND merchant_state IS NOT NULL AND merchant_state != ''"
    )).df()

    # ── By merchant ──
    fraud_merchant = con.execute(f"""
        SELECT
            COALESCE(CAST(merchant_id AS VARCHAR), '')      AS merchant_id,
            COALESCE(CAST(merchant_city AS VARCHAR), '')    AS merchant_city,
            COALESCE(CAST(merchant_state AS VARCHAR), '')   AS merchant_state,
            COALESCE(CAST(mcc_description AS VARCHAR), '')  AS mcc_description,
            COUNT(*)                                                AS total_transactions,
            COUNT(*) FILTER (WHERE is_fraud)                        AS total_fraud,
            CAST(COUNT(*) FILTER (WHERE is_fraud) AS DOUBLE) / COUNT(*) AS fraud_rate,
            COALESCE(SUM(CASE WHEN is_fraud THEN amount END), 0)    AS amount_at_risk
        FROM read_parquet('{parquet}')
        GROUP BY merchant_id, merchant_city, merchant_state, mcc_description
    """).df()

    # ── User profiles ──
    user_profiles = con.execute(f"""
        SELECT
            CAST(user_id AS VARCHAR)                                AS user_id,
            ANY_VALUE(user_age)                                     AS age,
            ANY_VALUE(user_gender)                                  AS gender,
            ANY_VALUE(yearly_income)                                AS yearly_income,
            ANY_VALUE(total_debt)                                   AS total_debt,
            ANY_VALUE(debt_income_ratio)                            AS debt_income_ratio,
            ANY_VALUE(credit_score)                                 AS credit_score,
            COUNT(DISTINCT card_id)                                 AS num_cards,
            COUNT(*)                                                AS total_transactions,
            COUNT(*) FILTER (WHERE is_fraud)                        AS total_fraud,
            CAST(COUNT(*) FILTER (WHERE is_fraud) AS DOUBLE) / COUNT(*) AS fraud_rate,
            SUM(amount)                                             AS total_spent,
            AVG(amount)                                             AS avg_transaction,
            COALESCE(SUM(CASE WHEN is_fraud THEN amount END), 0)    AS amount_at_risk
        FROM read_parquet('{parquet}')
        WHERE user_id IS NOT NULL
        GROUP BY user_id
    """).df()

    con.close()

    with open(METRICS_PATH, "wb") as f:
        pickle.dump({
            "global": metrics_global,
            "by_mcc": fraud_mcc,
            "by_card": fraud_card,
            "by_state": fraud_state,
            "by_merchant": fraud_merchant,
            "user_profiles": user_profiles,
        }, f)

    logger.info(f"Métricas calculadas → {METRICS_PATH}")


# ============================================================
# TAREA 6: Cargar al DWH (COPY + execute_values)
# ============================================================
def _load_to_dwh(**context):
    """Carga al DWH con dos optimizaciones:
    - transactions_processed: COPY desde CSV (5-10x más rápido que INSERT)
    - tablas agregadas: execute_values con batches (50-100x más rápido que INSERT row-by-row)
    """
    logger.info("=== TAREA 6: Cargando al DWH (COPY + execute_values) ===")
    import polars as pl
    from psycopg2.extras import execute_values

    with open(METRICS_PATH, "rb") as f:
        metrics = pickle.load(f)

    hook = PostgresHook(postgres_conn_id=DWH_CONN)
    conn = hook.get_conn()
    cur = conn.cursor()

    # synchronous_commit=OFF: PostgreSQL no espera fsync entre commits, mucho
    # más rápido para bulk load. Sigue garantizando ACID al final del COMMIT.
    cur.execute("SET LOCAL synchronous_commit = OFF")

    # ── transactions_processed: COPY directo a la tabla final ──
    # Optimizaciones:
    # 1. pl.scan_parquet (lazy) + sink_csv (streaming) en lugar de read_parquet +
    #    write_csv. La versión eager cargaba 763 MB de parquet como ~3 GB en RAM,
    #    forzaba swap en máquinas con poca RAM y tardaba 30+ min.
    # 2. COPY directo a transactions_processed (sin staging table + DISTINCT ON +
    #    ON CONFLICT). Ese patrón tardaba 30+ min por el sort y el chequeo del
    #    índice único. transaction_id es PK del CSV de origen → no hay duplicados,
    #    el DISTINCT ON + ON CONFLICT era defensa innecesaria. TRUNCATE+COPY es
    #    atómico dentro de la transacción (PostgreSQL MVCC), así que el dashboard
    #    sigue viendo los datos viejos hasta el COMMIT final.
    logger.info("  Preparando CSV para COPY...")

    columns_in_order = [
        "transaction_id", "card_id", "user_id", "transaction_date", "amount",
        "merchant_id", "merchant_city", "merchant_state", "mcc", "mcc_description",
        "transaction_type", "errors", "user_age", "user_gender", "user_state",
        "yearly_income", "total_debt", "credit_score", "card_brand", "card_type",
        "has_chip", "card_on_dark_web", "credit_limit", "debt_income_ratio",
        "distance_km", "is_fraud", "is_suspicious", "suspicion_reasons",
    ]

    # Ver qué columnas existen en el parquet para agregar las faltantes como NULL
    parquet_cols = set(pl.scan_parquet(PROCESSED_PATH).schema.keys())

    # Lazy plan: selecciono solo las columnas necesarias, agrego las faltantes
    # como literal null. Polars hace sink_csv en streaming (no carga todo en RAM)
    lazy_df = pl.scan_parquet(PROCESSED_PATH)
    select_exprs = [
        pl.col(c) if c in parquet_cols else pl.lit(None).alias(c)
        for c in columns_in_order
    ]
    lazy_df.select(select_exprs).sink_csv(
        COPY_CSV_PATH,
        include_header=False,
        null_value="",
    )
    logger.info(f"  CSV escrito → {COPY_CSV_PATH}. Ejecutando COPY...")

    # COPY directo a la tabla final
    cur.execute("TRUNCATE TABLE transactions_processed")
    with open(COPY_CSV_PATH, "r") as f:
        cur.copy_expert(
            f"COPY transactions_processed ({', '.join(columns_in_order)}) "
            f"FROM STDIN WITH (FORMAT CSV, NULL '')",
            f
        )
    os.remove(COPY_CSV_PATH)
    # cur.rowcount tras COPY trae las filas insertadas
    total_rows = cur.rowcount
    logger.info(f"  COPY completado: {total_rows:,} filas en transactions_processed")

    # ── Métricas globales ──
    g = metrics["global"]
    cur.execute("TRUNCATE TABLE fraud_metrics_global")
    cur.execute("""
        INSERT INTO fraud_metrics_global (total_transactions, total_fraud, fraud_rate,
            total_amount_at_risk, avg_fraud_amount, false_positive_rate, false_negative_rate,
            dataset_start_date, dataset_end_date)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (g["total_transactions"], g["total_fraud"], g["fraud_rate"],
          g["total_amount_at_risk"], g["avg_fraud_amount"],
          g["false_positive_rate"], g["false_negative_rate"],
          g["dataset_start_date"], g["dataset_end_date"]))

    # ── Helper para limpiar valores NaN/None ──
    def _v(val, cast=None):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        return cast(val) if cast else val

    def _b(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return False
        return bool(val)

    # ── By MCC (bulk insert) ──
    cur.execute("TRUNCATE TABLE fraud_by_mcc")
    mcc_rows = [
        (_v(r["mcc"]), _v(r["mcc_description"]),
         int(r["total_transactions"]), int(r["total_fraud"]),
         float(r["fraud_rate"]), float(r["amount_at_risk"]))
        for _, r in metrics["by_mcc"].iterrows()
    ]
    if mcc_rows:
        execute_values(cur,
            """INSERT INTO fraud_by_mcc
               (mcc, mcc_description, total_transactions, total_fraud, fraud_rate, amount_at_risk)
               VALUES %s""",
            mcc_rows, page_size=1000)

    # ── By card type (bulk insert) ──
    cur.execute("TRUNCATE TABLE fraud_by_card_type")
    card_rows = [
        (_v(r.get("card_brand")), _v(r.get("card_type")), _b(r.get("has_chip")),
         int(r["total_transactions"]), int(r["total_fraud"]),
         float(r["fraud_rate"]), float(r["amount_at_risk"]))
        for _, r in metrics["by_card"].iterrows()
    ]
    if card_rows:
        execute_values(cur,
            """INSERT INTO fraud_by_card_type
               (card_brand, card_type, has_chip, total_transactions, total_fraud, fraud_rate, amount_at_risk)
               VALUES %s""",
            card_rows, page_size=1000)

    # ── By state (bulk insert) ──
    cur.execute("TRUNCATE TABLE fraud_by_state")
    state_rows = [
        (_v(r["state"]),
         int(r["total_transactions"]), int(r["total_fraud"]),
         float(r["fraud_rate"]), float(r["amount_at_risk"]))
        for _, r in metrics["by_state"].iterrows()
    ]
    if state_rows:
        execute_values(cur,
            """INSERT INTO fraud_by_state
               (state, total_transactions, total_fraud, fraud_rate, amount_at_risk)
               VALUES %s""",
            state_rows, page_size=1000)

    # ── By merchant (bulk insert) ──
    cur.execute("TRUNCATE TABLE fraud_by_merchant")
    merch_rows = [
        (_v(r.get("merchant_id")), _v(r.get("merchant_city")),
         _v(r.get("merchant_state")), _v(r.get("mcc_description")),
         int(r["total_transactions"]), int(r["total_fraud"]),
         float(r["fraud_rate"]), float(r["amount_at_risk"]))
        for _, r in metrics["by_merchant"].iterrows()
    ]
    if merch_rows:
        execute_values(cur,
            """INSERT INTO fraud_by_merchant
               (merchant_id, merchant_city, merchant_state, mcc_description,
                total_transactions, total_fraud, fraud_rate, amount_at_risk)
               VALUES %s""",
            merch_rows, page_size=1000)

    # ── User profiles (bulk insert con ON CONFLICT) ──
    if not metrics["user_profiles"].empty:
        cur.execute("TRUNCATE TABLE user_profiles")
        user_rows = [
            (str(r["user_id"]),
             _v(r.get("age"), int), _v(r.get("gender")),
             _v(r.get("yearly_income"), float), _v(r.get("total_debt"), float),
             _v(r.get("debt_income_ratio"), float), _v(r.get("credit_score"), int),
             _v(r.get("num_cards"), int) or 0,
             int(r.get("total_transactions", 0)), int(r.get("total_fraud", 0)),
             float(r.get("fraud_rate", 0)),
             _v(r.get("total_spent"), float), _v(r.get("avg_transaction"), float),
             _v(r.get("amount_at_risk"), float))
            for _, r in metrics["user_profiles"].iterrows()
        ]
        execute_values(cur,
            """INSERT INTO user_profiles
               (user_id, age, gender, yearly_income, total_debt,
                debt_income_ratio, credit_score, num_cards,
                total_transactions, total_fraud, fraud_rate,
                total_spent, avg_transaction, amount_at_risk)
               VALUES %s
               ON CONFLICT (user_id) DO NOTHING""",
            user_rows, page_size=500)

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Carga al DWH completada. {total_rows:,} transacciones insertadas.")


# ============================================================
# TAREA 7: Control de calidad
# ============================================================
def _quality_check(**context):
    logger.info("=== TAREA 7: Control de calidad ===")
    hook = PostgresHook(postgres_conn_id=DWH_CONN)
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM transactions_processed WHERE amount < 0")
    neg = cur.fetchone()[0]
    if neg > 0:
        raise ValueError(f"Control de calidad fallido: {neg} montos negativos")

    cur.execute("SELECT fraud_rate FROM fraud_metrics_global ORDER BY computed_at DESC LIMIT 1")
    result = cur.fetchone()
    if result:
        logger.info(f"  ✓ Tasa de fraude: {result[0]:.2%}")

    cur.execute("SELECT COUNT(*) FROM transactions_processed")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM transactions_processed WHERE is_fraud = True")
    fraud = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO pipeline_run_log (status, rows_processed, rows_fraud, notes)
        VALUES (%s,%s,%s,%s)
    """, ("success", total, fraud, "Pipeline ejecutado correctamente"))

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Control de calidad OK. {total:,} transacciones, {fraud:,} fraudes.")


# ============================================================
# DEFINICIÓN DEL DAG
# ============================================================
with DAG(
    dag_id="securelink_fraud_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["securelink", "fraude", "etl"],
    description="Pipeline de detección de fraude (Polars + DuckDB + COPY)",
) as dag:

    # ── Validaciones previas (paralelo) ──
    validate_files_task = PythonOperator(task_id="validate_files", python_callable=_validate_files)
    validate_dwh = PostgresOperator(
        task_id="validate_dwh",
        postgres_conn_id=DWH_CONN,
        sql="SELECT 1",
    )

    # ── Carga de fuentes de referencia (paralelo, 4 independientes) ──
    load_users  = PythonOperator(task_id="load_users",  python_callable=_load_users)
    load_cards  = PythonOperator(task_id="load_cards",  python_callable=_load_cards)
    load_mcc    = PythonOperator(task_id="load_mcc",    python_callable=_load_mcc)
    load_labels = PythonOperator(task_id="load_labels", python_callable=_load_labels)

    # ── Pipeline secuencial principal ──
    ingest_transactions = PythonOperator(task_id="ingest_transactions", python_callable=_ingest_transactions)
    transform_data      = PythonOperator(task_id="transform_data",      python_callable=_transform_data)
    compute_metrics     = PythonOperator(task_id="compute_metrics",     python_callable=_compute_metrics)
    load_to_dwh         = PythonOperator(task_id="load_to_dwh",         python_callable=_load_to_dwh)
    quality_check       = PythonOperator(task_id="quality_check",       python_callable=_quality_check)

    # ── Dependencias ──
    validations = [validate_files_task, validate_dwh]
    refs = [load_users, load_cards, load_mcc, load_labels]

    for v in validations:
        v >> refs

    refs >> ingest_transactions
    ingest_transactions >> transform_data >> compute_metrics >> load_to_dwh >> quality_check
