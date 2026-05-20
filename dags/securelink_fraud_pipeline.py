"""
SecureLink - Pipeline de Detección de Fraude
=============================================
Versión corregida con los separadores y nombres de columnas reales del dataset.
"""

import logging
import os
import json
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
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

    # Verificar que existen
    for filepath in [TRANSACTIONS_FILE, USERS_FILE, CARDS_FILE, LABELS_FILE, MCC_FILE]:
        if not os.path.exists(filepath):
            raise FileNotFoundError(
                f"Archivo no encontrado: {filepath}\n"
                f"Copiá tus archivos a la carpeta ./data/ del proyecto."
            )
        logger.info(f"  ✓ {os.path.basename(filepath)} existe")

    # Verificar columnas mínimas (usando el separador correcto de cada archivo)
    txn = pd.read_csv(TRANSACTIONS_FILE, nrows=1, sep=',')
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
# TAREAS 2a–2d: Cargar fuentes de referencia (corren en PARALELO)
# ============================================================
# Cada tarea preprocesa una fuente y la guarda a /tmp para que la consuma
# ingest_transactions. Permite mejor visibilidad en el grafo de Airflow.

REF_USERS_PATH  = "/tmp/securelink_ref_users.parquet"
REF_CARDS_PATH  = "/tmp/securelink_ref_cards.parquet"
REF_MCC_PATH    = "/tmp/securelink_ref_mcc.pkl"
REF_LABELS_PATH = "/tmp/securelink_ref_labels.pkl"


def _load_users(**context):
    logger.info("=== TAREA 2a: Leyendo users_data.csv ===")
    users = pd.read_csv(USERS_FILE, sep=None, engine='python')
    users = users.rename(columns={"id": "user_id"})
    cols_users = [c for c in ["user_id", "current_age", "gender", "latitude",
                              "longitude", "yearly_income", "total_debt", "credit_score"]
                  if c in users.columns]
    users[cols_users].to_parquet(REF_USERS_PATH, index=False)
    logger.info(f"  Usuarios: {len(users):,} filas → {REF_USERS_PATH}")


def _load_cards(**context):
    logger.info("=== TAREA 2b: Leyendo cards_data.csv ===")
    cards = pd.read_csv(CARDS_FILE, sep=None, engine='python')
    cards = cards.rename(columns={"id": "card_id"})
    cols_cards = [c for c in ["card_id", "card_brand", "card_type", "has_chip",
                              "credit_limit", "card_on_dark_web", "client_id"]
                  if c in cards.columns]
    cards[cols_cards].to_parquet(REF_CARDS_PATH, index=False)
    logger.info(f"  Tarjetas: {len(cards):,} filas → {REF_CARDS_PATH}")


def _load_mcc(**context):
    logger.info("=== TAREA 2c: Leyendo mcc_codes.json ===")
    import pickle
    with open(MCC_FILE, "r") as f:
        mcc_raw = json.load(f)
    mcc_lookup = {str(k): v for k, v in mcc_raw.items()}
    with open(REF_MCC_PATH, "wb") as f:
        pickle.dump(mcc_lookup, f)
    logger.info(f"  MCC: {len(mcc_lookup):,} categorías → {REF_MCC_PATH}")


def _load_labels(**context):
    logger.info("=== TAREA 2d: Leyendo train_fraud_labels.json ===")
    import pickle
    # JSON anidado {"target": {"txn_id": "Yes"/"No", ...}}
    with open(LABELS_FILE, "r") as f:
        labels_raw = json.load(f)
    labels_inner = labels_raw.get("target", labels_raw)
    labels_dict = {str(k): (v == "Yes") for k, v in labels_inner.items()}
    with open(REF_LABELS_PATH, "wb") as f:
        pickle.dump(labels_dict, f)
    logger.info(f"  Labels: {len(labels_dict):,} registros → {REF_LABELS_PATH}")


# ============================================================
# TAREA 3: Ingestar transacciones y enriquecer
# ============================================================
def _ingest_transactions(**context):
    """Lee transactions_data.csv en chunks y enriquece con las 4 fuentes de referencia
    pre-cargadas en paralelo por las tareas 2a–2d. Escribe parquet de forma incremental
    para no cargar 13M filas en memoria."""
    logger.info("=== TAREA 3: Ingesta y unión de transacciones ===")
    import pickle
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Cargar referencias preprocesadas por las tareas paralelas
    users = pd.read_parquet(REF_USERS_PATH)
    cards = pd.read_parquet(REF_CARDS_PATH)
    with open(REF_MCC_PATH, "rb") as f:
        mcc_lookup = pickle.load(f)
    with open(REF_LABELS_PATH, "rb") as f:
        labels_dict = pickle.load(f)
    logger.info(f"  Referencias cargadas: users={len(users):,}, cards={len(cards):,}, "
                f"mcc={len(mcc_lookup):,}, labels={len(labels_dict):,}")

    cols_cards = list(cards.columns)
    cols_users = list(users.columns)

    output_path = "/tmp/securelink_merged.parquet"
    writer = None
    total_rows = 0

    for i, chunk in enumerate(pd.read_csv(TRANSACTIONS_FILE, chunksize=100_000, sep=',')):
        chunk = chunk.rename(columns={"id": "transaction_id"})
        chunk["transaction_id"] = chunk["transaction_id"].astype(str)

        # Labels via dict lookup O(1)
        chunk["is_fraud"] = chunk["transaction_id"].map(labels_dict).fillna(False)

        # Merge con cards (6k filas — liviano)
        chunk = chunk.merge(cards[cols_cards], on="card_id", how="left")
        if "client_id_x" in chunk.columns:
            chunk = chunk.rename(columns={"client_id_x": "client_id"})
            if "client_id_y" in chunk.columns:
                chunk = chunk.drop(columns=["client_id_y"])

        # Merge con users (2k filas — liviano)
        left_key = "client_id" if "client_id" in chunk.columns else "user_id"
        chunk = chunk.merge(users[cols_users], left_on=left_key, right_on="user_id", how="left")

        # MCC via dict lookup
        chunk["mcc"] = chunk["mcc"].astype(str)
        chunk["mcc_description"] = chunk["mcc"].map(mcc_lookup).fillna("Unknown")

        # Escribir chunk al parquet de forma incremental
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(output_path, table.schema)
        writer.write_table(table)

        total_rows += len(chunk)
        logger.info(f"  Chunk {i+1}: {len(chunk):,} filas procesadas")

    if writer:
        writer.close()

    logger.info(f"Dataset final: {total_rows:,} filas → {output_path}")
    return output_path


# ============================================================
# TAREA 3: Limpiar datos
# ============================================================
def _clean_data(**context):
    logger.info("=== TAREA 3: Limpieza de datos ===")
    import pyarrow as pa
    import pyarrow.parquet as pq

    pf = pq.ParquetFile("/tmp/securelink_merged.parquet")
    writer = None
    rows_in, rows_out = 0, 0

    for batch in pf.iter_batches(batch_size=200_000):
        df = batch.to_pandas()
        rows_in += len(df)

        df["transaction_date"] = pd.to_datetime(df["date"], errors="coerce")
        df["amount"] = pd.to_numeric(
            df["amount"].astype(str)
                .str.replace("$", "", regex=False)
                .str.replace(",", "", regex=False)
                .str.strip(),
            errors="coerce"
        )
        df = df[(df["amount"] >= 0) & df["amount"].notna() & df["transaction_date"].notna()]
        df = df.drop_duplicates(subset="transaction_id")

        for flag_col in ["has_chip", "card_on_dark_web"]:
            if flag_col in df.columns:
                df[flag_col] = df[flag_col].astype(str).str.upper().isin(["YES", "TRUE", "1"])

        df["is_online"] = df["use_chip"].astype(str).str.strip().str.lower() == "online transaction"

        for col in ["credit_limit", "yearly_income", "total_debt", "per_capita_income"]:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str)
                        .str.replace("$", "", regex=False)
                        .str.replace(",", "", regex=False)
                        .str.strip(),
                    errors="coerce"
                )

        rows_out += len(df)
        table = pa.Table.from_pandas(df, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter("/tmp/securelink_clean.parquet", table.schema)
        writer.write_table(table)

    if writer:
        writer.close()
    logger.info(f"  Filas: {rows_in:,} → {rows_out:,}. Limpieza completada.")


# ============================================================
# TAREA 4: Construir indicadores derivados
# ============================================================
def _build_features(**context):
    logger.info("=== TAREA 4: Construcción de indicadores derivados ===")
    import pyarrow as pa
    import pyarrow.parquet as pq

    pf = pq.ParquetFile("/tmp/securelink_clean.parquet")
    writer = None

    for batch in pf.iter_batches(batch_size=200_000):
        df = batch.to_pandas()

        if "total_debt" in df.columns and "yearly_income" in df.columns:
            df["yearly_income"] = pd.to_numeric(df["yearly_income"], errors="coerce")
            df["total_debt"] = pd.to_numeric(df["total_debt"], errors="coerce")
            df["debt_income_ratio"] = np.where(
                df["yearly_income"] > 0,
                df["total_debt"] / df["yearly_income"],
                np.nan
            )
        else:
            df["debt_income_ratio"] = np.nan

        df["distance_km"] = np.nan
        df["transaction_type"] = np.where(df["is_online"], "Online", "Swipe")

        table = pa.Table.from_pandas(df, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter("/tmp/securelink_features.parquet", table.schema)
        writer.write_table(table)

    if writer:
        writer.close()
    logger.info("Indicadores derivados calculados.")


# ============================================================
# TAREA 5: Aplicar reglas de detección
# ============================================================
def _apply_fraud_rules(**context):
    logger.info("=== TAREA 5: Aplicando reglas de detección ===")
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.compute as pc

    # Primer paso: calcular p99 leyendo solo la columna amount (sin cargar todo en RAM)
    pf = pq.ParquetFile("/tmp/securelink_features.parquet")
    amounts = pf.read(columns=["amount"]).column("amount")
    amount_threshold = float(pc.quantile(amounts, q=0.99)[0].as_py())
    logger.info(f"  Umbral de monto alto (p99): ${amount_threshold:,.2f}")
    del amounts

    # Segundo paso: aplicar reglas por chunks
    writer = None
    total_suspicious = 0
    total_fraud = 0

    for batch in pf.iter_batches(batch_size=200_000):
        df = batch.to_pandas()

        reasons = pd.Series([""] * len(df), index=df.index)

        r1 = df["amount"] > amount_threshold
        reasons[r1] += "monto_atipico;"

        r2 = (df["errors"].notna() &
              (df["errors"].astype(str).str.strip() != "") &
              (df["errors"].astype(str).str.strip() != "nan"))
        reasons[r2] += "error_en_transaccion;"

        r3 = pd.Series(False, index=df.index)
        if "card_on_dark_web" in df.columns:
            r3 = df["card_on_dark_web"] == True
            reasons[r3] += "tarjeta_en_dark_web;"

        r4 = df["debt_income_ratio"] > 3
        reasons[r4] += "alto_endeudamiento;"

        df["is_suspicious"] = r1 | r2 | r3 | r4
        df["suspicion_reasons"] = reasons.str.rstrip(";")

        total_suspicious += int(df["is_suspicious"].sum())
        total_fraud += int(df["is_fraud"].sum())

        table = pa.Table.from_pandas(df, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter("/tmp/securelink_rules.parquet", table.schema)
        writer.write_table(table)

    if writer:
        writer.close()
    logger.info(f"  Total sospechosas: {total_suspicious:,}")
    logger.info(f"  Total fraudes reales: {total_fraud:,}")
    logger.info("Reglas aplicadas.")


# ============================================================
# TAREA 6: Calcular métricas
# ============================================================
def _compute_metrics(**context):
    logger.info("=== TAREA 6: Calculando métricas ===")
    import pickle
    import pyarrow.parquet as pq
    from collections import defaultdict

    pf = pq.ParquetFile("/tmp/securelink_rules.parquet")
    available = pf.schema_arrow.names

    card_grp_cols  = [c for c in ["card_brand", "card_type", "has_chip"] if c in available]
    merch_grp_cols = [c for c in ["merchant_id", "merchant_city", "merchant_state", "mcc_description"] if c in available]
    user_col = "user_id" if "user_id" in available else ("client_id" if "client_id" in available else None)

    # Acumuladores globales
    g_total = g_fraud = g_fp = g_fn = g_legit = 0
    g_fraud_amount = 0.0
    g_min_date = g_max_date = None

    # Acumuladores dimensionales: key → [total, fraud_count, amount_at_risk]
    mcc_acc   = defaultdict(lambda: [0, 0, 0.0])
    card_acc  = defaultdict(lambda: [0, 0, 0.0])
    state_acc = defaultdict(lambda: [0, 0, 0.0])
    merch_acc = defaultdict(lambda: [0, 0, 0.0])
    user_acc  = {}

    for batch in pf.iter_batches(batch_size=200_000):
        df = batch.to_pandas()
        fm = df["is_fraud"] == True

        g_total        += len(df)
        g_fraud        += int(fm.sum())
        g_fraud_amount += float(df.loc[fm, "amount"].sum())
        g_fp           += int((df["is_suspicious"] & ~fm).sum())
        g_fn           += int((~df["is_suspicious"] & fm).sum())
        g_legit        += int((~fm).sum())

        if df["transaction_date"].notna().any():
            mn = df["transaction_date"].dropna().min()
            mx = df["transaction_date"].dropna().max()
            g_min_date = mn if g_min_date is None else min(g_min_date, mn)
            g_max_date = mx if g_max_date is None else max(g_max_date, mx)

        for (mcc, desc), grp in df.groupby(["mcc", "mcc_description"]):
            k = (str(mcc), str(desc))
            mcc_acc[k][0] += len(grp)
            mcc_acc[k][1] += int(grp["is_fraud"].sum())
            mcc_acc[k][2] += float(grp.loc[grp["is_fraud"], "amount"].sum())

        if card_grp_cols:
            for key, grp in df.groupby(card_grp_cols, dropna=False):
                k = key if isinstance(key, tuple) else (key,)
                card_acc[k][0] += len(grp)
                card_acc[k][1] += int(grp["is_fraud"].sum())
                card_acc[k][2] += float(grp.loc[grp["is_fraud"], "amount"].sum())

        if "merchant_state" in df.columns:
            df_phys = df[~df["is_online"] & df["merchant_state"].notna()]
            for state, grp in df_phys.groupby("merchant_state"):
                state_acc[str(state)][0] += len(grp)
                state_acc[str(state)][1] += int(grp["is_fraud"].sum())
                state_acc[str(state)][2] += float(grp.loc[grp["is_fraud"], "amount"].sum())

        if merch_grp_cols:
            df_m = df.copy()
            for col in merch_grp_cols:
                df_m[col] = df_m[col].fillna("").astype(str)
            for key, grp in df_m.groupby(merch_grp_cols):
                k = key if isinstance(key, tuple) else (key,)
                merch_acc[k][0] += len(grp)
                merch_acc[k][1] += int(grp["is_fraud"].sum())
                merch_acc[k][2] += float(grp.loc[grp["is_fraud"], "amount"].sum())

        if user_col:
            for uid, grp in df.groupby(user_col, dropna=True):
                uid = str(uid)
                if uid not in user_acc:
                    user_acc[uid] = {
                        "total": 0, "fraud": 0, "spent": 0.0, "fraud_amount": 0.0,
                        "cards": set(),
                        "age": None, "gender": None, "income": None,
                        "debt": None, "dir": None, "score": None,
                    }
                    for src, dst in [("current_age","age"), ("gender","gender"),
                                     ("yearly_income","income"), ("total_debt","debt"),
                                     ("debt_income_ratio","dir"), ("credit_score","score")]:
                        if src in grp.columns and grp[src].notna().any():
                            user_acc[uid][dst] = grp[src].dropna().iloc[0]
                user_acc[uid]["total"] += len(grp)
                user_acc[uid]["fraud"] += int(grp["is_fraud"].sum())
                user_acc[uid]["spent"] += float(grp["amount"].sum())
                user_acc[uid]["fraud_amount"] += float(grp.loc[grp["is_fraud"], "amount"].sum())
                if "card_id" in grp.columns:
                    user_acc[uid]["cards"].update(grp["card_id"].dropna().astype(str).unique())

    # Construir resultados
    metrics_global = {
        "total_transactions": g_total,
        "total_fraud": g_fraud,
        "fraud_rate": g_fraud / g_total if g_total > 0 else 0.0,
        "total_amount_at_risk": g_fraud_amount,
        "avg_fraud_amount": g_fraud_amount / g_fraud if g_fraud > 0 else 0.0,
        "false_positive_rate": g_fp / g_legit if g_legit > 0 else 0.0,
        "false_negative_rate": g_fn / g_fraud if g_fraud > 0 else 0.0,
        "dataset_start_date": g_min_date.date() if g_min_date is not None else None,
        "dataset_end_date": g_max_date.date() if g_max_date is not None else None,
    }
    logger.info(f"  Tasa de fraude: {metrics_global['fraud_rate']:.2%}")
    logger.info(f"  Monto en riesgo: ${g_fraud_amount:,.2f}")

    fraud_mcc = pd.DataFrame([
        {"mcc": k[0], "mcc_description": k[1], "total_transactions": v[0],
         "total_fraud": v[1], "fraud_rate": v[1]/v[0] if v[0] > 0 else 0.0, "amount_at_risk": v[2]}
        for k, v in mcc_acc.items()
    ]) if mcc_acc else pd.DataFrame(columns=["mcc","mcc_description","total_transactions","total_fraud","fraud_rate","amount_at_risk"])

    fraud_card_rows = []
    for k, v in card_acc.items():
        row = dict(zip(card_grp_cols, k if isinstance(k, tuple) else (k,)))
        row.update({"total_transactions": v[0], "total_fraud": v[1],
                    "fraud_rate": v[1]/v[0] if v[0] > 0 else 0.0, "amount_at_risk": v[2]})
        fraud_card_rows.append(row)
    fraud_card = pd.DataFrame(fraud_card_rows) if fraud_card_rows else pd.DataFrame(
        columns=["card_brand","card_type","has_chip","total_transactions","total_fraud","fraud_rate","amount_at_risk"])
    for col in ["card_brand", "card_type", "has_chip"]:
        if col not in fraud_card.columns:
            fraud_card[col] = None

    fraud_state = pd.DataFrame([
        {"state": k, "total_transactions": v[0], "total_fraud": v[1],
         "fraud_rate": v[1]/v[0] if v[0] > 0 else 0.0, "amount_at_risk": v[2]}
        for k, v in state_acc.items()
    ]) if state_acc else pd.DataFrame(columns=["state","total_transactions","total_fraud","fraud_rate","amount_at_risk"])

    fraud_merchant_rows = []
    for k, v in merch_acc.items():
        row = dict(zip(merch_grp_cols, k if isinstance(k, tuple) else (k,)))
        row.update({"total_transactions": v[0], "total_fraud": v[1],
                    "fraud_rate": v[1]/v[0] if v[0] > 0 else 0.0, "amount_at_risk": v[2]})
        fraud_merchant_rows.append(row)
    fraud_merchant = pd.DataFrame(fraud_merchant_rows) if fraud_merchant_rows else pd.DataFrame()
    for col in ["merchant_id", "merchant_city", "merchant_state", "mcc_description"]:
        if col not in fraud_merchant.columns:
            fraud_merchant[col] = ""

    if user_acc:
        user_rows = []
        for uid, u in user_acc.items():
            fr = u["fraud"] / u["total"] if u["total"] > 0 else 0.0
            user_rows.append({
                "user_id": uid, "age": u["age"], "gender": u["gender"],
                "yearly_income": u["income"], "total_debt": u["debt"],
                "debt_income_ratio": u["dir"], "credit_score": u["score"],
                "num_cards": len(u["cards"]),
                "total_transactions": u["total"], "total_fraud": u["fraud"],
                "fraud_rate": fr, "total_spent": u["spent"],
                "avg_transaction": u["spent"] / u["total"] if u["total"] > 0 else 0.0,
                "amount_at_risk": u["fraud_amount"],
            })
        user_profiles = pd.DataFrame(user_rows)
    else:
        user_profiles = pd.DataFrame()

    with open("/tmp/securelink_metrics.pkl", "wb") as f:
        pickle.dump({
            "global": metrics_global, "by_mcc": fraud_mcc, "by_card": fraud_card,
            "by_state": fraud_state, "by_merchant": fraud_merchant,
            "user_profiles": user_profiles,
        }, f)

    logger.info("Métricas calculadas.")


# ============================================================
# TAREA 7: Cargar al DWH
# ============================================================
def _load_to_dwh(**context):
    logger.info("=== TAREA 7: Cargando al DWH ===")
    import pickle
    import pyarrow.parquet as pq

    with open("/tmp/securelink_metrics.pkl", "rb") as f:
        metrics = pickle.load(f)

    hook = PostgresHook(postgres_conn_id=DWH_CONN)
    conn = hook.get_conn()
    cur = conn.cursor()

    from psycopg2.extras import execute_values

    def safe(val, cast=None):
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return None
        return cast(val) if cast else val

    def safe_bool(val):
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return False
        return bool(val)

    # Optimización de bulk load: desactivamos commit síncrono al WAL para esta sesión.
    # PostgreSQL aún garantiza durabilidad ACID, solo deja de bloquear esperando fsync
    # tras cada commit. Para un pipeline reejecutable es seguro.
    cur.execute("SET LOCAL synchronous_commit = OFF")

    # Cargar transacciones: patrón "atomic full refresh" — TRUNCATE + INSERT ON CONFLICT DO NOTHING
    # dentro de la misma transacción de psycopg2. PostgreSQL MVCC garantiza que el dashboard
    # siga viendo los datos viejos hasta el COMMIT final. Es idempotente (mismo resultado
    # al reejecutar) y ~10x más rápido que ON CONFLICT DO UPDATE para datasets grandes.
    logger.info("  Cargando transacciones procesadas (atomic refresh)...")
    cur.execute("TRUNCATE TABLE transactions_processed")
    pf = pq.ParquetFile("/tmp/securelink_rules.parquet")
    total_inserted = 0

    insert_sql = """
        INSERT INTO transactions_processed VALUES %s
        ON CONFLICT (transaction_id) DO NOTHING
    """

    for arrow_batch in pf.iter_batches(batch_size=50_000):
        df = arrow_batch.to_pandas()
        rows = [
            (
                str(t.transaction_id or ""),
                safe(t.card_id), safe(t.user_id),
                safe(t.transaction_date),
                safe(t.amount, float),
                safe(t.merchant_id), safe(t.merchant_city), safe(t.merchant_state),
                safe(t.mcc), safe(t.mcc_description), safe(t.transaction_type), safe(t.errors),
                safe(t.current_age, int), safe(t.gender),
                None,
                safe(t.yearly_income, float), safe(t.total_debt, float), safe(t.credit_score, int),
                safe(t.card_brand), safe(t.card_type),
                safe_bool(t.has_chip), safe_bool(t.card_on_dark_web),
                safe(t.credit_limit, float), safe(t.debt_income_ratio, float), safe(t.distance_km, float),
                safe_bool(t.is_fraud), safe_bool(t.is_suspicious),
                safe(t.suspicion_reasons),
            )
            for t in df.itertuples(index=False)
        ]
        execute_values(cur, insert_sql, rows, page_size=5_000)
        total_inserted += len(rows)
        logger.info(f"    Insertadas {total_inserted:,} filas")

    # ─────────────────────────────────────────────────────────────────────────
    # Tablas agregadas: patrón "atomic refresh".
    # Las TRUNCATE + INSERT de abajo se ejecutan dentro de la misma transacción
    # de psycopg2 (autocommit=False por defecto). PostgreSQL MVCC garantiza que
    # los lectores del dashboard sigan viendo los datos viejos hasta el COMMIT
    # final — nunca ven una tabla vacía.
    # Las tablas agregadas no tienen unique constraints en la clave de negocio,
    # así que UPSERT no aplica acá; el refresh atómico es el patrón correcto.
    # ─────────────────────────────────────────────────────────────────────────

    # Métricas globales
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

    # Por MCC
    cur.execute("TRUNCATE TABLE fraud_by_mcc")
    for _, row in metrics["by_mcc"].iterrows():
        cur.execute("""
            INSERT INTO fraud_by_mcc (mcc, mcc_description, total_transactions, total_fraud, fraud_rate, amount_at_risk)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (str(row["mcc"]), str(row["mcc_description"]),
              int(row["total_transactions"]), int(row["total_fraud"]),
              float(row["fraud_rate"]), float(row["amount_at_risk"])))

    # Por tarjeta
    cur.execute("TRUNCATE TABLE fraud_by_card_type")
    for _, row in metrics["by_card"].iterrows():
        cur.execute("""
            INSERT INTO fraud_by_card_type (card_brand, card_type, has_chip, total_transactions, total_fraud, fraud_rate, amount_at_risk)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (safe(row.get("card_brand")), safe(row.get("card_type")),
              bool(row.get("has_chip", False)),
              int(row["total_transactions"]), int(row["total_fraud"]),
              float(row["fraud_rate"]), float(row["amount_at_risk"])))

    # Por estado
    cur.execute("TRUNCATE TABLE fraud_by_state")
    for _, row in metrics["by_state"].iterrows():
        cur.execute("""
            INSERT INTO fraud_by_state (state, total_transactions, total_fraud, fraud_rate, amount_at_risk)
            VALUES (%s,%s,%s,%s,%s)
        """, (str(row["state"]), int(row["total_transactions"]),
              int(row["total_fraud"]), float(row["fraud_rate"]), float(row["amount_at_risk"])))

    # Por merchant
    cur.execute("TRUNCATE TABLE fraud_by_merchant")
    for _, row in metrics["by_merchant"].iterrows():
        cur.execute("""
            INSERT INTO fraud_by_merchant (merchant_id, merchant_city, merchant_state, mcc_description,
                total_transactions, total_fraud, fraud_rate, amount_at_risk)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (safe(row.get("merchant_id")), safe(row.get("merchant_city")),
              safe(row.get("merchant_state")), safe(row.get("mcc_description")),
              int(row["total_transactions"]), int(row["total_fraud"]),
              float(row["fraud_rate"]), float(row["amount_at_risk"])))

    # Perfiles de usuario (faltaba en la versión anterior)
    if not metrics["user_profiles"].empty:
        cur.execute("TRUNCATE TABLE user_profiles")
        for _, row in metrics["user_profiles"].iterrows():
            cur.execute("""
                INSERT INTO user_profiles (user_id, age, gender, yearly_income, total_debt,
                    debt_income_ratio, credit_score, num_cards, total_transactions, total_fraud,
                    fraud_rate, total_spent, avg_transaction, amount_at_risk)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (user_id) DO NOTHING
            """, (
                str(row["user_id"]),
                safe(row.get("age"), int), safe(row.get("gender")),
                safe(row.get("yearly_income"), float), safe(row.get("total_debt"), float),
                safe(row.get("debt_income_ratio"), float), safe(row.get("credit_score"), int),
                safe(row.get("num_cards"), int),
                int(row.get("total_transactions", 0)), int(row.get("total_fraud", 0)),
                float(row.get("fraud_rate", 0)),
                safe(row.get("total_spent"), float), safe(row.get("avg_transaction"), float),
                safe(row.get("amount_at_risk"), float),
            ))

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Carga al DWH completada. {total_inserted:,} transacciones insertadas.")


# ============================================================
# TAREA 8: Control de calidad
# ============================================================
def _quality_check(**context):
    logger.info("=== TAREA 8: Control de calidad ===")
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
    # Batch analítico diario. catchup=False evita ejecutar runs históricos al activarlo.
    # El DAG arranca pausado (AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True).
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["securelink", "fraude", "etl"],
    description="Pipeline de detección de fraude en transacciones financieras (batch diario)",
) as dag:

    # ── Validaciones previas (paralelo) ──
    validate_files_task = PythonOperator(task_id="validate_files", python_callable=_validate_files)
    validate_dwh = PostgresOperator(
        task_id="validate_dwh",
        postgres_conn_id=DWH_CONN,
        sql="SELECT 1",
    )

    # ── Carga de datos de referencia (paralelo, 4 fuentes independientes) ──
    load_users  = PythonOperator(task_id="load_users",  python_callable=_load_users)
    load_cards  = PythonOperator(task_id="load_cards",  python_callable=_load_cards)
    load_mcc    = PythonOperator(task_id="load_mcc",    python_callable=_load_mcc)
    load_labels = PythonOperator(task_id="load_labels", python_callable=_load_labels)

    # ── Pipeline secuencial principal ──
    ingest_transactions = PythonOperator(task_id="ingest_transactions", python_callable=_ingest_transactions)
    clean_data         = PythonOperator(task_id="clean_data",        python_callable=_clean_data)
    build_features     = PythonOperator(task_id="build_features",    python_callable=_build_features)
    apply_fraud_rules  = PythonOperator(task_id="apply_fraud_rules", python_callable=_apply_fraud_rules)
    compute_metrics    = PythonOperator(task_id="compute_metrics",   python_callable=_compute_metrics)
    load_to_dwh        = PythonOperator(task_id="load_to_dwh",       python_callable=_load_to_dwh)
    quality_check      = PythonOperator(task_id="quality_check",     python_callable=_quality_check)

    # ── Dependencias ──
    # 1. Las dos validaciones corren en paralelo y ambas deben pasar.
    # 2. Pasadas las validaciones, las 4 cargas de referencia arrancan en paralelo.
    # 3. ingest_transactions espera a las 4 (necesita los .pkl/.parquet de cada una).
    # 4. El resto del pipeline es secuencial porque cada tarea depende del parquet anterior.
    validations = [validate_files_task, validate_dwh]
    refs = [load_users, load_cards, load_mcc, load_labels]

    # Cross-downstream: cada validación es upstream de cada carga de referencia
    # (Airflow no soporta list >> list directamente, hay que iterar).
    for v in validations:
        v >> refs

    refs >> ingest_transactions
    ingest_transactions >> clean_data >> build_features >> apply_fraud_rules >> compute_metrics >> load_to_dwh >> quality_check