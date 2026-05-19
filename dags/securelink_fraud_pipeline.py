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

logger = logging.getLogger(__name__)

DATA_DIR = "/opt/airflow/data"
TRANSACTIONS_FILE = os.path.join(DATA_DIR, "transactions_data.csv")
USERS_FILE        = os.path.join(DATA_DIR, "users_data.csv")
CARDS_FILE        = os.path.join(DATA_DIR, "cards_data.csv")
LABELS_FILE       = os.path.join(DATA_DIR, "train_fraud_labels.json")
MCC_FILE          = os.path.join(DATA_DIR, "mcc_codes.json")

DWH_CONN = "postgres_dwh"

default_args = {
    "retries": 1,
    "retry_delay": 30,
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
    txn = pd.read_csv(TRANSACTIONS_FILE, nrows=1, sep=';')
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
# TAREA 2: Ingestar y unir datos
# ============================================================
def _ingest_data(**context):
    logger.info("=== TAREA 2: Ingesta y unión de datos ===")

    # -- Leer users --
    logger.info("Leyendo users_data.csv...")
    users = pd.read_csv(USERS_FILE, sep=None, engine='python')
    users = users.rename(columns={"id": "user_id"})
    logger.info(f"  Usuarios: {len(users):,} filas, columnas: {list(users.columns)}")

    # -- Leer cards --
    logger.info("Leyendo cards_data.csv...")
    cards = pd.read_csv(CARDS_FILE, sep=None, engine='python')
    # cards tiene 'id' (card id) y 'client_id' (id del usuario dueño)
    cards = cards.rename(columns={"id": "card_id"})
    logger.info(f"  Tarjetas: {len(cards):,} filas, columnas: {list(cards.columns)}")

    # -- Leer MCC --
    logger.info("Leyendo mcc_codes.json...")
    with open(MCC_FILE, "r") as f:
        mcc_dict = json.load(f)
    mcc_df = pd.DataFrame([{"mcc": k, "mcc_description": v} for k, v in mcc_dict.items()])

    # -- Leer labels --
    logger.info("Leyendo train_fraud_labels.json...")
    with open(LABELS_FILE, "r") as f:
        labels_raw = json.load(f)
    labels_df = pd.DataFrame([
        {"transaction_id": str(k), "is_fraud": v == "Yes"}
        for k, v in labels_raw.items()
    ])
    logger.info(f"  Labels: {len(labels_df):,} registros")

    # -- Leer transactions en chunks --
    logger.info("Leyendo transactions_data.csv en chunks...")
    chunks = []
    chunk_size = 100_000
    for i, chunk in enumerate(pd.read_csv(TRANSACTIONS_FILE, chunksize=chunk_size, sep=';')):
        chunk = chunk.rename(columns={"id": "transaction_id"})
        chunk["transaction_id"] = chunk["transaction_id"].astype(str)
        chunks.append(chunk)
        logger.info(f"  Chunk {i+1}: {len(chunk):,} filas leídas")

    transactions = pd.concat(chunks, ignore_index=True)
    logger.info(f"Total transacciones leídas: {len(transactions):,}")
    logger.info(f"Columnas transactions: {list(transactions.columns)}")

    # -- JOIN 1: transactions + labels --
    logger.info("Uniendo con etiquetas de fraude...")
    df = transactions.merge(labels_df, on="transaction_id", how="left")
    df["is_fraud"] = df["is_fraud"].fillna(False)

    # -- JOIN 2: + cards (usando card_id) --
    logger.info("Uniendo con tarjetas...")
    cols_cards = ["card_id", "card_brand", "card_type", "has_chip", "credit_limit", "card_on_dark_web"]
    # Agregar client_id si existe en cards
    if "client_id" in cards.columns:
        cols_cards.append("client_id")
    cols_cards = [c for c in cols_cards if c in cards.columns]
    
    df = df.merge(cards[cols_cards], on="card_id", how="left")
    logger.info(f"  Post-merge cards: {len(df):,} filas")

    # -- JOIN 3: + users --
    logger.info("Uniendo con usuarios...")
    # Determinar la columna de unión con users
    # Si cards tiene client_id, usamos eso; si no, usamos client_id de transactions
    if "client_id_x" in df.columns:
        # Pandas renombró porque había client_id en ambos lados
        df = df.rename(columns={"client_id_x": "client_id"})
        if "client_id_y" in df.columns:
            df = df.drop(columns=["client_id_y"])

    cols_users = ["user_id", "current_age", "gender", "latitude", "longitude",
                  "yearly_income", "total_debt", "credit_score"]
    cols_users = [c for c in cols_users if c in users.columns]

    # La columna de unión del lado izquierdo puede ser client_id o user_id
    left_key = "client_id" if "client_id" in df.columns else "user_id"
    df = df.merge(users[cols_users], left_on=left_key, right_on="user_id", how="left")
    logger.info(f"  Post-merge users: {len(df):,} filas")

    # -- JOIN 4: + MCC --
    logger.info("Uniendo con descripciones MCC...")
    df["mcc"] = df["mcc"].astype(str)
    mcc_df["mcc"] = mcc_df["mcc"].astype(str)
    df = df.merge(mcc_df, on="mcc", how="left")
    df["mcc_description"] = df["mcc_description"].fillna("Unknown")

    logger.info(f"Dataset final: {len(df):,} filas, {len(df.columns)} columnas")

    output_path = "/tmp/securelink_merged.parquet"
    df.to_parquet(output_path, index=False)
    logger.info(f"Guardado en {output_path}")
    return output_path


# ============================================================
# TAREA 3: Limpiar datos
# ============================================================
def _clean_data(**context):
    logger.info("=== TAREA 3: Limpieza de datos ===")
    df = pd.read_parquet("/tmp/securelink_merged.parquet")
    rows_initial = len(df)
    logger.info(f"Columnas disponibles: {list(df.columns)}")

    # Convertir fecha
    df["transaction_date"] = pd.to_datetime(df["date"], errors="coerce")

    # Limpiar amount
    df["amount"] = (
        df["amount"].astype(str)
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
    )
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    # Descartar montos negativos
    neg_count = (df["amount"] < 0).sum()
    df = df[df["amount"] >= 0]
    logger.info(f"  Montos negativos descartados: {neg_count:,}")

    # Descartar duplicados
    dup_count = df.duplicated(subset="transaction_id").sum()
    df = df.drop_duplicates(subset="transaction_id")
    logger.info(f"  Duplicados eliminados: {dup_count:,}")

    # Descartar filas sin fecha o monto válido
    invalid = df["transaction_date"].isna() | df["amount"].isna()
    df = df[~invalid]
    logger.info(f"  Filas inválidas eliminadas: {invalid.sum():,}")

    # Normalizar booleanos
    if "has_chip" in df.columns:
        df["has_chip"] = df["has_chip"].astype(str).str.upper().isin(["YES", "TRUE", "1"])
    if "card_on_dark_web" in df.columns:
        df["card_on_dark_web"] = df["card_on_dark_web"].astype(str).str.upper().isin(["YES", "TRUE", "1"])

    # Identificar transacciones online
    df["is_online"] = (
        df["use_chip"].astype(str).str.strip().str.lower() == "online transaction"
    )

    logger.info(f"  Filas iniciales: {rows_initial:,} → finales: {len(df):,}")
    # Limpiar columnas monetarias que vienen con $ o comas
    for col in ["credit_limit", "yearly_income", "total_debt", "per_capita_income"]:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace("$", "", regex=False)
                .str.replace(",", "", regex=False)
                .str.strip()
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df.to_parquet("/tmp/securelink_clean.parquet", index=False)
    logger.info("Limpieza completada.")


# ============================================================
# TAREA 4: Construir indicadores derivados
# ============================================================
def _build_features(**context):
    logger.info("=== TAREA 4: Construcción de indicadores derivados ===")
    df = pd.read_parquet("/tmp/securelink_clean.parquet")

    # Ratio deuda / ingreso
    if "total_debt" in df.columns and "yearly_income" in df.columns:
        df["yearly_income"] = pd.to_numeric(df["yearly_income"], errors="coerce")
        df["total_debt"] = pd.to_numeric(df["total_debt"], errors="coerce")
        df["debt_income_ratio"] = np.where(
            df["yearly_income"] > 0,
            df["total_debt"] / df["yearly_income"],
            np.nan
        )
        logger.info("  ✓ debt_income_ratio calculado")
    else:
        df["debt_income_ratio"] = np.nan

    # Distancia (solo si hay coordenadas del merchant)
    df["distance_km"] = np.nan

    # Tipo de transacción
    df["transaction_type"] = np.where(df["is_online"], "Online", "Swipe")
    logger.info("  ✓ transaction_type calculado")

    df.to_parquet("/tmp/securelink_features.parquet", index=False)
    logger.info("Indicadores derivados calculados.")


# ============================================================
# TAREA 5: Aplicar reglas de detección
# ============================================================
def _apply_fraud_rules(**context):
    logger.info("=== TAREA 5: Aplicando reglas de detección ===")
    df = pd.read_parquet("/tmp/securelink_features.parquet")

    amount_threshold = df["amount"].quantile(0.99)
    logger.info(f"  Umbral de monto alto (p99): ${amount_threshold:,.2f}")

    reasons = pd.Series([""] * len(df), index=df.index)

    # Regla 1: monto atípico
    r1 = df["amount"] > amount_threshold
    reasons[r1] += "monto_atipico;"
    logger.info(f"  Regla 1 (monto atípico): {r1.sum():,}")

    # Regla 2: error en transacción
    r2 = df["errors"].notna() & (df["errors"].astype(str).str.strip() != "") & (df["errors"].astype(str).str.strip() != "nan")
    reasons[r2] += "error_en_transaccion;"
    logger.info(f"  Regla 2 (con error): {r2.sum():,}")

    # Regla 3: tarjeta en dark web
    r3 = pd.Series(False, index=df.index)
    if "card_on_dark_web" in df.columns:
        r3 = df["card_on_dark_web"] == True
        reasons[r3] += "tarjeta_en_dark_web;"
        logger.info(f"  Regla 3 (dark web): {r3.sum():,}")

    # Regla 4: alto endeudamiento
    r4 = df["debt_income_ratio"] > 3
    reasons[r4] += "alto_endeudamiento;"
    logger.info(f"  Regla 4 (deuda/ingreso > 3): {r4.sum():,}")

    df["is_suspicious"] = r1 | r2 | r3 | r4
    df["suspicion_reasons"] = reasons.str.rstrip(";")

    logger.info(f"  Total sospechosas: {df['is_suspicious'].sum():,}")
    logger.info(f"  Total fraudes reales: {df['is_fraud'].sum():,}")

    df.to_parquet("/tmp/securelink_rules.parquet", index=False)
    logger.info("Reglas aplicadas.")


# ============================================================
# TAREA 6: Calcular métricas
# ============================================================
def _compute_metrics(**context):
    logger.info("=== TAREA 6: Calculando métricas ===")
    import pickle
    df = pd.read_parquet("/tmp/securelink_rules.parquet")

    total = len(df)
    total_fraud = int(df["is_fraud"].sum())
    amount_at_risk = float(df[df["is_fraud"]]["amount"].sum())

    fp = int(((df["is_suspicious"] == True) & (df["is_fraud"] == False)).sum())
    fn = int(((df["is_suspicious"] == False) & (df["is_fraud"] == True)).sum())
    fp_rate = fp / max(int((df["is_fraud"] == False).sum()), 1)
    fn_rate = fn / max(total_fraud, 1)

    metrics_global = {
        "total_transactions": total,
        "total_fraud": total_fraud,
        "fraud_rate": float(total_fraud / total) if total > 0 else 0,
        "total_amount_at_risk": amount_at_risk,
        "avg_fraud_amount": float(df[df["is_fraud"]]["amount"].mean()) if total_fraud > 0 else 0,
        "false_positive_rate": float(fp_rate),
        "false_negative_rate": float(fn_rate),
        "dataset_start_date": df["transaction_date"].min().date() if df["transaction_date"].notna().any() else None,
        "dataset_end_date": df["transaction_date"].max().date() if df["transaction_date"].notna().any() else None,
    }
    logger.info(f"  Tasa de fraude: {metrics_global['fraud_rate']:.2%}")
    logger.info(f"  Monto en riesgo: ${amount_at_risk:,.2f}")

    # Por MCC
    fraud_mcc = (
        df.groupby(["mcc", "mcc_description"])
        .agg(total_transactions=("transaction_id", "count"), total_fraud=("is_fraud", "sum"))
        .reset_index()
    )
    fraud_mcc["fraud_rate"] = fraud_mcc["total_fraud"] / fraud_mcc["total_transactions"]
    fraud_mcc["amount_at_risk"] = fraud_mcc.apply(
        lambda row: float(df[(df["mcc"] == row["mcc"]) & df["is_fraud"]]["amount"].sum()), axis=1
    )

    # Por tipo de tarjeta
    group_cols = [c for c in ["card_brand", "card_type", "has_chip"] if c in df.columns]
    if group_cols:
        fraud_card = (
            df.groupby(group_cols)
            .agg(total_transactions=("transaction_id", "count"), total_fraud=("is_fraud", "sum"))
            .reset_index()
        )
        fraud_card["fraud_rate"] = fraud_card["total_fraud"] / fraud_card["total_transactions"]
        fraud_card["amount_at_risk"] = 0.0
        for col in ["card_brand", "card_type", "has_chip"]:
            if col not in fraud_card.columns:
                fraud_card[col] = None
    else:
        fraud_card = pd.DataFrame(columns=["card_brand","card_type","has_chip","total_transactions","total_fraud","fraud_rate","amount_at_risk"])

    # Por estado
    state_col = "merchant_state"
    if state_col in df.columns:
        fraud_state = (
            df[~df["is_online"] & df[state_col].notna()]
            .groupby(state_col)
            .agg(total_transactions=("transaction_id", "count"), total_fraud=("is_fraud", "sum"))
            .reset_index()
            .rename(columns={state_col: "state"})
        )
        fraud_state["fraud_rate"] = fraud_state["total_fraud"] / fraud_state["total_transactions"]
        fraud_state["amount_at_risk"] = 0.0
    else:
        fraud_state = pd.DataFrame(columns=["state","total_transactions","total_fraud","fraud_rate","amount_at_risk"])

    # Por merchant
    merchant_cols = [c for c in ["merchant_id", "merchant_city", "merchant_state", "mcc_description"] if c in df.columns]
    fraud_merchant = (
        df.groupby(merchant_cols)
        .agg(total_transactions=("transaction_id", "count"), total_fraud=("is_fraud", "sum"))
        .reset_index()
    )
    fraud_merchant["fraud_rate"] = fraud_merchant["total_fraud"] / fraud_merchant["total_transactions"]
    fraud_merchant["amount_at_risk"] = 0.0
    for col in ["merchant_id","merchant_city","merchant_state","mcc_description"]:
        if col not in fraud_merchant.columns:
            fraud_merchant[col] = ""

    # Perfiles de usuario
    user_id_col = "user_id" if "user_id" in df.columns else ("client_id" if "client_id" in df.columns else None)
    if user_id_col:
        agg_dict = {
            "total_transactions": ("transaction_id", "count"),
            "total_fraud": ("is_fraud", "sum"),
            "total_spent": ("amount", "sum"),
            "avg_transaction": ("amount", "mean"),
        }
        for col, alias in [("current_age","age"),("gender","gender"),("yearly_income","yearly_income"),
                           ("total_debt","total_debt"),("debt_income_ratio","debt_income_ratio"),
                           ("credit_score","credit_score")]:
            if col in df.columns:
                agg_dict[alias] = (col, "first")
        if "card_id" in df.columns:
            agg_dict["num_cards"] = ("card_id", "nunique")

        user_profiles = df.groupby(user_id_col).agg(**agg_dict).reset_index()
        user_profiles = user_profiles.rename(columns={user_id_col: "user_id"})
        user_profiles["fraud_rate"] = user_profiles["total_fraud"] / user_profiles["total_transactions"]
        user_profiles["amount_at_risk"] = 0.0
        for col in ["age","gender","yearly_income","total_debt","debt_income_ratio","credit_score","num_cards"]:
            if col not in user_profiles.columns:
                user_profiles[col] = None
    else:
        user_profiles = pd.DataFrame()

    with open("/tmp/securelink_metrics.pkl", "wb") as f:
        pickle.dump({
            "global": metrics_global,
            "by_mcc": fraud_mcc,
            "by_card": fraud_card,
            "by_state": fraud_state,
            "by_merchant": fraud_merchant,
            "user_profiles": user_profiles,
        }, f)

    logger.info("Métricas calculadas.")


# ============================================================
# TAREA 7: Cargar al DWH
# ============================================================
def _load_to_dwh(**context):
    logger.info("=== TAREA 7: Cargando al DWH ===")
    import pickle

    df = pd.read_parquet("/tmp/securelink_rules.parquet")
    with open("/tmp/securelink_metrics.pkl", "rb") as f:
        metrics = pickle.load(f)

    hook = PostgresHook(postgres_conn_id=DWH_CONN)
    conn = hook.get_conn()
    cur = conn.cursor()

    # Cargar transacciones en lotes
    logger.info("  Cargando transacciones procesadas...")
    cur.execute("TRUNCATE TABLE transactions_processed")

    def safe(val, cast=None):
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return None
        return cast(val) if cast else val

    batch_size = 10_000
    for start in range(0, len(df), batch_size):
        batch = df.iloc[start:start + batch_size]
        rows = []
        for _, row in batch.iterrows():
            rows.append((
                str(row.get("transaction_id", "")),
                safe(row.get("card_id")),
                safe(row.get("user_id")),
                safe(row.get("transaction_date")),
                safe(row.get("amount"), float),
                safe(row.get("merchant_id")),
                safe(row.get("merchant_city")),
                safe(row.get("merchant_state")),
                safe(row.get("mcc")),
                safe(row.get("mcc_description")),
                safe(row.get("transaction_type")),
                safe(row.get("errors")),
                safe(row.get("current_age"), int),
                safe(row.get("gender")),
                None,
                safe(row.get("yearly_income"), float),
                safe(row.get("total_debt"), float),
                safe(row.get("credit_score"), int),
                safe(row.get("card_brand")),
                safe(row.get("card_type")),
                bool(row.get("has_chip", False)) if row.get("has_chip") is not None else False,
                bool(row.get("card_on_dark_web", False)) if row.get("card_on_dark_web") is not None else False,
                safe(row.get("credit_limit"), float),
                safe(row.get("debt_income_ratio"), float),
                safe(row.get("distance_km"), float),
                bool(row.get("is_fraud", False)),
                bool(row.get("is_suspicious", False)),
                safe(row.get("suspicion_reasons")),
            ))
        cur.executemany("""
            INSERT INTO transactions_processed VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            ) ON CONFLICT (transaction_id) DO NOTHING
        """, rows)
        logger.info(f"    Insertadas filas {start}–{start+len(batch)}")

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

    conn.commit()
    cur.close()
    conn.close()
    logger.info("Carga al DWH completada.")


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
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["securelink", "fraude", "etl"],
    description="Pipeline de detección de fraude en transacciones financieras",
) as dag:

    t1 = PythonOperator(task_id="validate_files", python_callable=_validate_files)
    t2 = PythonOperator(task_id="ingest_data", python_callable=_ingest_data)
    t3 = PythonOperator(task_id="clean_data", python_callable=_clean_data)
    t4 = PythonOperator(task_id="build_features", python_callable=_build_features)
    t5 = PythonOperator(task_id="apply_fraud_rules", python_callable=_apply_fraud_rules)
    t6 = PythonOperator(task_id="compute_metrics", python_callable=_compute_metrics)
    t7 = PythonOperator(task_id="load_to_dwh", python_callable=_load_to_dwh)
    t8 = PythonOperator(task_id="quality_check", python_callable=_quality_check)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8