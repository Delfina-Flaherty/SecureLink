"""
SecureLink - Dashboard de Detección de Fraude
Conecta al DWH (postgres-dwh) y muestra los resultados del pipeline.

Para correr localmente (sin Docker):
    pip install streamlit pandas psycopg2-binary sqlalchemy plotly
    streamlit run dashboard/app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os

# ── Configuración de la página ───────────────────────────────────────────────
st.set_page_config(
    page_title="SecureLink - Detección de Fraude",
    page_icon="🔒",
    layout="wide",
)

# ── Conexión a la base de datos ──────────────────────────────────────────────
@st.cache_resource
def get_engine():
    host     = os.getenv("DB_HOST", "localhost")
    port     = os.getenv("DB_PORT", "5432")
    db       = os.getenv("DB_NAME", "securelink")
    user     = os.getenv("DB_USER", "dwh")
    password = os.getenv("DB_PASSWORD", "dwh123")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)

@st.cache_data(ttl=60)
def query(sql):
    """Ejecuta una query y devuelve un DataFrame. Cache de 60 segundos."""
    try:
        engine = get_engine()
        return pd.read_sql(sql, engine)
    except Exception as e:
        return None

# ── Barra lateral: navegación ────────────────────────────────────────────────
st.sidebar.title("🔒 SecureLink")
pagina = st.sidebar.radio(
    "Navegar a:",
    ["Panel General", "Panel de Usuario", "Panel de Comercios"]
)

# ═══════════════════════════════════════════════════════════════════════════
# PANEL GENERAL
# ═══════════════════════════════════════════════════════════════════════════
if pagina == "Panel General":
    st.title("Panel General de Fraude")
    st.caption("Visión agregada del fenómeno del fraude en el dataset histórico.")

    # ── Verificar si el pipeline ya corrió ──
    metricas = query("SELECT * FROM fraud_metrics_global ORDER BY computed_at DESC LIMIT 1")

    if metricas is None or metricas.empty:
        st.warning(
            "⚠️ No hay datos aún. "
            "Primero tenés que correr el pipeline en Airflow: "
            "http://localhost:8080 → DAGs → securelink_fraud_pipeline → ▶️ Trigger DAG"
        )
        st.stop()

    m = metricas.iloc[0]

    # ── Tarjetas resumen ──
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total transacciones", f"{m['total_transactions']:,}")
    col2.metric("Transacciones fraudulentas", f"{m['total_fraud']:,}",
                f"{m['fraud_rate']:.2%} del total")
    col3.metric("Monto total en riesgo", f"${m['total_amount_at_risk']:,.0f}")
    col4.metric("Monto promedio fraude", f"${m['avg_fraud_amount']:,.2f}")

    st.divider()

    col1, col2 = st.columns(2)
    col1.metric("Tasa de Falsos Positivos",
                f"{m['false_positive_rate']:.2%}",
                help="Transacciones legítimas marcadas como sospechosas")
    col2.metric("Tasa de Falsos Negativos",
                f"{m['false_negative_rate']:.2%}",
                help="Fraudes reales que el sistema no detectó")

    st.divider()

    # ── Fraude por MCC ──
    st.subheader("🏪 Fraude por Categoría de Comercio (MCC)")
    st.caption("Las categorías con mayor tasa de fraude o mayor monto en riesgo.")

    mcc_df = query("""
        SELECT mcc_description, total_fraud, fraud_rate, amount_at_risk
        FROM fraud_by_mcc
        ORDER BY amount_at_risk DESC
        LIMIT 15
    """)
    if mcc_df is not None and not mcc_df.empty:
        fig = px.bar(
            mcc_df, x="amount_at_risk", y="mcc_description",
            orientation="h",
            labels={"amount_at_risk": "Monto en riesgo ($)", "mcc_description": "Categoría"},
            color="fraud_rate",
            color_continuous_scale="Reds",
            title="Top 15 categorías por monto en riesgo"
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

    # ── Fraude por estado ──
    st.subheader("🗺️ Fraude por Estado (EEUU)")
    state_df = query("""
        SELECT state, total_fraud, fraud_rate, amount_at_risk
        FROM fraud_by_state
        WHERE state IS NOT NULL AND state != ''
        ORDER BY total_fraud DESC
    """)
    if state_df is not None and not state_df.empty:
        fig = px.choropleth(
            state_df,
            locations="state",
            locationmode="USA-states",
            color="fraud_rate",
            scope="usa",
            color_continuous_scale="Reds",
            labels={"fraud_rate": "Tasa de fraude"},
            title="Tasa de fraude por estado"
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Fraude por tipo de tarjeta ──
    st.subheader("💳 Fraude por Tipo de Tarjeta")
    card_df = query("""
        SELECT card_brand, card_type, has_chip, total_fraud, fraud_rate, amount_at_risk
        FROM fraud_by_card_type
        ORDER BY total_fraud DESC
    """)
    if card_df is not None and not card_df.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.pie(
                card_df.groupby("card_brand")["total_fraud"].sum().reset_index(),
                values="total_fraud", names="card_brand",
                title="Fraude por marca de tarjeta"
            )
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.pie(
                card_df.groupby("card_type")["total_fraud"].sum().reset_index(),
                values="total_fraud", names="card_type",
                title="Fraude por tipo (Crédito/Débito/Prepago)"
            )
            st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# PANEL DE USUARIO
# ═══════════════════════════════════════════════════════════════════════════
elif pagina == "Panel de Usuario":
    st.title("Panel de Perfil de Usuario")
    st.caption("Analizá el comportamiento transaccional de un cliente específico.")

    user_id = st.text_input("Ingresá el ID del usuario:", placeholder="Ej: 1234")

    if not user_id:
        st.info("Ingresá un ID de usuario para ver su perfil.")
        st.stop()

    perfil = query(f"""
        SELECT * FROM user_profiles WHERE user_id = '{user_id}'
    """)

    if perfil is None or perfil.empty:
        st.error(f"No se encontró el usuario con ID: {user_id}")
        st.stop()

    p = perfil.iloc[0]

    col1, col2, col3 = st.columns(3)
    col1.metric("Edad", f"{p['age']} años")
    col1.metric("Género", p["gender"])
    col2.metric("Ingreso anual", f"${p['yearly_income']:,.0f}")
    col2.metric("Deuda total", f"${p['total_debt']:,.0f}")
    col3.metric("Score crediticio", f"{p['credit_score']}")
    col3.metric("Cantidad de tarjetas", f"{p['num_cards']}")

    st.divider()

    col1, col2, col3 = st.columns(3)
    col1.metric("Transacciones totales", f"{p['total_transactions']:,}")
    col2.metric("Transacciones fraudulentas", f"{p['total_fraud']:,}",
                f"{p['fraud_rate']:.2%}")
    col3.metric("Monto en riesgo", f"${p['amount_at_risk']:,.2f}")

    # Historial de transacciones del usuario
    st.subheader("Historial de transacciones")
    txns = query(f"""
        SELECT transaction_date, amount, merchant_city, merchant_state,
               mcc_description, transaction_type, is_fraud, is_suspicious
        FROM transactions_processed
        WHERE user_id = '{user_id}'
        ORDER BY transaction_date DESC
        LIMIT 100
    """)
    if txns is not None and not txns.empty:
        st.dataframe(txns, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# PANEL DE COMERCIOS
# ═══════════════════════════════════════════════════════════════════════════
elif pagina == "Panel de Comercios":
    st.title("Panel de Comercios")
    st.caption("Merchants con mayor concentración de fraude.")

    top_n = st.slider("Mostrar top N merchants:", 10, 50, 20)

    # Por cantidad de fraudes
    st.subheader(f"🏆 Top {top_n} merchants por cantidad de fraudes")
    merchants_count = query(f"""
        SELECT merchant_id, merchant_city, merchant_state, mcc_description,
               total_fraud, fraud_rate, amount_at_risk
        FROM fraud_by_merchant
        ORDER BY total_fraud DESC
        LIMIT {top_n}
    """)
    if merchants_count is not None and not merchants_count.empty:
        st.dataframe(merchants_count, use_container_width=True)

    # Por monto en riesgo
    st.subheader(f"💰 Top {top_n} merchants por monto en riesgo")
    merchants_amount = query(f"""
        SELECT merchant_id, merchant_city, merchant_state, mcc_description,
               total_fraud, fraud_rate, amount_at_risk
        FROM fraud_by_merchant
        ORDER BY amount_at_risk DESC
        LIMIT {top_n}
    """)
    if merchants_amount is not None and not merchants_amount.empty:
        fig = px.bar(
            merchants_amount,
            x="merchant_id", y="amount_at_risk",
            color="fraud_rate",
            color_continuous_scale="Reds",
            labels={"amount_at_risk": "Monto en riesgo ($)", "merchant_id": "Merchant"},
            title=f"Top {top_n} merchants por monto en riesgo"
        )
        st.plotly_chart(fig, use_container_width=True)
