"""
SecureLink - Dashboard de Detección de Fraude
Conecta al DWH (postgres-dwh) y muestra los resultados del pipeline.
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text

# ── Configuración de la página ───────────────────────────────────────────────
st.set_page_config(
    page_title="SecureLink - Detección de Fraude",
    page_icon="🔒",
    layout="wide",
)

# Códigos válidos de estados de EEUU (50 + DC). Filtra países y valores raros
# que el dataset mete en merchant_state.
US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA",
    "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX",
    "UT", "VT", "VA", "WA", "WV", "WI", "WY",
}

DOW_LABELS = {0: "Dom", 1: "Lun", 2: "Mar", 3: "Mié", 4: "Jue", 5: "Vie", 6: "Sáb"}

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

@st.cache_data(ttl=300)
def query(sql, params=None):
    """Ejecuta una query con cache de 5 min. Usa bind params (`:nombre`) si se pasa params."""
    try:
        engine = get_engine()
        if params:
            return pd.read_sql(text(sql), engine, params=params)
        return pd.read_sql(sql, engine)
    except Exception:
        return None


# ── Barra lateral ────────────────────────────────────────────────────────────
st.sidebar.title("🔒 SecureLink")
st.sidebar.caption("Sistema de detección de fraude")

pagina = st.sidebar.radio(
    "Navegar a:",
    ["Panel General", "Panel de Usuario", "Panel de Comercios",
     "Explorador con Filtros", "Análisis de Datasets"]
)

# Info de la última corrida del pipeline (en sidebar)
run_info = query("""
    SELECT run_at, status, rows_processed, rows_fraud
    FROM pipeline_run_log
    ORDER BY run_at DESC LIMIT 1
""")
if run_info is not None and not run_info.empty:
    r = run_info.iloc[0]
    st.sidebar.divider()
    st.sidebar.caption("📅 Última corrida del pipeline (flujo de procesamiento)")
    st.sidebar.write(f"**{r['run_at'].strftime('%Y-%m-%d %H:%M')}**")
    st.sidebar.write(f"Estado: `{r['status']}`")
    st.sidebar.write(f"{r['rows_processed']:,} filas procesadas")
    st.sidebar.write(f"{r['rows_fraud']:,} fraudes detectados")

# ── Filtro global: ¿qué población mostramos en la pestaña Datos? ──
# "Fraude real" = etiqueta oficial (is_fraud, ground truth).
# "Sospechosos" = lo que marca el puntaje del pipeline (is_suspicious).
st.sidebar.divider()
modo_vista = st.sidebar.radio(
    "🔎 Mostrar datos de:",
    ["Fraude real", "Sospechosos (modelo)"],
    help="**Fraude real**: transacciones etiquetadas como fraude por la verdad de "
         "referencia (`is_fraud`). **Sospechosos**: lo que el puntaje ponderado del "
         "pipeline marcó como sospechoso (`is_suspicious`). Afecta a toda la pestaña "
         "Datos del Panel General; la matriz de confusión siempre compara ambos.",
)
SUSP_MODE = modo_vista.startswith("Sospechosos")
FRAUD_COL = "is_suspicious" if SUSP_MODE else "is_fraud"
# Nombres de columnas en las tablas pre-agregadas según el modo
SEG_COUNT = "total_suspicious" if SUSP_MODE else "total_fraud"
SEG_AMOUNT = "amount_suspicious" if SUSP_MODE else "amount_at_risk"
# Etiquetas para títulos y textos según el modo de vista
VISTA_LABEL = "sospechosas" if SUSP_MODE else "fraude"   # sustantivo plural / masa
VISTA_CAP = "Sospechosas" if SUSP_MODE else "Fraude"      # capitalizado (ejes, títulos)
VISTA_CANT = "Cantidad de sospechosas" if SUSP_MODE else "Cantidad de fraudes"
VISTA_TASA = "Tasa de sospechosas" if SUSP_MODE else "Tasa de fraude"
VISTA_PCT = "% Sospechosas" if SUSP_MODE else "% Fraude"
VISTA_MONTO = "Monto sospechoso" if SUSP_MODE else "Monto en riesgo"


# ═══════════════════════════════════════════════════════════════════════════
# PANEL GENERAL
# ═══════════════════════════════════════════════════════════════════════════
if pagina == "Panel General":
    st.title("🔍 Panel General de Fraude")
    st.caption("Visión agregada del fenómeno del fraude en el dataset (conjunto de datos) histórico.")

    tab_explic, tab_datos = st.tabs(["📖 Explicaciones", "📊 Datos"])

    # ══════════════════════════════════════════════════════════════════════
    # PESTAÑA EXPLICACIONES — todo el texto y contexto del proyecto
    # ══════════════════════════════════════════════════════════════════════
    with tab_explic:
        st.header("📖 ¿Qué es SecureLink?")
        st.markdown(
            "**SecureLink** es un sistema de **detección de fraude** en transacciones con tarjeta. "
            "Toma datos históricos (~13.3 millones de transacciones entre 2010 y 2019), los procesa "
            "con un pipeline (flujo de tareas) orquestado en **Apache Airflow**, aplica reglas para "
            "marcar transacciones sospechosas y publica los resultados en este dashboard (tablero).\n\n"
            "**Flujo general:**\n"
            "1. **Ingesta** — lee 5 archivos (3 CSV + 2 JSON) con transacciones, usuarios, tarjetas, "
            "etiquetas de fraude y catálogo de categorías de comercio.\n"
            "2. **Limpieza** — normaliza fechas, montos, descarta filas inválidas y duplicadas.\n"
            "3. **Features (variables derivadas)** — calcula `debt_income_ratio` (cociente deuda/ingreso) "
            "y clasifica cada transacción como Online (en línea) o Presencial.\n"
            "4. **Detección** — calcula un puntaje ponderado de riesgo (ver abajo) y marca cada transacción como sospechosa o no.\n"
            "5. **Carga al DWH** — guarda el detalle y agregados en PostgreSQL.\n"
            "6. **Consumo** — este dashboard consulta los agregados para mostrar las visualizaciones.\n\n"
            "**¿Qué responde el sistema?**\n"
            "- ¿Qué porcentaje de transacciones es fraude?\n"
            "- ¿Dónde se concentra (geografía, comercios, categorías)?\n"
            "- ¿Cuál es el monto en riesgo?\n"
            "- ¿Qué tan precisas son las reglas (precision / recall / F1)?"
        )

        st.divider()

        st.header("⚖️ Detección por puntaje ponderado")
        st.markdown(
            "El pipeline ya **no** usa un OR de reglas (donde bastaba que una se cumpliera). "
            "Ese enfoque daba una precisión de ~0.4%: el 99.6% de las alarmas eran falsas, "
            "porque señales como *alto endeudamiento* u *horario inusual* casi no correlacionan "
            "con el fraude real.\n\n"
            "Ahora cada **señal suma puntos** según su poder predictivo (medido sobre los datos "
            "reales: tasa de fraude por canal, monto y error). Se calcula un **puntaje** "
            "(`fraud_score`) por transacción y se marca **sospechosa** (`is_suspicious = True`) "
            "solo si el puntaje **alcanza o supera el umbral = 4**. La columna `suspicion_reasons` "
            "lista las señales que sumaron."
        )
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(
                "##### Señales y pesos del puntaje\n"
                "| Señal | Puntos | Por qué |\n"
                "|---|---|---|\n"
                "| 🌐 **Online** | +3 | Online tiene **13× más fraude** que presencial (0.54% vs 0.04%) |\n"
                "| 💰 **Monto > \\$500** | +4 | A mayor monto, mayor tasa (>\\$500 = 0.82%, 8× la base) |\n"
                "| 💰 Monto \\$200–500 | +3 | 0.66% de fraude |\n"
                "| 💰 Monto \\$100–200 | +1 | 0.23% de fraude |\n"
                "| 🧮 **Monto atípico p/usuario** | +2 | Supera el p99 del historial de ESE titular |\n"
                "| ⚠️ **Con error** | +1 | `errors` no vacío → 2.7× más fraude |\n"
                "| 🕷️ **Tarjeta en dark web** | +5 | Señal dura: la tarjeta circuló en filtraciones |\n"
                "| 🌐💰 Combo online y >\\$200 | +3 | Interacción: lo online + caro es lo más riesgoso |\n"
            )
        with col2:
            st.markdown(
                "##### Cómo se decide\n"
                "1. Se suman los puntos de todas las señales que se cumplen.\n"
                "2. Si `fraud_score >= 4` → **sospechosa**.\n\n"
                "**Por qué un puntaje y no un OR:** un puntaje permite exigir "
                "*acumulación de evidencia*. Una transacción online sola (+3) no alcanza; "
                "online + monto alto (+3+3) sí. Así se filtran las alarmas débiles sin perder "
                "los casos con varias señales a la vez.\n\n"
                "**Punto de operación elegido (balanceado):** triplica la precisión "
                "(de ~0.4% a ~1.3%) manteniendo la sensibilidad (~34%) — o sea, detectamos "
                "el mismo fraude que antes pero con **un tercio de las falsas alarmas**.\n\n"
                "El umbral es ajustable: subirlo da más precisión (menos ruido), bajarlo da "
                "más sensibilidad (atrapa más fraude). Ver la **matriz de confusión** en la "
                "pestaña Datos para las métricas exactas."
            )
        st.info(
            "💡 **Limitación reconocida:** sigue siendo un modelo heurístico (pesos elegidos a mano "
            "con base en los datos), no un modelo de Machine Learning entrenado. Los perfiles por "
            "usuario (p99 de monto) se calculan sobre el historial completo del dataset; en producción "
            "se calcularían solo con transacciones verificadas como legítimas. La evolución prevista es "
            "entrenar un clasificador con `train_fraud_labels.json` (ground truth) que reemplace estos "
            "pesos fijos por un puntaje probabilístico aprendido."
        )

        st.divider()

        st.header("📌 Fuente de los datos en los gráficos")
        st.markdown(
            "**TP / FP / FN / TN** (matriz de confusión de la pestaña Datos):\n"
            "- ✅ **TP (True Positive / Verdadero Positivo)**: fraude real correctamente detectado por el modelo.\n"
            "- ⚠️ **FP (False Positive / Falso Positivo)**: legítima marcada por error (falsa alarma).\n"
            "- ❌ **FN (False Negative / Falso Negativo)**: fraude que se escapó (no detectado).\n"
            "- ✅ **TN (True Negative / Verdadero Negativo)**: legítima correctamente ignorada."
        )
        st.info(
            "🔎 **Filtro de vista (barra lateral):** la pestaña Datos tiene un selector "
            "**Fraude real / Sospechosos (modelo)** que cambia TODOS los KPIs y gráficos entre:\n"
            "- **Fraude real** (`is_fraud`): dónde ocurrió fraude de verdad según el ground truth "
            "(`train_fraud_labels.json`).\n"
            "- **Sospechosos (modelo)** (`is_suspicious`): dónde el puntaje del pipeline *cree* que "
            "hay riesgo.\n\n"
            "Comparar ambas vistas muestra dónde el modelo acierta y dónde se desvía. "
            "La **matriz de confusión** es el único gráfico que siempre compara los dos a la vez, "
            "para medir precisión / sensibilidad / F1."
        )

        st.divider()

        st.header("🚀 Próximos pasos — cómo mejorar el acierto de fraudes")
        st.markdown(
            "El puntaje ponderado actual es un baseline (punto de partida) heurístico. "
            "Estas son las palancas concretas para subir el acierto (precision / recall / F1), "
            "ordenadas por impacto esperado:"
        )

        with st.expander("1️⃣ Modelo de Machine Learning (mayor impacto)", expanded=True):
            st.markdown(
                "El puntaje ponderado usa pesos fijos elegidos a mano y se queda corto en **recall (sensibilidad)**. "
                "Como ya tenemos el ground truth (verdad de referencia) en `train_fraud_labels.json`, "
                "podemos entrenar un **clasificador supervisado** que devuelva una **probabilidad** "
                "de fraude en vez de un sí/no.\n\n"
                "**Algoritmos recomendados:**\n"
                "- **XGBoost** o **LightGBM** — estándar para fraude tabular: manejan bien clases "
                "muy desbalanceadas (~0.1% fraude), no requieren mucho preprocesamiento y son rápidos.\n"
                "- **Random Forest** — más simple, buena baseline para comparar.\n\n"
                "**Ventaja clave:** la probabilidad permite **calibrar el umbral** según el costo "
                "de falsos positivos vs falsos negativos del negocio (no es lo mismo molestar a un "
                "cliente legítimo que perder $10.000 en un fraude).\n\n"
                "**Implementación sugerida en el pipeline:** agregar una tarea `score_model` después "
                "de `build_features` que reemplace (o complemente) a `apply_fraud_rules`."
            )

        with st.expander("2️⃣ Feature engineering (variables derivadas) — alto impacto, bajo costo", expanded=True):
            st.markdown(
                "Las señales más predictivas en fraude **no están en la fila**, están en el "
                "**comportamiento histórico del usuario**. Algunas features (variables) a sumar:\n\n"
                "**Velocity features (frecuencia / velocidad):**\n"
                "- Transacciones por hora / por día del usuario.\n"
                "- Cantidad de comercios distintos en últimas 24 horas.\n"
                "- Monto promedio de los últimos 30 días.\n\n"
                "**Desviación del patrón habitual:**\n"
                "- ¿Este monto está a más de N desvíos estándar del promedio del usuario?\n"
                "- ¿El usuario nunca compró en esta categoría MCC (código de categoría de comercio)?\n"
                "- Hora atípica: el usuario nunca operó a las 3 AM y ahora sí.\n\n"
                "**Distancia geográfica:**\n"
                "- Dos compras en lugares lejanos con minutos de diferencia (físicamente imposible).\n"
                "- Operación en un estado/país donde el usuario nunca operó."
            )

        with st.expander("3️⃣ Calibrar el umbral de la Regla 1 (monto atípico)", expanded=False):
            st.markdown(
                "Hoy la Regla 1 usa el **percentil 99 global** del dataset (conjunto de datos). "
                "Esto trata a todas las transacciones igual, pero un monto alto en *Grocery Stores* "
                "(supermercados) no significa lo mismo que en *Jewelry Stores* (joyerías).\n\n"
                "**Mejoras posibles:**\n"
                "- **Por categoría MCC:** percentil 99 calculado por categoría de comercio.\n"
                "- **Por usuario:** percentil 99 relativo al histórico del titular (un monto $500 "
                "puede ser normal para un usuario y atípico para otro).\n"
                "- **Por canal:** umbrales distintos para Online (en línea) vs Presencial (con tarjeta física)."
            )

        with st.expander("4️⃣ Cost-sensitive learning (optimización por costo)", expanded=False):
            st.markdown(
                "Actualmente la matriz de confusión y F1 tratan a falsos positivos y falsos negativos "
                "como equivalentes. En la realidad **no lo son**:\n\n"
                "- **FN (Falso Negativo)** — fraude que se escapó → costo = monto perdido (puede ser miles de dólares).\n"
                "- **FP (Falso Positivo)** — legítima marcada por error → costo = fricción con el cliente "
                "(llamada de verificación, transacción bloqueada).\n\n"
                "**Cost-sensitive learning (aprendizaje sensible al costo):** definir el costo monetario "
                "de cada error y optimizar el umbral del modelo para **minimizar costo esperado**, no F1. "
                "Esto suele bajar el umbral de detección (más alertas) cuando el costo de un FN es alto."
            )

        with st.expander("5️⃣ Ensamble: combinar reglas + modelo", expanded=False):
            st.markdown(
                "No hace falta elegir entre reglas y modelo — se pueden combinar:\n\n"
                "- Las **reglas** son interpretables y atrapan casos obvios (tarjeta en dark web).\n"
                "- El **modelo** captura patrones sutiles que las reglas no ven.\n\n"
                "**Estrategia:** marcar como sospechosa si **(regla cualquiera) OR (probabilidad del modelo > umbral)**. "
                "También permite usar el modelo solo cuando ninguna regla disparó, manteniendo la "
                "explicabilidad cuando se puede."
            )

        with st.expander("6️⃣ Otras mejoras (más experimentales)", expanded=False):
            st.markdown(
                "- **Detección de anomalías no supervisada** (Isolation Forest, Autoencoder): "
                "detecta transacciones \"raras\" sin necesidad de labels — útil para fraude nuevo "
                "que aún no fue etiquetado.\n"
                "- **Manejo del desbalance con SMOTE** o class weights al entrenar el modelo "
                "(la clase 'fraude' es ~0.1%, casi todos los algoritmos por defecto la ignoran).\n"
                "- **Particionar el DWH** (almacén de datos) por fecha para acelerar consultas "
                "y permitir entrenamientos incrementales.\n"
                "- **API REST de scoring** (puntuación en tiempo real) para que otros sistemas "
                "consulten el riesgo antes de aprobar una transacción.\n"
                "- **Streaming** (Kafka + Flink) si hace falta detección con latencia < 1 segundo."
            )

        st.success(
            "💡 **Próximo paso recomendado para esta entrega académica:** entrenar un **LightGBM** "
            "básico con las features actuales + 2-3 velocity features por usuario, y comparar "
            "precision / recall / F1 contra las reglas. Mantener las reglas como **baseline** "
            "explicable y mostrar el modelo como mejora **medida**. "
            "Tradeoff (compromiso): un modelo es menos interpretable que las reglas — para una "
            "entrega académica conviene reportar **ambos** resultados, no reemplazar uno por el otro."
        )

    # ══════════════════════════════════════════════════════════════════════
    # PESTAÑA DATOS — KPIs + visualizaciones (texto al mínimo)
    # ══════════════════════════════════════════════════════════════════════
    with tab_datos:
        metricas = query("SELECT * FROM fraud_metrics_global ORDER BY computed_at DESC LIMIT 1")
        if metricas is None or metricas.empty:
            st.warning(
                "⚠️ No hay datos aún. Hay que correr el pipeline (flujo de procesamiento) primero. "
                "**Opción A (UI):** abrir http://localhost:8080 → DAG `securelink_fraud_pipeline` → ▶️ Trigger DAG. "
                "**Opción B (CLI, recomendada si tu PC tiene poca RAM):** ejecutar en una terminal "
                "`docker exec airflow-scheduler airflow dags trigger securelink_fraud_pipeline` "
                "(ver sección *Troubleshooting avanzado* del README si el webserver se cae)."
            )
            st.stop()
        m = metricas.iloc[0]

        # Indicador del modo de vista activo
        if SUSP_MODE:
            st.info("🔎 **Vista: Sospechosos (modelo)** — los KPIs y gráficos de abajo "
                    "muestran lo que el puntaje del pipeline marcó como sospechoso "
                    "(`is_suspicious`). Cambiá a *Fraude real* en la barra lateral para ver "
                    "la etiqueta oficial.")
        else:
            st.info("🔎 **Vista: Fraude real** — los KPIs y gráficos de abajo muestran el "
                    "fraude confirmado por la verdad de referencia (`is_fraud`). Cambiá a "
                    "*Sospechosos (modelo)* en la barra lateral para ver lo que marca el pipeline.")

        # ── KPIs principales (dependen del modo de vista) ──
        # Se recalculan desde transactions_processed con la columna elegida, así el
        # mismo bloque sirve para fraude real y para sospechosos.
        kpi = query(f"""
            SELECT COUNT(*) AS total_transactions,
                   COUNT(*) FILTER (WHERE {FRAUD_COL}) AS total_flag,
                   COALESCE(SUM(amount) FILTER (WHERE {FRAUD_COL}), 0) AS amount_flag,
                   COALESCE(AVG(amount) FILTER (WHERE {FRAUD_COL}), 0) AS avg_flag
            FROM transactions_processed
        """)
        k = kpi.iloc[0] if (kpi is not None and not kpi.empty) else None

        label_det = "Marcadas sospechosas" if SUSP_MODE else "Fraudes detectados"
        label_amt = "Monto sospechoso" if SUSP_MODE else "Monto en riesgo"
        label_avg = "Monto prom. sospechosa" if SUSP_MODE else "Monto promedio fraude"

        col1, col2, col3, col4 = st.columns(4)
        if k is not None:
            total_tx = int(k["total_transactions"])
            rate = (int(k["total_flag"]) / total_tx) if total_tx else 0
            col1.metric("Total transacciones", f"{total_tx:,}")
            col2.metric(label_det, f"{int(k['total_flag']):,}", f"{rate:.3%} del total")
            col3.metric(label_amt, f"${float(k['amount_flag']):,.0f}")
            col4.metric(label_avg, f"${float(k['avg_flag']):,.2f}")

        # Métricas de evaluación del modelo (siempre sobre is_suspicious vs is_fraud)
        col1, col2 = st.columns(2)
        col1.metric("Tasa Falsos Positivos (modelo)",
                    f"{m['false_positive_rate']:.3%}",
                    help="Transacciones legítimas marcadas como sospechosas por el modelo. "
                         "Es una métrica de evaluación del modelo, no cambia con la vista.")
        col2.metric("Tasa Falsos Negativos (modelo)",
                    f"{m['false_negative_rate']:.3%}",
                    help="Fraudes reales que el modelo NO detectó. "
                         "Es una métrica de evaluación del modelo, no cambia con la vista.")

        if m["dataset_start_date"] is not None and m["dataset_end_date"] is not None:
            st.caption(
                f"📅 **Ventana del dataset**: {m['dataset_start_date']} → {m['dataset_end_date']}"
            )

        st.divider()

        # ── Sub-pestañas con análisis ──
        tab_resumen, tab_tend, tab_geo, tab_com = st.tabs(
            ["📊 Resumen", "📈 Tendencias", "🗺️ Geografía", "🏪 Comercios y tarjetas"]
        )

        # ──────────────────────────────────────────────────────────────────
        # SUB-TAB: RESUMEN
        # ──────────────────────────────────────────────────────────────────
        with tab_resumen:
            st.subheader("💻 Online vs 💳 Presencial (Swipe)")
            st.caption(f"{VISTA_TASA} y monto por canal — online es más vulnerable porque no hay verificación física de la tarjeta.")

            online_df = query(f"""
                SELECT transaction_type,
                       COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE {FRAUD_COL}) AS fraud,
                       ROUND(100.0 * COUNT(*) FILTER (WHERE {FRAUD_COL}) / COUNT(*), 4) AS fraud_pct,
                       ROUND(SUM(amount) FILTER (WHERE {FRAUD_COL})::numeric, 2) AS amount_at_risk
                FROM transactions_processed
                WHERE transaction_type IS NOT NULL
                GROUP BY transaction_type
                ORDER BY transaction_type
            """)
            if online_df is not None and not online_df.empty:
                col1, col2 = st.columns(2)
                with col1:
                    fig = px.bar(
                        online_df, x="transaction_type", y="fraud_pct",
                        text="fraud_pct",
                        color="transaction_type",
                        color_discrete_map={"Online": "#d62728", "Swipe": "#2ca02c"},
                        labels={"fraud_pct": VISTA_PCT, "transaction_type": "Tipo"},
                        title=f"{VISTA_TASA} por canal (%)"
                    )
                    fig.update_traces(texttemplate="%{text:.3f}%", textposition="inside",
                                      insidetextfont=dict(size=14, color="white"))
                    ymax = float(online_df["fraud_pct"].max()) * 1.15
                    fig.update_layout(showlegend=False, height=380,
                                      margin=dict(t=60, b=40, l=40, r=40),
                                      yaxis=dict(range=[0, ymax]))
                    st.plotly_chart(fig, use_container_width=True)
                with col2:
                    fig = px.bar(
                        online_df, x="transaction_type", y="amount_at_risk",
                        text="amount_at_risk",
                        color="transaction_type",
                        color_discrete_map={"Online": "#d62728", "Swipe": "#2ca02c"},
                        labels={"amount_at_risk": f"{VISTA_MONTO} ($)", "transaction_type": "Tipo"},
                        title=f"{VISTA_MONTO} por canal"
                    )
                    fig.update_traces(texttemplate="$%{text:,.0f}", textposition="inside",
                                      insidetextfont=dict(size=14, color="white"))
                    ymax = float(online_df["amount_at_risk"].max()) * 1.15
                    fig.update_layout(showlegend=False, height=380,
                                      margin=dict(t=60, b=40, l=40, r=40),
                                      yaxis=dict(range=[0, ymax]))
                    st.plotly_chart(fig, use_container_width=True)

            st.divider()
            _pos = "Sospechosa" if SUSP_MODE else "Fraude"
            _neg = "No sospechosa" if SUSP_MODE else "Legítima"
            st.subheader(f"💵 Distribución de montos: {_pos.lower()} vs {_neg.lower()}")
            st.caption(f"Histograma normalizado (rango $0–$2000): ¿el monto distingue {_pos.lower()} de {_neg.lower()}?")

            # Usamos TABLESAMPLE BERNOULLI para muestrear las legitimas eficientemente.
            # ORDER BY RANDOM() LIMIT requiere ordenar 12M filas → muy lento.
            # TABLESAMPLE BERNOULLI(2) toma cada fila con 2% de prob al leerla del
            # disco, sin generar random ni ordenar. >100x más rápido y el histograma
            # se ve igual.
            amt_df = query(f"""
                (SELECT amount, '{_pos}' AS tipo FROM transactions_processed
                 WHERE {FRAUD_COL} AND amount BETWEEN 0 AND 2000)
                UNION ALL
                (SELECT amount, '{_neg}' AS tipo
                 FROM transactions_processed TABLESAMPLE BERNOULLI(2)
                 WHERE NOT {FRAUD_COL} AND amount BETWEEN 0 AND 2000
                 LIMIT 100000)
            """)
            if amt_df is not None and not amt_df.empty:
                fig = px.histogram(
                    amt_df, x="amount", color="tipo",
                    nbins=50, barmode="overlay", opacity=0.6,
                    histnorm="percent",
                    color_discrete_map={_pos: "#d62728", _neg: "#2ca02c"},
                    labels={"amount": "Monto ($)", "percent": "% (normalizado)"},
                    title="Distribución de montos por tipo (% normalizado, 0–$2000)"
                )
                fig.update_layout(height=420)
                st.plotly_chart(fig, use_container_width=True)

            st.divider()
            st.subheader("🎯 Matriz de confusión del modelo de sospechosos")
            st.caption("Cruza predicción del modelo (`is_suspicious`) vs realidad (`is_fraud`). "
                       "Siempre compara ambos, independientemente del filtro de vista. "
                       "Definiciones de TP/FP/FN/TN en la pestaña Explicaciones.")
            confusion = query("""
                SELECT
                    COUNT(*) FILTER (WHERE is_fraud AND is_suspicious)         AS tp,
                    COUNT(*) FILTER (WHERE NOT is_fraud AND is_suspicious)     AS fp,
                    COUNT(*) FILTER (WHERE is_fraud AND NOT is_suspicious)     AS fn,
                    COUNT(*) FILTER (WHERE NOT is_fraud AND NOT is_suspicious) AS tn
                FROM transactions_processed
            """)
            if confusion is not None and not confusion.empty:
                c = confusion.iloc[0]
                cm = pd.DataFrame({
                    "Predicho: Sospechosa": [int(c["tp"]), int(c["fp"])],
                    "Predicho: Limpia":     [int(c["fn"]), int(c["tn"])],
                }, index=["Real: Fraude", "Real: Legítima"])
                col1, col2 = st.columns([2, 1])
                with col1:
                    fig = px.imshow(
                        cm.values, text_auto=",",
                        x=cm.columns, y=cm.index,
                        color_continuous_scale="Blues",
                        aspect="auto",
                        title="Matriz de confusión de las reglas"
                    )
                    fig.update_layout(height=350)
                    st.plotly_chart(fig, use_container_width=True)
                with col2:
                    tp, fp, fn, tn = int(c["tp"]), int(c["fp"]), int(c["fn"]), int(c["tn"])
                    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
                    recall    = tp / (tp + fn) if (tp + fn) > 0 else 0
                    f1        = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
                    st.metric("Precisión (precision)", f"{precision:.2%}",
                              help="De las que marca como sospechosas, ¿qué % son fraude real?")
                    st.metric("Sensibilidad (recall)", f"{recall:.2%}",
                              help="De los fraudes reales, ¿qué % detecta el sistema?")
                    st.metric("F1 (media armónica)", f"{f1:.2%}",
                              help="Media armónica entre precisión y sensibilidad")

        # ──────────────────────────────────────────────────────────────────
        # SUB-TAB: TENDENCIAS
        # ──────────────────────────────────────────────────────────────────
        with tab_tend:
            st.subheader(f"📅 Tendencia anual ({VISTA_LABEL})")
            st.caption("Barras rojas = cantidad absoluta. Línea azul = tasa porcentual.")
            yearly = query(f"""
                SELECT EXTRACT(YEAR FROM transaction_date)::INT AS yr,
                       COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE {FRAUD_COL}) AS fraud,
                       ROUND(100.0 * COUNT(*) FILTER (WHERE {FRAUD_COL}) / COUNT(*), 4) AS fraud_pct,
                       ROUND(SUM(amount) FILTER (WHERE {FRAUD_COL})::numeric, 2) AS amount_at_risk
                FROM transactions_processed
                WHERE transaction_date IS NOT NULL
                GROUP BY yr ORDER BY yr
            """)
            if yearly is not None and not yearly.empty:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=yearly["yr"], y=yearly["fraud"],
                    name=VISTA_CANT,
                    marker_color="#d62728",
                    yaxis="y1",
                ))
                fig.add_trace(go.Scatter(
                    x=yearly["yr"], y=yearly["fraud_pct"],
                    name=VISTA_PCT,
                    mode="lines+markers",
                    line=dict(color="#1f77b4", width=3),
                    yaxis="y2",
                ))
                fig.update_layout(
                    title=f"{VISTA_CANT} y tasa por año",
                    xaxis=dict(title="Año", dtick=1),
                    yaxis=dict(title=VISTA_CANT, side="left"),
                    yaxis2=dict(title=VISTA_PCT, overlaying="y", side="right",
                                tickformat=".3f"),
                    height=400,
                    legend=dict(orientation="h", y=1.1),
                )
                st.plotly_chart(fig, use_container_width=True)

            st.subheader("📆 Tendencia mensual")
            st.caption(f"Transacciones {VISTA_LABEL} mes a mes (2010–2019).")
            monthly = query(f"""
                SELECT DATE_TRUNC('month', transaction_date) AS month,
                       COUNT(*) FILTER (WHERE {FRAUD_COL}) AS fraud,
                       ROUND(SUM(amount) FILTER (WHERE {FRAUD_COL})::numeric, 2) AS amount_at_risk
                FROM transactions_processed
                WHERE transaction_date IS NOT NULL
                GROUP BY month ORDER BY month
            """)
            if monthly is not None and not monthly.empty:
                fig = px.area(
                    monthly, x="month", y="fraud",
                    labels={"month": "Mes", "fraud": VISTA_CANT},
                    title=f"Transacciones {VISTA_LABEL} por mes (2010–2019)"
                )
                fig.update_traces(line_color="#d62728", fillcolor="rgba(214,39,40,0.3)")
                fig.update_layout(height=380)
                st.plotly_chart(fig, use_container_width=True)

            st.subheader("🕒 Mapa de calor (heatmap): día × hora")
            st.caption(f"Concentración de transacciones {VISTA_LABEL} por día de la semana y hora del día. Color más oscuro = más casos.")
            heatmap = query(f"""
                SELECT EXTRACT(DOW FROM transaction_date)::INT AS dow,
                       EXTRACT(HOUR FROM transaction_date)::INT AS hr,
                       COUNT(*) FILTER (WHERE {FRAUD_COL}) AS fraud
                FROM transactions_processed
                WHERE transaction_date IS NOT NULL
                GROUP BY dow, hr
            """)
            if heatmap is not None and not heatmap.empty:
                heatmap["dow_label"] = heatmap["dow"].map(DOW_LABELS)
                pivot = heatmap.pivot(index="dow_label", columns="hr", values="fraud").reindex(
                    ["Dom", "Lun", "Mar", "Mié", "Jue", "Vie", "Sáb"]
                )
                fig = px.imshow(
                    pivot, aspect="auto",
                    color_continuous_scale="Reds",
                    labels=dict(x="Hora del día", y="Día de la semana", color=VISTA_CAP),
                    title=f"Concentración de transacciones {VISTA_LABEL} por día y hora"
                )
                fig.update_layout(height=380)
                st.plotly_chart(fig, use_container_width=True)

        # ──────────────────────────────────────────────────────────────────
        # SUB-TAB: GEOGRAFÍA
        # ──────────────────────────────────────────────────────────────────
        with tab_geo:
            st.caption("📍 Los datos geográficos representan la ubicación del comercio, no del titular. Las online se excluyen del mapa.")
            st.subheader(f"🗺️ {VISTA_TASA} por estado (EEUU)")
            st.caption(f"Mapa coroplético: zonas oscuras = mayor proporción de {VISTA_LABEL}.")
            state_df = query(f"""
                SELECT state, total_transactions,
                       {SEG_COUNT}  AS total_fraud,
                       {SEG_AMOUNT} AS amount_at_risk,
                       CASE WHEN total_transactions > 0
                            THEN {SEG_COUNT}::DOUBLE PRECISION / total_transactions ELSE 0 END AS fraud_rate
                FROM fraud_by_state
                WHERE state IS NOT NULL AND state != ''
            """)
            if state_df is not None and not state_df.empty:
                us_only = state_df[state_df["state"].isin(US_STATES)].copy()
                intl    = state_df[~state_df["state"].isin(US_STATES)].copy()

                if not us_only.empty:
                    fig = px.choropleth(
                        us_only,
                        locations="state", locationmode="USA-states",
                        color="fraud_rate", scope="usa",
                        color_continuous_scale="Reds",
                        hover_data={"total_fraud": ":,", "amount_at_risk": ":,.0f",
                                    "total_transactions": ":,"},
                        labels={"fraud_rate": VISTA_TASA},
                        title=f"{VISTA_TASA} por estado — {len(us_only)} estados US"
                    )
                    fig.update_layout(height=500)
                    st.plotly_chart(fig, use_container_width=True)

                    st.caption("Dos rankings: por tasa (proporción relativa) y por monto absoluto.")
                    col1, col2 = st.columns(2)
                    with col1:
                        top_states = us_only.nlargest(10, "fraud_rate")
                        fig = px.bar(
                            top_states, x="fraud_rate", y="state",
                            orientation="h", color="fraud_rate",
                            color_continuous_scale="Reds",
                            labels={"fraud_rate": VISTA_TASA, "state": "Estado"},
                            title=f"🥇 Top 10 estados por TASA de {VISTA_LABEL}"
                        )
                        fig.update_layout(height=400, yaxis={'categoryorder':'total ascending'})
                        st.plotly_chart(fig, use_container_width=True)
                    with col2:
                        top_amount = us_only.nlargest(10, "amount_at_risk")
                        fig = px.bar(
                            top_amount, x="amount_at_risk", y="state",
                            orientation="h", color="amount_at_risk",
                            color_continuous_scale="Reds",
                            labels={"amount_at_risk": f"{VISTA_MONTO} ($)", "state": "Estado"},
                            title=f"💰 Top 10 estados por MONTO ({VISTA_LABEL})"
                        )
                        fig.update_layout(height=400, yaxis={'categoryorder':'total ascending'})
                        st.plotly_chart(fig, use_container_width=True)

                st.divider()
                st.subheader("🌎 Transacciones internacionales")
                st.caption(f"Top 15 países por volumen de transacciones; color = % de {VISTA_LABEL} en ese país.")
                if not intl.empty:
                    top_intl = intl.nlargest(15, "total_transactions").copy()
                    top_intl["fraud_rate_pct"] = (top_intl["fraud_rate"] * 100).round(2)
                    fig = px.bar(
                        top_intl,
                        x="total_transactions", y="state",
                        orientation="h",
                        color="fraud_rate_pct",
                        color_continuous_scale="Reds",
                        range_color=[0, max(5, top_intl["fraud_rate_pct"].max())],
                        hover_data={"total_fraud": ":,",
                                    "amount_at_risk": ":,.0f"},
                        labels={"total_transactions": "Transacciones",
                                "state": "", "fraud_rate_pct": VISTA_PCT},
                        title=f"Top 15 países por volumen de transacciones (color = {VISTA_PCT})"
                    )
                    fig.update_layout(height=520, yaxis={'categoryorder':'total ascending'},
                                      margin=dict(l=10, r=10, t=60, b=40))
                    st.plotly_chart(fig, use_container_width=True)

                    paises_con_fraude = intl[intl["total_fraud"] > 0].sort_values("total_fraud", ascending=False)
                    if not paises_con_fraude.empty:
                        nombres = ", ".join(paises_con_fraude["state"].head(3).tolist())
                        st.info(
                            f"💡 De los {len(intl)} países detectados, solo "
                            f"**{len(paises_con_fraude)}** tienen fraudes etiquetados ({nombres}). "
                            f"Italia es extremo: ~45% de las transacciones marcadas como fraude "
                            f"(~1000x el promedio global de 0.04%)."
                        )
                else:
                    st.info("No se detectaron transacciones internacionales.")

        # ──────────────────────────────────────────────────────────────────
        # SUB-TAB: COMERCIOS Y TARJETAS
        # ──────────────────────────────────────────────────────────────────
        with tab_com:
            st.subheader(f"🏪 {VISTA_CAP} por categoría de comercio (MCC)")
            st.caption(f"Arriba: categorías que más dinero mueven en {VISTA_LABEL}. Abajo: categorías con mayor tasa.")
            mcc_df = query(f"""
                SELECT mcc_description, total_transactions,
                       {SEG_COUNT}  AS total_fraud,
                       {SEG_AMOUNT} AS amount_at_risk,
                       CASE WHEN total_transactions > 0
                            THEN {SEG_COUNT}::DOUBLE PRECISION / total_transactions ELSE 0 END AS fraud_rate
                FROM fraud_by_mcc
                WHERE mcc_description IS NOT NULL AND mcc_description != 'Unknown'
                  AND total_transactions > 100
            """)
            if mcc_df is not None and not mcc_df.empty:
                top_amount = mcc_df.nlargest(15, "amount_at_risk")
                fig = px.bar(
                    top_amount.sort_values("amount_at_risk"),
                    x="amount_at_risk", y="mcc_description",
                    orientation="h", color="fraud_rate",
                    color_continuous_scale="Reds",
                    hover_data={"total_transactions": ":,", "total_fraud": ":,"},
                    labels={"amount_at_risk": f"{VISTA_MONTO} ($)",
                            "mcc_description": "", "fraud_rate": "Tasa"},
                    title=f"Top 15 categorías por MONTO ({VISTA_LABEL})"
                )
                fig.update_layout(height=600, margin=dict(l=10, r=10, t=60, b=40),
                                  font=dict(size=13))
                st.plotly_chart(fig, use_container_width=True)

                top_rate = mcc_df.nlargest(15, "fraud_rate")
                top_rate["fraud_rate_pct"] = (top_rate["fraud_rate"] * 100).round(3)
                fig = px.bar(
                    top_rate.sort_values("fraud_rate"),
                    x="fraud_rate_pct", y="mcc_description",
                    orientation="h", color="fraud_rate_pct",
                    color_continuous_scale="Reds",
                    hover_data={"total_transactions": ":,", "total_fraud": ":,"},
                    labels={"fraud_rate_pct": VISTA_PCT,
                            "mcc_description": ""},
                    title=f"Top 15 categorías por TASA de {VISTA_LABEL} (%)"
                )
                fig.update_layout(height=600, margin=dict(l=10, r=10, t=60, b=40),
                                  font=dict(size=13))
                st.plotly_chart(fig, use_container_width=True)

            st.divider()

            st.subheader("💳 Análisis por tipo de tarjeta")
            st.caption(f"Cruce marca × tipo (Crédito/Débito/Prepago). Cada celda = % de {VISTA_LABEL}.")
            card_df = query(f"""
                SELECT card_brand, card_type, has_chip, total_transactions,
                       {SEG_COUNT}  AS total_fraud,
                       CASE WHEN total_transactions > 0
                            THEN {SEG_COUNT}::DOUBLE PRECISION / total_transactions ELSE 0 END AS fraud_rate
                FROM fraud_by_card_type
                WHERE card_brand IS NOT NULL
            """)
            if card_df is not None and not card_df.empty:
                CARD_TYPE_ES = {"Credit": "Crédito", "Debit": "Débito", "Debit (Prepaid)": "Débito (Prepago)", "Prepaid": "Prepago"}
                heat = card_df.groupby(["card_brand", "card_type"]).agg(
                    total=("total_transactions", "sum"),
                    fraud=("total_fraud", "sum"),
                ).reset_index()
                heat["card_type"] = heat["card_type"].map(lambda v: CARD_TYPE_ES.get(v, v))
                heat["fraud_pct"] = (100 * heat["fraud"] / heat["total"]).round(4)
                pivot = heat.pivot(index="card_brand", columns="card_type", values="fraud_pct")

                col1, col2 = st.columns([3, 2])
                with col1:
                    fig = px.imshow(
                        pivot,
                        text_auto=".3f",
                        color_continuous_scale="Reds",
                        aspect="auto",
                        labels=dict(x="Tipo", y="Marca", color=VISTA_PCT),
                        title=f"{VISTA_TASA} (%) por marca × tipo de tarjeta"
                    )
                    fig.update_layout(height=380, margin=dict(t=60, b=40))
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    by_brand = card_df.groupby("card_brand").agg(
                        total=("total_transactions", "sum"),
                        fraud=("total_fraud", "sum"),
                    ).reset_index()
                    by_brand["fraud_pct"] = (100 * by_brand["fraud"] / by_brand["total"]).round(4)
                    by_brand = by_brand.sort_values("fraud_pct", ascending=False)
                    st.caption(f"Ranking por {VISTA_TASA.lower()}")
                    st.dataframe(
                        by_brand.rename(columns={
                            "card_brand": "Marca", "total": "Transacciones",
                            "fraud": VISTA_CAP, "fraud_pct": VISTA_PCT
                        }),
                        hide_index=True, use_container_width=True,
                    )

                st.divider()
                col_chip, col_dw = st.columns(2)

                with col_chip:
                    st.subheader("🔐 Chip EMV")
                    st.caption(f"Con chip vs sin chip — ¿la autenticación criptográfica reduce las {VISTA_LABEL}?")
                    by_chip = card_df.groupby("has_chip").agg(
                        total=("total_transactions", "sum"),
                        fraud=("total_fraud", "sum"),
                    ).reset_index()
                    by_chip["fraud_pct"] = 100 * by_chip["fraud"] / by_chip["total"]
                    by_chip["has_chip"] = by_chip["has_chip"].map({True: "Con chip", False: "Sin chip"})
                    fig = px.bar(
                        by_chip, x="has_chip", y="fraud_pct",
                        text="fraud_pct",
                        color="has_chip",
                        color_discrete_map={"Con chip": "#2ca02c", "Sin chip": "#d62728"},
                        labels={"fraud_pct": VISTA_PCT, "has_chip": ""},
                        title=f"{VISTA_TASA} por presencia de chip"
                    )
                    fig.update_traces(texttemplate="%{text:.3f}%", textposition="inside",
                                      insidetextfont=dict(size=14, color="white"))
                    ymax = float(by_chip["fraud_pct"].max()) * 1.15
                    fig.update_layout(showlegend=False, height=340,
                                      margin=dict(t=60, b=40),
                                      yaxis=dict(range=[0, ymax]))
                    st.plotly_chart(fig, use_container_width=True)

                with col_dw:
                    st.subheader("🕷️ Tarjetas en dark web")
                    st.caption("¿La tarjeta apareció en filtraciones de la red oscura? Comparación de tasas.")
                    dw = query(f"""
                        SELECT card_on_dark_web,
                               COUNT(*) AS total,
                               COUNT(*) FILTER (WHERE {FRAUD_COL}) AS fraud,
                               ROUND(100.0 * COUNT(*) FILTER (WHERE {FRAUD_COL}) / COUNT(*), 4) AS fraud_pct
                        FROM transactions_processed
                        WHERE card_on_dark_web IS NOT NULL
                        GROUP BY card_on_dark_web
                    """)
                    if dw is not None and not dw.empty:
                        dw["label"] = dw["card_on_dark_web"].map({True: "Sí", False: "No"})
                        fig = px.bar(
                            dw, x="label", y="fraud_pct",
                            text="fraud_pct",
                            color="label",
                            color_discrete_map={"Sí": "#d62728", "No": "#2ca02c"},
                            labels={"fraud_pct": VISTA_PCT, "label": "En dark web"},
                            title=f"{VISTA_TASA} si la tarjeta apareció en dark web"
                        )
                        fig.update_traces(texttemplate="%{text:.3f}%", textposition="inside",
                                          insidetextfont=dict(size=14, color="white"))
                        ymax = float(dw["fraud_pct"].max()) * 1.15
                        fig.update_layout(showlegend=False, height=340,
                                          margin=dict(t=60, b=40),
                                          yaxis=dict(range=[0, ymax]))
                        st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# PANEL DE USUARIO
# ═══════════════════════════════════════════════════════════════════════════
elif pagina == "Panel de Usuario":
    st.title("👤 Panel de Perfil de Usuario")
    st.caption("Analizá el comportamiento transaccional de un cliente específico.")

    # Sugerencias de IDs útiles (los que tienen más fraude)
    sample_users = query("""
        SELECT user_id, total_transactions, total_fraud, fraud_rate
        FROM user_profiles
        WHERE total_fraud > 0
        ORDER BY total_fraud DESC
        LIMIT 5
    """)

    col1, col2 = st.columns([2, 3])
    with col1:
        user_id = st.text_input(
            "ID del usuario:",
            placeholder="Ej: 1102",
            help="Ingresá un user_id. Mirá las sugerencias a la derecha.",
        )
    with col2:
        if sample_users is not None and not sample_users.empty:
            st.caption("🔍 Usuarios con más fraudes (sugerencias):")
            sample_users["fraud_pct"] = sample_users["fraud_rate"].apply(lambda x: f"{x:.2%}")
            st.dataframe(
                sample_users[["user_id", "total_transactions", "total_fraud", "fraud_pct"]]
                    .rename(columns={
                        "user_id": "ID (identificador)",
                        "total_transactions": "Total Transacciones",
                        "total_fraud": "Fraudes",
                        "fraud_pct": "% Fraude",
                    }),
                hide_index=True,
                use_container_width=True,
            )

    if not user_id:
        st.info("Ingresá un ID de usuario o copiá uno de las sugerencias.")
        st.stop()

    perfil = query(
        "SELECT * FROM user_profiles WHERE user_id = :user_id",
        {"user_id": str(user_id)},
    )
    if perfil is None or perfil.empty:
        st.error(f"No se encontró el usuario con ID: {user_id}")
        st.stop()
    p = perfil.iloc[0]

    st.divider()
    tab_perfil, tab_compor, tab_hist = st.tabs(["👤 Perfil", "📊 Comportamiento", "🧾 Historial"])

    # ── TAB: PERFIL ──
    with tab_perfil:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Edad", f"{int(p['age'])} años" if pd.notna(p["age"]) else "—")
        col2.metric("Género", p["gender"] if pd.notna(p["gender"]) else "—")
        col3.metric("Puntaje crediticio (credit score)",
                    f"{int(p['credit_score'])}" if pd.notna(p["credit_score"]) else "—")
        col4.metric("Tarjetas", f"{int(p['num_cards'])}" if pd.notna(p["num_cards"]) else "—")

        col1, col2, col3 = st.columns(3)
        if pd.notna(p["yearly_income"]):
            col1.metric("Ingreso anual", f"${p['yearly_income']:,.0f}")
        if pd.notna(p["total_debt"]):
            col2.metric("Deuda total", f"${p['total_debt']:,.0f}")
        if pd.notna(p["debt_income_ratio"]):
            ratio = float(p["debt_income_ratio"])
            col3.metric(
                "Cociente deuda/ingreso (ratio)", f"{ratio:.2f}",
                delta=f"{'⚠️ Alto' if ratio > 3 else 'OK (correcto)'}",
                delta_color="inverse" if ratio > 3 else "normal",
                help="Cociente > 3 indica endeudamiento alto. (Ya no marca sospecha por sí solo: "
                     "se comprobó que casi no correlaciona con el fraude real y fue quitado del puntaje.)",
            )

        st.divider()
        col1, col2, col3 = st.columns(3)
        col1.metric("Transacciones totales", f"{int(p['total_transactions']):,}")
        col2.metric("Fraudes detectados",
                    f"{int(p['total_fraud']):,}",
                    f"{p['fraud_rate']:.2%}")
        col3.metric("Monto total gastado",
                    f"${p['total_spent']:,.0f}" if pd.notna(p["total_spent"]) else "—")

    # ── TAB: COMPORTAMIENTO ──
    with tab_compor:
        # Gasto mensual
        mensual = query("""
            SELECT DATE_TRUNC('month', transaction_date) AS month,
                   SUM(amount) AS total_amount,
                   COUNT(*) AS total_txns,
                   COUNT(*) FILTER (WHERE is_fraud) AS fraud
            FROM transactions_processed
            WHERE user_id = :user_id AND transaction_date IS NOT NULL
            GROUP BY month ORDER BY month
        """, {"user_id": str(user_id)})

        if mensual is not None and not mensual.empty:
            st.subheader("💸 Gasto mensual")
            st.caption(
                "Evolución del gasto total del usuario mes a mes. Picos repentinos "
                "(gastos atípicos) pueden ser indicio de actividad fraudulenta o de "
                "consumo inusual que merece revisión."
            )
            fig = px.area(
                mensual, x="month", y="total_amount",
                labels={"month": "Mes", "total_amount": "Monto ($)"},
                title="Gasto total por mes"
            )
            fig.update_traces(line_color="#1f77b4", fillcolor="rgba(31,119,180,0.3)")
            fig.update_layout(height=320)
            st.plotly_chart(fig, use_container_width=True)

        col1, col2 = st.columns(2)

        # Distribución de montos del usuario
        montos_user = query("""
            SELECT amount, is_fraud
            FROM transactions_processed
            WHERE user_id = :user_id AND amount > 0 AND amount < 2000
        """, {"user_id": str(user_id)})
        if montos_user is not None and not montos_user.empty:
            with col1:
                montos_user["tipo"] = montos_user["is_fraud"].map({True: "Fraude", False: "Legítima"})
                fig = px.histogram(
                    montos_user, x="amount", color="tipo",
                    nbins=30, barmode="overlay", opacity=0.6,
                    color_discrete_map={"Fraude": "#d62728", "Legítima": "#2ca02c"},
                    labels={"amount": "Monto ($)"},
                    title="Distribución de montos del usuario"
                )
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)
                st.caption(
                    "¿Los fraudes de este usuario son por montos típicos o atípicos? "
                    "Si las distribuciones se parecen, el atacante probablemente intentó "
                    "pasar desapercibido imitando el patrón habitual del titular."
                )

        # Mix por categoría MCC
        mix_mcc = query("""
            SELECT mcc_description, COUNT(*) AS txns,
                   SUM(amount) AS gasto,
                   COUNT(*) FILTER (WHERE is_fraud) AS fraud
            FROM transactions_processed
            WHERE user_id = :user_id AND mcc_description IS NOT NULL
            GROUP BY mcc_description
            ORDER BY gasto DESC
            LIMIT 10
        """, {"user_id": str(user_id)})
        if mix_mcc is not None and not mix_mcc.empty:
            with col2:
                fig = px.bar(
                    mix_mcc.sort_values("gasto"),
                    x="gasto", y="mcc_description",
                    orientation="h", color="fraud",
                    color_continuous_scale="Reds",
                    labels={"gasto": "Gasto total ($)", "mcc_description": "Categoría",
                            "fraud": "Fraudes"},
                    title="Top 10 categorías donde gasta"
                )
                fig.update_layout(height=350, yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig, use_container_width=True)
                st.caption(
                    "En qué rubros gasta más el usuario. El color indica si esas categorías "
                    "tuvieron fraudes (más oscuro = más fraudes en esa categoría)."
                )

        # ── RF13: mapa geográfico de las transacciones del usuario ──
        st.divider()
        st.subheader("🗺️ Mapa de transacciones del usuario")
        st.caption(
            "Distribución geográfica de las transacciones presenciales del usuario "
            "por estado de EEUU (las Online no tienen ubicación). Color = cantidad "
            "de transacciones; útil para ver si opera lejos de su zona habitual."
        )
        geo_user = query("""
            SELECT merchant_state AS state,
                   COUNT(*) AS txns,
                   COUNT(*) FILTER (WHERE is_fraud) AS fraudes
            FROM transactions_processed
            WHERE user_id = :user_id
              AND merchant_state IS NOT NULL AND merchant_state != ''
              AND LENGTH(merchant_state) = 2
            GROUP BY merchant_state
        """, {"user_id": str(user_id)})
        if geo_user is not None and not geo_user.empty:
            geo_us = geo_user[geo_user["state"].isin(US_STATES)]
            if not geo_us.empty:
                fig = px.choropleth(
                    geo_us, locations="state", locationmode="USA-states",
                    color="txns", scope="usa", color_continuous_scale="Blues",
                    hover_data={"fraudes": ":,"},
                    labels={"txns": "Transacciones"},
                    title="Transacciones del usuario por estado",
                )
                fig.update_layout(height=420, margin=dict(t=50, b=10))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("El usuario no tiene transacciones presenciales en estados de EEUU (posiblemente solo opera Online).")
        else:
            st.info("Sin datos geográficos para este usuario (puede operar solo Online).")

    # ── TAB: HISTORIAL ──
    with tab_hist:
        st.subheader("Historial de transacciones (últimas 100)")
        st.markdown(
            "Listado cronológico (más recientes primero) con coloreado por estado: "
            "🟥 **rojo** = fraude confirmado (`is_fraud = True` según el label oficial); "
            "🟨 **amarillo** = sospechosa según las reglas del pipeline pero no confirmada como fraude; "
            "⬜ **sin color** = transacción legítima. "
            "La columna *suspicion_reasons* indica qué reglas dispararon la marca."
        )
        txns = query(
            """
            SELECT transaction_date, amount, merchant_city, merchant_state,
                   mcc_description, transaction_type, is_fraud, is_suspicious,
                   suspicion_reasons
            FROM transactions_processed
            WHERE user_id = :user_id
            ORDER BY transaction_date DESC
            LIMIT 100
            """,
            {"user_id": str(user_id)},
        )
        if txns is not None and not txns.empty:
            # Coloreado: filas de fraude en rojo
            def highlight_fraud(row):
                if row["is_fraud"]:
                    return ["background-color: rgba(214,39,40,0.2)"] * len(row)
                if row["is_suspicious"]:
                    return ["background-color: rgba(255,193,7,0.2)"] * len(row)
                return [""] * len(row)

            st.dataframe(
                txns.style.apply(highlight_fraud, axis=1),
                use_container_width=True,
                height=500,
            )


# ═══════════════════════════════════════════════════════════════════════════
# PANEL DE COMERCIOS
# ═══════════════════════════════════════════════════════════════════════════
elif pagina == "Panel de Comercios":
    st.title("🏪 Panel de Comercios")
    st.markdown(
        "Responde dos preguntas clave: **¿qué comercios concentran más fraude?** "
        "y **¿qué tipo de negocios son los más afectados?**"
    )

    # ─────────────────────────────────────────────────────────────────────────
    # KPIs — resumen del universo de comercios
    # ─────────────────────────────────────────────────────────────────────────
    kpis = query("""
        SELECT
            COUNT(*)                                  AS total_merchants,
            COUNT(*) FILTER (WHERE total_fraud > 0)   AS merchants_con_fraude,
            COALESCE(SUM(amount_at_risk), 0)          AS riesgo_total,
            COALESCE(SUM(total_fraud), 0)             AS fraudes_totales
        FROM fraud_by_merchant
        WHERE merchant_id IS NOT NULL AND merchant_id != ''
    """)
    concentration = query("""
        WITH ranked AS (
            SELECT amount_at_risk,
                   ROW_NUMBER() OVER (ORDER BY amount_at_risk DESC) AS rk,
                   SUM(amount_at_risk) OVER ()                       AS total_risk
            FROM fraud_by_merchant
            WHERE amount_at_risk > 0
        )
        SELECT ROUND(100.0 * SUM(amount_at_risk) FILTER (WHERE rk <= 10) /
                     NULLIF(MAX(total_risk), 0), 1) AS pct_top10
        FROM ranked
    """)

    if kpis is not None and not kpis.empty:
        k = kpis.iloc[0]
        pct_con_fraude = (100 * k["merchants_con_fraude"] / k["total_merchants"]) \
            if k["total_merchants"] > 0 else 0
        pct_top10 = concentration.iloc[0]["pct_top10"] if concentration is not None and not concentration.empty else 0

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Comercios totales", f"{int(k['total_merchants']):,}",
                  help="Cantidad de comercios (merchants) distintos detectados en el conjunto de datos (dataset).")
        c2.metric("Con fraude registrado",
                  f"{int(k['merchants_con_fraude']):,}",
                  f"{pct_con_fraude:.1f}% del total",
                  help="Comercios que tienen al menos 1 transacción fraudulenta.")
        c3.metric("Concentración top 10 (los 10 más expuestos)",
                  f"{(pct_top10 or 0):.1f}%",
                  help="Porcentaje del monto total en riesgo que se concentra en solo los 10 "
                       "comercios más expuestos. Un valor alto indica que el fraude se concentra "
                       "en pocos comercios (atacar esos pocos resuelve la mayor parte del problema).")
        c4.metric("Riesgo total", f"${k['riesgo_total']:,.0f}",
                  help="Suma de los montos de todas las transacciones marcadas como fraude.")

    st.divider()

    # ─────────────────────────────────────────────────────────────────────────
    # 1) TOP COMERCIOS — la pregunta principal
    # ─────────────────────────────────────────────────────────────────────────
    st.header("1. Comercios con mayor exposición")
    st.markdown(
        "Cada barra es un comercio. **El largo** indica el monto total perdido en fraudes "
        "para ese comercio; **el color** indica qué porcentaje de sus transacciones fueron "
        "fraude. Un comercio largo y oscuro es el peor escenario: mueve mucho fraude y "
        "tiene tasa alta."
    )

    top_n = st.slider("¿Cuántos comercios mostrar?", 5, 30, 15)
    top_n = int(top_n)

    top_data = query("""
        SELECT merchant_id, merchant_city, merchant_state, mcc_description,
               total_transactions, total_fraud, fraud_rate, amount_at_risk
        FROM fraud_by_merchant
        WHERE merchant_id IS NOT NULL AND merchant_id != ''
              AND total_fraud > 0
        ORDER BY COALESCE(NULLIF(amount_at_risk, 0), total_fraud) DESC
        LIMIT :top_n
    """, {"top_n": top_n})

    if top_data is not None and not top_data.empty:
        top_data["fraud_rate_pct"] = (top_data["fraud_rate"] * 100).round(2)
        top_data["label"] = (
            "Comercio " + top_data["merchant_id"].astype(str)
            + " (" + top_data["merchant_city"].fillna("?").astype(str)
            + ", " + top_data["merchant_state"].fillna("?").astype(str) + ")"
        )

        # Si el pipeline ya está actualizado, usamos monto. Si no, fallback a cantidad.
        usar_monto = top_data["amount_at_risk"].sum() > 0
        x_col = "amount_at_risk" if usar_monto else "total_fraud"
        x_label = "Monto perdido en fraude (USD / dólares)" if usar_monto else "Cantidad de fraudes"

        if not usar_monto:
            st.warning(
                "ℹ️ El monto exacto perdido no está disponible (requiere reejecutar "
                "`compute_metrics` (cálculo de métricas) y `load_to_dwh` (carga al almacén) en Airflow). Mientras tanto, "
                "se muestra la **cantidad de fraudes** por comercio."
            )

        fig = px.bar(
            top_data.sort_values(x_col),
            x=x_col, y="label",
            orientation="h",
            color="fraud_rate_pct",
            color_continuous_scale="Reds",
            hover_data={
                "total_transactions": ":,",
                "total_fraud": ":,",
                "mcc_description": True,
                "label": False,
                "amount_at_risk": ":,.0f",
            },
            labels={
                x_col: x_label,
                "label": "",
                "fraud_rate_pct": "% Fraude",
            },
        )
        fig.update_layout(
            height=max(420, top_n * 32),
            margin=dict(l=10, r=10, t=20, b=40),
            font=dict(size=13),
        )
        st.plotly_chart(fig, use_container_width=True)

        if usar_monto:
            st.caption(
                f"💡 **Lectura**: los {top_n} comercios mostrados representan **"
                f"${top_data['amount_at_risk'].sum():,.0f}** del fraude total. "
                "Pasá el mouse sobre cada barra para ver el detalle."
            )
        else:
            st.caption(
                f"💡 **Lectura**: los {top_n} comercios mostrados concentran **"
                f"{int(top_data['total_fraud'].sum()):,}** fraudes. "
                "Pasá el mouse sobre cada barra para ver el detalle."
            )

    st.divider()

    # ─────────────────────────────────────────────────────────────────────────
    # 2) FRAUDE POR TIPO DE NEGOCIO (CATEGORÍA / MCC)
    # ─────────────────────────────────────────────────────────────────────────
    st.header("2. Tipos de negocio más afectados")
    st.markdown(
        "Agrupando los comercios por categoría (MCC = Merchant Category Code), "
        "¿qué tipo de negocios concentran el mayor monto de fraude?"
    )

    # Acá usamos fraud_by_mcc (que SÍ tiene amount_at_risk correcto, ya estaba bien en el pipeline).
    cat_data = query("""
        SELECT mcc_description,
               total_transactions AS txns,
               total_fraud        AS fraud,
               fraud_rate,
               amount_at_risk
        FROM fraud_by_mcc
        WHERE mcc_description IS NOT NULL AND mcc_description != 'Unknown'
              AND total_fraud > 0
        ORDER BY amount_at_risk DESC NULLS LAST, total_fraud DESC
        LIMIT 10
    """)

    if cat_data is not None and not cat_data.empty:
        cat_data["fraud_pct"] = (cat_data["fraud_rate"] * 100).round(3)
        usar_monto = cat_data["amount_at_risk"].sum() > 0
        x_col = "amount_at_risk" if usar_monto else "fraud"
        x_label = "Monto perdido en fraude (USD / dólares)" if usar_monto else "Cantidad de fraudes"

        fig = px.bar(
            cat_data.sort_values(x_col),
            x=x_col, y="mcc_description",
            orientation="h",
            color="fraud_pct",
            color_continuous_scale="Reds",
            hover_data={
                "txns": ":,",
                "fraud": ":,",
                "amount_at_risk": ":,.0f",
            },
            labels={
                x_col: x_label,
                "mcc_description": "",
                "fraud_pct": "% Fraude",
            },
        )
        fig.update_layout(
            height=480,
            margin=dict(l=10, r=10, t=20, b=40),
            font=dict(size=13),
        )
        st.plotly_chart(fig, use_container_width=True)

        st.caption(
            "💡 **Lectura**: las categorías con barra larga **y** color oscuro "
            "(mayor tasa de fraude) son las más críticas — combinan volumen alto con "
            "alta probabilidad de fraude en cada transacción."
        )


# ═══════════════════════════════════════════════════════════════════════════
# EXPLORADOR CON FILTROS (RF16)
# ═══════════════════════════════════════════════════════════════════════════
elif pagina == "Explorador con Filtros":
    st.title("🔎 Explorador de transacciones")
    st.caption(
        "Aplicá filtros combinados sobre el detalle de transacciones: usuario, "
        "tarjeta, tipo, categoría de comercio (MCC), estado y rango de fechas. "
        "Los filtros se aplican sobre la tabla `transactions_processed`."
    )

    # ── Rango de fechas disponible (para los defaults del filtro) ──
    # Lo tomamos de fraud_metrics_global (dataset_start/end_date), evitando un
    # MIN/MAX sobre las 12.6M filas.
    rango = query("""
        SELECT dataset_start_date AS dmin, dataset_end_date AS dmax
        FROM fraud_metrics_global ORDER BY computed_at DESC LIMIT 1
    """)
    import datetime as _dt
    if rango is not None and not rango.empty and rango.iloc[0]["dmin"] is not None:
        dmin = rango.iloc[0]["dmin"]
        dmax = rango.iloc[0]["dmax"]
    else:
        dmin, dmax = _dt.date(2010, 1, 1), _dt.date(2019, 12, 31)

    # ── Opciones para los selectores ──
    mcc_opts = query("""
        SELECT DISTINCT mcc_description FROM fraud_by_mcc
        WHERE mcc_description IS NOT NULL AND mcc_description != 'Unknown'
        ORDER BY mcc_description
    """)
    mcc_list = ["(todas)"] + (mcc_opts["mcc_description"].tolist() if mcc_opts is not None and not mcc_opts.empty else [])
    estados_list = ["(todos)"] + sorted(US_STATES)

    # ── Widgets de filtro ──
    st.subheader("Filtros")
    c1, c2, c3 = st.columns(3)
    with c1:
        f_user = st.text_input("ID de usuario", placeholder="ej: 1102").strip()
        f_card = st.text_input("ID de tarjeta", placeholder="ej: 2972").strip()
    with c2:
        f_tipo = st.selectbox("Tipo de transacción", ["(todas)", "Online", "Swipe"])
        f_mcc = st.selectbox("Categoría (MCC)", mcc_list)
    with c3:
        f_estado = st.selectbox("Estado (EEUU)", estados_list)
        f_marca = st.selectbox("Marca de tarjeta", ["(todas)", "Visa", "Mastercard", "Amex", "Discover"])

    f_fechas = st.date_input(
        "Rango de fechas", value=(dmin, dmax), min_value=dmin, max_value=dmax,
    )
    solo_fraude = st.checkbox("Mostrar solo transacciones fraudulentas (is_fraud)")
    solo_sosp = st.checkbox("Mostrar solo sospechosas según las reglas (is_suspicious)")

    # ── Construcción dinámica del WHERE con bind params ──
    conds, params = [], {}
    if f_user:
        conds.append("user_id = :user"); params["user"] = f_user
    if f_card:
        conds.append("card_id = :card"); params["card"] = f_card
    if f_tipo != "(todas)":
        conds.append("transaction_type = :tipo"); params["tipo"] = f_tipo
    if f_mcc != "(todas)":
        conds.append("mcc_description = :mcc"); params["mcc"] = f_mcc
    if f_estado != "(todos)":
        conds.append("merchant_state = :estado"); params["estado"] = f_estado
    if f_marca != "(todas)":
        conds.append("card_brand = :marca"); params["marca"] = f_marca
    if isinstance(f_fechas, (tuple, list)) and len(f_fechas) == 2:
        conds.append("transaction_date >= :d1 AND transaction_date < (:d2::date + 1)")
        params["d1"] = str(f_fechas[0]); params["d2"] = str(f_fechas[1])
    if solo_fraude:
        conds.append("is_fraud = TRUE")
    if solo_sosp:
        conds.append("is_suspicious = TRUE")

    where_sql = ("WHERE " + " AND ".join(conds)) if conds else ""

    if not conds:
        st.info(
            "Aplicá al menos un filtro para explorar. Sin filtros, la consulta "
            "recorrería las 12.6M de transacciones y sería lenta."
        )
        st.stop()

    # ── Métricas del subconjunto filtrado ──
    resumen = query(f"""
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE is_fraud) AS fraude,
               COUNT(*) FILTER (WHERE is_suspicious) AS sospechosas,
               COALESCE(SUM(amount), 0) AS monto_total,
               COALESCE(SUM(amount) FILTER (WHERE is_fraud), 0) AS monto_fraude
        FROM transactions_processed
        {where_sql}
    """, params)

    if resumen is not None and not resumen.empty:
        r = resumen.iloc[0]
        st.divider()
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Transacciones", f"{int(r['total']):,}")
        m2.metric("Fraudes (reales)", f"{int(r['fraude']):,}")
        m3.metric("Sospechosas (reglas)", f"{int(r['sospechosas']):,}")
        m4.metric("Monto en riesgo", f"${float(r['monto_fraude']):,.0f}")

    # ── Tabla de detalle (primeras 500 filas) ──
    st.subheader("Detalle (primeras 500 transacciones)")
    detalle = query(f"""
        SELECT transaction_id, transaction_date, user_id, card_id, amount,
               transaction_type, merchant_city, merchant_state, mcc_description,
               card_brand, distance_km, is_fraud, is_suspicious, fraud_score,
               suspicion_reasons
        FROM transactions_processed
        {where_sql}
        ORDER BY transaction_date DESC
        LIMIT 500
    """, params)
    if detalle is not None and not detalle.empty:
        st.dataframe(detalle, use_container_width=True, height=460, hide_index=True)
    else:
        st.warning("No hay transacciones que cumplan los filtros seleccionados.")


# ═══════════════════════════════════════════════════════════════════════════
# ANÁLISIS DE DATASETS
# ═══════════════════════════════════════════════════════════════════════════
elif pagina == "Análisis de Datasets":
    st.title("📂 Análisis de Datasets (conjuntos de datos)")
    st.markdown(
        "Esta sección describe **los 5 archivos de entrada** que alimentan el pipeline (flujo), "
        "su composición, distribuciones clave y rol en el sistema. Cada dataset (conjunto de datos) aporta "
        "una pieza del rompecabezas: transacciones (los hechos), usuarios y tarjetas "
        "(las dimensiones), labels (etiquetas: la verdad sobre el fraude) y MCC (catálogo de "
        "categorías de comercio)."
    )

    # ── Resumen general de los datasets ──
    st.divider()
    st.header("📋 Resumen de los datasets (conjuntos de datos)")
    st.markdown(
        "Los archivos crudos viven en `./data/` y se procesan en el DAG (grafo del flujo de tareas) de Airflow. "
        "Cada uno tiene un rol específico en el modelo dimensional del DWH (Data Warehouse / almacén de datos)."
    )

    datasets_info = pd.DataFrame([
        {"Archivo": "transactions_data.csv",   "Formato": "CSV",  "Volumen aprox.": "13.3M (millones) filas (1.2 GB)",
         "Rol": "Hechos: una fila por transacción", "Clave": "transaction_id (id transacción)"},
        {"Archivo": "users_data.csv",          "Formato": "CSV",  "Volumen aprox.": "2,000 filas",
         "Rol": "Dimensión: titulares de tarjeta", "Clave": "user_id (id usuario)"},
        {"Archivo": "cards_data.csv",          "Formato": "CSV",  "Volumen aprox.": "6,146 filas",
         "Rol": "Dimensión: tarjetas emitidas", "Clave": "card_id (id tarjeta)"},
        {"Archivo": "train_fraud_labels.json", "Formato": "JSON", "Volumen aprox.": "8.9M registros",
         "Rol": "Ground truth (verdad de referencia): ¿fue fraude o no?", "Clave": "transaction_id → Yes/No (Sí/No)"},
        {"Archivo": "mcc_codes.json",          "Formato": "JSON", "Volumen aprox.": "~900 entradas",
         "Rol": "Catálogo: descripción de categorías", "Clave": "mcc → descripción"},
    ])
    st.dataframe(datasets_info, hide_index=True, use_container_width=True)

    # ─────────────────────────────────────────────────────────────────────────
    # 1) TRANSACTIONS
    # ─────────────────────────────────────────────────────────────────────────
    st.divider()
    st.header("1️⃣ Transactions (Transacciones) — la tabla de hechos")
    st.markdown(
        "El archivo más grande del proyecto. Cada fila es una transacción con tarjeta. "
        "Es la base sobre la que se construye todo el análisis."
    )

    txn_resumen = query("""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT user_id) AS usuarios_unicos,
            COUNT(DISTINCT card_id) AS tarjetas_unicas,
            COUNT(DISTINCT merchant_id) AS comercios_unicos,
            MIN(transaction_date) AS desde,
            MAX(transaction_date) AS hasta,
            ROUND(AVG(amount)::numeric, 2) AS monto_promedio,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount)::numeric, 2) AS monto_mediana
        FROM transactions_processed
    """)
    if txn_resumen is not None and not txn_resumen.empty:
        r = txn_resumen.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Transacciones totales", f"{int(r['total']):,}")
        c2.metric("Usuarios únicos", f"{int(r['usuarios_unicos']):,}")
        c3.metric("Tarjetas únicas", f"{int(r['tarjetas_unicas']):,}")
        c4.metric("Comercios únicos", f"{int(r['comercios_unicos']):,}")

        c1, c2, c3 = st.columns(3)
        c1.metric("Rango temporal", f"{r['desde'].year} → {r['hasta'].year}",
                  help=f"De {r['desde'].date()} a {r['hasta'].date()}")
        c2.metric("Monto promedio", f"${float(r['monto_promedio']):.2f}")
        c3.metric("Monto mediana", f"${float(r['monto_mediana']):.2f}",
                  help="La mediana suele ser mucho menor que el promedio porque hay "
                       "transacciones de monto muy alto que tiran del promedio hacia arriba.")

    st.subheader("Volumen de transacciones por año")
    st.caption("Distribución temporal: ¿hay años con mucho más o menos volumen?")
    volumen_anual = query("""
        SELECT EXTRACT(YEAR FROM transaction_date)::INT AS anio,
               COUNT(*) AS transacciones
        FROM transactions_processed
        WHERE transaction_date IS NOT NULL
        GROUP BY anio ORDER BY anio
    """)
    if volumen_anual is not None and not volumen_anual.empty:
        fig = px.bar(
            volumen_anual, x="anio", y="transacciones",
            color_discrete_sequence=["#1f77b4"],
            labels={"anio": "Año", "transacciones": "Cantidad de transacciones"},
        )
        fig.update_layout(height=350, xaxis=dict(dtick=1),
                          margin=dict(t=20, b=40, l=10, r=10))
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Canal: Online vs Presencial")
    st.caption(
        "El campo `use_chip` se transforma en `transaction_type`: si dice "
        "'Online Transaction' es online, si no es presencial (swipe o chip)."
    )
    canal = query("""
        SELECT transaction_type, COUNT(*) AS total
        FROM transactions_processed
        WHERE transaction_type IS NOT NULL
        GROUP BY transaction_type ORDER BY total DESC
    """)
    if canal is not None and not canal.empty:
        canal["pct"] = (100 * canal["total"] / canal["total"].sum()).round(2)
        fig = px.bar(
            canal, x="transaction_type", y="total",
            text="pct",
            color="transaction_type",
            color_discrete_map={"Online": "#d62728", "Swipe": "#2ca02c"},
            labels={"transaction_type": "Canal", "total": "Cantidad"},
        )
        fig.update_traces(texttemplate="%{text:.1f}%", textposition="inside",
                          insidetextfont=dict(size=14, color="white"))
        fig.update_layout(showlegend=False, height=320,
                          margin=dict(t=20, b=40, l=10, r=10))
        st.plotly_chart(fig, use_container_width=True)

    # ─────────────────────────────────────────────────────────────────────────
    # 2) USERS
    # ─────────────────────────────────────────────────────────────────────────
    st.divider()
    st.header("2️⃣ Users (Usuarios) — perfiles demográficos")
    st.markdown(
        "Cada usuario es un titular de tarjeta. El dataset (conjunto de datos) incluye edad, género, "
        "ingreso anual, deuda total y puntaje crediticio (credit score). Estas variables permiten "
        "perfilar al cliente y construir reglas como `debt_income_ratio > 3` (cociente deuda/ingreso > 3)."
    )

    users_resumen = query("""
        SELECT
            COUNT(*) AS total_usuarios,
            ROUND(AVG(age)::numeric, 1) AS edad_promedio,
            ROUND(AVG(yearly_income)::numeric, 0) AS ingreso_promedio,
            ROUND(AVG(credit_score)::numeric, 0) AS score_promedio,
            ROUND(AVG(debt_income_ratio)::numeric, 2) AS dir_promedio
        FROM user_profiles
    """)
    if users_resumen is not None and not users_resumen.empty:
        r = users_resumen.iloc[0]
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Usuarios", f"{int(r['total_usuarios']):,}")
        c2.metric("Edad promedio", f"{float(r['edad_promedio']):.1f}")
        c3.metric("Ingreso anual prom. (promedio)", f"${float(r['ingreso_promedio']):,.0f}")
        c4.metric("Puntaje crediticio prom. (credit score)", f"{float(r['score_promedio']):.0f}")
        c5.metric("Cociente deuda/ingreso (ratio)", f"{float(r['dir_promedio']):.2f}")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Distribución de edad")
        st.caption("¿Cómo se distribuyen las edades de los titulares?")
        edades = query("SELECT age FROM user_profiles WHERE age IS NOT NULL")
        if edades is not None and not edades.empty:
            fig = px.histogram(
                edades, x="age", nbins=30,
                color_discrete_sequence=["#1f77b4"],
                labels={"age": "Edad", "count": "Usuarios"},
            )
            fig.update_layout(height=320, margin=dict(t=20, b=40, l=10, r=10),
                              showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Distribución por género")
        genero = query("""
            SELECT gender, COUNT(*) AS total
            FROM user_profiles
            WHERE gender IS NOT NULL
            GROUP BY gender ORDER BY total DESC
        """)
        if genero is not None and not genero.empty:
            genero["pct"] = (100 * genero["total"] / genero["total"].sum()).round(1)
            fig = px.bar(
                genero, x="gender", y="total",
                text="pct",
                color="gender",
                color_discrete_map={"Male": "#1f77b4", "Female": "#ff7f0e"},
                labels={"gender": "Género", "total": "Usuarios"},
            )
            fig.update_traces(texttemplate="%{text}%", textposition="inside",
                              insidetextfont=dict(size=14, color="white"))
            fig.update_layout(height=320, margin=dict(t=20, b=40, l=10, r=10),
                              showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Distribución de ingresos anuales")
        st.caption("Muestra la diversidad económica de los titulares.")
        ingresos = query("""
            SELECT yearly_income FROM user_profiles
            WHERE yearly_income IS NOT NULL AND yearly_income BETWEEN 0 AND 300000
        """)
        if ingresos is not None and not ingresos.empty:
            fig = px.histogram(
                ingresos, x="yearly_income", nbins=40,
                color_discrete_sequence=["#2ca02c"],
                labels={"yearly_income": "Ingreso anual ($)", "count": "Usuarios"},
            )
            fig.update_layout(height=320, margin=dict(t=20, b=40, l=10, r=10))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Distribución de puntaje crediticio (credit score)")
        st.caption(
            "Puntaje crediticio FICO (Fair Isaac Corporation, estándar de evaluación crediticia en EEUU): 300 (peor) – 850 (mejor). "
            "Mayoría de puntajes entre 600 y 800 indica usuarios financieramente saludables."
        )
        scores = query("""
            SELECT credit_score FROM user_profiles
            WHERE credit_score IS NOT NULL
        """)
        if scores is not None and not scores.empty:
            fig = px.histogram(
                scores, x="credit_score", nbins=30,
                color_discrete_sequence=["#9467bd"],
                labels={"credit_score": "Puntaje crediticio (credit score)", "count": "Usuarios"},
            )
            fig.update_layout(height=320, margin=dict(t=20, b=40, l=10, r=10))
            st.plotly_chart(fig, use_container_width=True)

    # ─────────────────────────────────────────────────────────────────────────
    # 3) CARDS
    # ─────────────────────────────────────────────────────────────────────────
    st.divider()
    st.header("3️⃣ Cards (Tarjetas) — tarjetas emitidas")
    st.markdown(
        "Cada fila es una tarjeta física emitida. Los usuarios pueden tener varias. "
        "Atributos clave: marca, tipo (Crédito/Débito/Prepago), si tiene chip EMV (Europay-Mastercard-Visa, estándar de chip), "
        "y si apareció en filtraciones de la dark web (red oscura, sitios no indexados)."
    )

    cards_resumen = query("""
        SELECT
            COUNT(DISTINCT card_id) AS total_cards,
            COUNT(DISTINCT card_brand) AS marcas,
            COUNT(DISTINCT card_type) AS tipos,
            COUNT(*) FILTER (WHERE has_chip = TRUE) AS con_chip,
            COUNT(*) FILTER (WHERE card_on_dark_web = TRUE) AS en_dark_web
        FROM transactions_processed
        WHERE card_id IS NOT NULL
    """)
    if cards_resumen is not None and not cards_resumen.empty:
        r = cards_resumen.iloc[0]
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Tarjetas únicas", f"{int(r['total_cards']):,}")
        c2.metric("Marcas", f"{int(r['marcas'])}")
        c3.metric("Tipos", f"{int(r['tipos'])}")
        c4.metric("Transacciones con chip", f"{int(r['con_chip']):,}",
                  help="Cantidad de transacciones realizadas con tarjetas que tienen chip EMV (estándar de chip).")
        c5.metric("Transacciones con tarjeta en dark web (red oscura)", f"{int(r['en_dark_web']):,}",
                  help="Transacciones con tarjetas comprometidas (número filtrado en la red oscura).")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Distribución por marca")
        st.caption("¿Qué marcas dominan? Visa y Mastercard suelen liderar.")
        marcas = query("""
            SELECT card_brand, SUM(total_transactions) AS txns
            FROM fraud_by_card_type
            WHERE card_brand IS NOT NULL
            GROUP BY card_brand ORDER BY txns DESC
        """)
        if marcas is not None and not marcas.empty:
            marcas["pct"] = (100 * marcas["txns"] / marcas["txns"].sum()).round(1)
            fig = px.bar(
                marcas.sort_values("txns"), x="txns", y="card_brand",
                orientation="h",
                text="pct",
                color="card_brand",
                labels={"txns": "Transacciones", "card_brand": ""},
            )
            fig.update_traces(texttemplate="%{text}%", textposition="inside",
                              insidetextfont=dict(size=14, color="white"))
            fig.update_layout(height=320, margin=dict(t=20, b=40, l=10, r=10),
                              showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Distribución por tipo")
        st.caption("Crédito / Débito / Prepago.")
        tipos = query("""
            SELECT card_type, SUM(total_transactions) AS txns
            FROM fraud_by_card_type
            WHERE card_type IS NOT NULL
            GROUP BY card_type ORDER BY txns DESC
        """)
        if tipos is not None and not tipos.empty:
            CARD_TYPE_ES = {"Credit": "Crédito", "Debit": "Débito", "Debit (Prepaid)": "Débito (Prepago)", "Prepaid": "Prepago"}
            tipos["card_type"] = tipos["card_type"].map(lambda v: CARD_TYPE_ES.get(v, v))
            tipos["pct"] = (100 * tipos["txns"] / tipos["txns"].sum()).round(1)
            fig = px.bar(
                tipos.sort_values("txns"), x="txns", y="card_type",
                orientation="h",
                text="pct",
                color="card_type",
                labels={"txns": "Transacciones", "card_type": ""},
            )
            fig.update_traces(texttemplate="%{text}%", textposition="inside",
                              insidetextfont=dict(size=14, color="white"))
            fig.update_layout(height=320, margin=dict(t=20, b=40, l=10, r=10),
                              showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    # ─────────────────────────────────────────────────────────────────────────
    # 4) LABELS
    # ─────────────────────────────────────────────────────────────────────────
    st.divider()
    st.header("4️⃣ Fraud Labels (Etiquetas de Fraude) — la verdad sobre cada transacción")
    st.markdown(
        "El archivo `train_fraud_labels.json` contiene, para cada `transaction_id` (id de transacción), "
        "si esa transacción fue fraude o no según un proceso de etiquetado externo "
        "(banco, equipo de seguridad, investigación post-mortem / después del hecho). "
        "Es el **ground truth (verdad de referencia)** que permite evaluar las reglas y entrenar modelos."
    )

    label_resumen = query("""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE is_fraud = TRUE) AS fraudes,
            COUNT(*) FILTER (WHERE is_fraud = FALSE) AS legitimas,
            ROUND(100.0 * COUNT(*) FILTER (WHERE is_fraud = TRUE) / NULLIF(COUNT(*), 0), 4) AS pct_fraude
        FROM transactions_processed
    """)
    if label_resumen is not None and not label_resumen.empty:
        r = label_resumen.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Etiquetas totales", f"{int(r['total']):,}")
        c2.metric("Marcadas como FRAUDE", f"{int(r['fraudes']):,}",
                  f"{float(r['pct_fraude']):.4f}%")
        c3.metric("Marcadas como LEGÍTIMAS", f"{int(r['legitimas']):,}",
                  f"{100 - float(r['pct_fraude']):.4f}%")
        c4.metric("Cociente de desbalance (ratio)",
                  f"1 : {int(r['legitimas'] / max(r['fraudes'], 1)):,}",
                  help="Por cada fraude, hay esta cantidad de transacciones legítimas. "
                       "Un dataset (conjunto de datos) altamente desbalanceado es típico del fraude — "
                       "esto hace que la métrica accuracy (precisión global / aciertos totales) sea engañosa y haya que usar "
                       "precision (precisión) / recall (sensibilidad) / F1 (media armónica entre ambas).")

    st.info(
        "⚠️ **Desbalance de clases**: solo ~0.1% de las transacciones son fraude. "
        "Esto es la realidad del dominio (afortunadamente el fraude es raro), pero "
        "implica que cualquier modelo o regla debe ser **muy preciso** para no "
        "ahogarse en falsos positivos."
    )

    # ─────────────────────────────────────────────────────────────────────────
    # 5) MCC CODES
    # ─────────────────────────────────────────────────────────────────────────
    st.divider()
    st.header("5️⃣ MCC Codes (Códigos MCC) — catálogo de categorías de comercio")
    st.markdown(
        "**MCC** = Merchant Category Code (código de categoría de comercio). Son códigos estandarizados de 4 dígitos "
        "que clasifican a los comercios por rubro (5411 = Grocery Stores / Supermercados, 5812 = Restaurants / Restaurantes, "
        "etc.). El archivo `mcc_codes.json` es un diccionario que mapea cada código a su "
        "descripción legible. **No tiene datos transaccionales**, solo enriquece la tabla "
        "de hechos con la descripción del tipo de negocio."
    )

    mcc_resumen = query("""
        SELECT
            COUNT(DISTINCT mcc) AS mccs_usados,
            COUNT(DISTINCT mcc_description) AS descripciones,
            COUNT(*) FILTER (WHERE mcc_description = 'Unknown') AS sin_descripcion
        FROM transactions_processed
    """)
    if mcc_resumen is not None and not mcc_resumen.empty:
        r = mcc_resumen.iloc[0]
        c1, c2, c3 = st.columns(3)
        c1.metric("Códigos MCC usados (categorías de comercio)", f"{int(r['mccs_usados']):,}")
        c2.metric("Descripciones únicas", f"{int(r['descripciones']):,}")
        c3.metric("Transacciones sin descripción", f"{int(r['sin_descripcion']):,}",
                  help="Códigos MCC (categorías de comercio) que no estaban en el diccionario.")

    st.subheader("Top 15 categorías por volumen de transacciones")
    st.caption(
        "Estas son las categorías más frecuentes en el dataset, independientemente "
        "de si tuvieron fraude o no. Permite entender qué tipo de gasto domina."
    )
    top_mcc_vol = query("""
        SELECT mcc_description, SUM(total_transactions) AS txns
        FROM fraud_by_mcc
        WHERE mcc_description IS NOT NULL AND mcc_description != 'Unknown'
        GROUP BY mcc_description ORDER BY txns DESC LIMIT 15
    """)
    if top_mcc_vol is not None and not top_mcc_vol.empty:
        fig = px.bar(
            top_mcc_vol.sort_values("txns"), x="txns", y="mcc_description",
            orientation="h",
            color_discrete_sequence=["#1f77b4"],
            labels={"txns": "Cantidad de transacciones", "mcc_description": ""},
        )
        fig.update_layout(height=500, margin=dict(t=20, b=40, l=10, r=10))
        st.plotly_chart(fig, use_container_width=True)

    # ─────────────────────────────────────────────────────────────────────────
    # 6) CALIDAD DE DATOS
    # ─────────────────────────────────────────────────────────────────────────
    st.divider()
    st.header("🔍 Calidad de datos — observaciones del pipeline")
    st.markdown(
        "Durante la ingesta, el pipeline aplica varias validaciones y limpiezas. "
        "Estas son las observaciones más relevantes encontradas en el dataset:"
    )

    quality_checks = query("""
        SELECT
            (SELECT COUNT(*) FROM transactions_processed) AS total_post_clean,
            (SELECT COUNT(DISTINCT merchant_state) FROM transactions_processed
             WHERE merchant_state IS NOT NULL AND merchant_state != '') AS estados_distintos,
            (SELECT COUNT(DISTINCT merchant_state) FROM transactions_processed
             WHERE merchant_state IS NOT NULL AND LENGTH(merchant_state) = 2) AS estados_us,
            (SELECT COUNT(DISTINCT merchant_state) FROM transactions_processed
             WHERE merchant_state IS NOT NULL AND LENGTH(merchant_state) > 2) AS paises_intl,
            (SELECT COUNT(*) FROM transactions_processed
             WHERE merchant_state IS NULL OR merchant_state = '') AS sin_estado,
            (SELECT COUNT(*) FROM transactions_processed WHERE errors IS NOT NULL AND errors != '') AS con_errores
    """)
    if quality_checks is not None and not quality_checks.empty:
        r = quality_checks.iloc[0]

        with st.expander("🌍 Diversidad geográfica", expanded=True):
            st.markdown(
                f"- **{int(r['estados_distintos'])}** valores distintos en `merchant_state` (estado del comercio)\n"
                f"- **{int(r['estados_us'])}** códigos de 2 letras (estados de EEUU + DC, Washington Distrito de Columbia)\n"
                f"- **{int(r['paises_intl'])}** nombres de países (Mexico/México, Canada/Canadá, Italy/Italia, etc.)\n"
                f"- **{int(r['sin_estado']):,}** transacciones sin estado (típicamente online / en línea)\n\n"
                "El dataset (conjunto de datos) mezcla **estados de EEUU** con **países internacionales** en la misma "
                "columna. Por eso el dashboard (tablero) filtra los códigos estadounidenses para el mapa choropleth (coroplético, coloreado por región) "
                "y muestra los países en una sección separada."
            )

        with st.expander("⚠️ Transacciones con errores", expanded=False):
            st.markdown(
                f"- **{int(r['con_errores']):,}** transacciones tienen un valor no vacío "
                "en la columna `errors` (errores)\n\n"
                "Esto incluye motivos como `Bad PIN` (PIN incorrecto), `Insufficient Balance` (saldo insuficiente), "
                "`Technical Glitch` (falla técnica), etc. La **Regla 2** del pipeline (flujo) marca estas "
                "transacciones como sospechosas."
            )

        with st.expander("🧹 Limpieza aplicada por el pipeline (flujo)", expanded=False):
            st.markdown(
                "Durante `clean_data` (limpieza de datos) se aplicaron las siguientes transformaciones:\n\n"
                "- Conversión de `date` (fecha) a `transaction_date` (fecha de transacción, timestamp / marca temporal)\n"
                "- Limpieza de montos: remover `$`, comas, espacios → numérico\n"
                "- Descarte de filas con monto negativo o nulo\n"
                "- Descarte de filas con fecha inválida\n"
                "- Deduplicación (quitar duplicados) por `transaction_id`\n"
                "- Normalización de booleanos (`has_chip` / tiene chip, `card_on_dark_web` / tarjeta en dark web)\n"
                "- Limpieza de columnas monetarias (`credit_limit` / límite de crédito, `yearly_income` / ingreso anual, `total_debt` / deuda total)\n\n"
                f"Resultado final: **{int(r['total_post_clean']):,}** transacciones válidas en el DWH (almacén de datos)."
            )
