# SecureLink – Sistema de Detección de Fraude

Pipeline ETL orquestado con **Apache Airflow** que procesa transacciones financieras históricas, aplica reglas de detección de fraude y publica métricas en un dashboard analítico.

El proyecto sigue el **framework de 8 pasos** visto en clase (material 7.1 – Fundamentos de ETL y Pipelines) y la guía del ejemplo Black Friday.

---

## Inicio rápido

```bash
docker compose up -d --build
```

| Servicio | URL | Credenciales |
|---|---|---|
| Airflow UI | http://localhost:8080 | `admin` / `admin` |
| Streamlit Dashboard | http://localhost:8501 | sin auth |
| PostgreSQL DWH | localhost:5433 | `dwh` / `dwh123` |

Una vez levantado, activar el DAG `securelink_fraud_pipeline` en Airflow y ejecutarlo manualmente (▶ Trigger DAG). El pipeline procesa ~13 M transacciones; en una notebook tarda aproximadamente 10–15 min.

---

## Los 8 pasos del pipeline aplicados a SecureLink

Esta sección mapea cada paso del material teórico (7.1 pág. 9) a la implementación concreta del proyecto.

| Paso (diseño) | En este proyecto |
|---|---|
| 1. Objetivo del pipeline | Detectar fraude en transacciones y publicar métricas (tasa de fraude, monto en riesgo, FP/FN) en un dashboard analítico |
| 2. Fuentes de datos | 3 archivos CSV (transacciones, usuarios, tarjetas) + 2 archivos JSON (labels de fraude, códigos MCC) |
| 3. Estrategia de ingesta | Batch en chunks de 100 k filas (PyArrow incremental) — sin cargar todo en RAM |
| 4. Procesamiento | Validación de esquema → limpieza ($, comas, fechas, duplicados) → features (debt/income, online vs swipe) → 4 reglas de detección |
| 5. Almacenamiento del resultado | PostgreSQL DWH: tablas de detalle (`transactions_processed`) y agregadas (`fraud_metrics_global`, `fraud_by_mcc`, `fraud_by_state`, `fraud_by_card_type`, `fraud_by_merchant`, `user_profiles`) |
| 6. Flujo de datos | DAG con 8 tareas secuenciales, dependencias explícitas con `>>`, retries con backoff exponencial |
| 7. Gobernanza y monitoreo | Validación de conexión a DWH antes de procesar, `quality_check` final (verifica montos negativos, registra ejecución en `pipeline_run_log`), logs detallados en Airflow UI |
| 8. Capa de consumo | Streamlit con 3 paneles: General (KPIs y mapa de EEUU), Usuario (perfil individual), Comercios (top merchants) |

### 1) Objetivo del pipeline

**Usuarios finales**: analistas de riesgo y equipo de fraude. **Pregunta de negocio**: ¿qué porcentaje de transacciones es fraudulento, dónde se concentran y cuál es el monto expuesto? **Métrica de éxito**: tasa de falsos positivos baja sin perder fraudes reales (FN rate). El producto analítico es el dashboard de Streamlit alimentado por las tablas agregadas del DWH.

### 2) Fuentes de datos

| Fuente | Formato | Volumen | Rol |
|---|---|---|---|
| `transactions_data.csv` | CSV, separador `,` | ~13.3 M filas, 1.2 GB | Transacciones (hechos) |
| `users_data.csv` | CSV | ~2 k filas | Dimensión usuario |
| `cards_data.csv` | CSV | ~6 k filas | Dimensión tarjeta |
| `train_fraud_labels.json` | JSON anidado `{"target": {...}}` | ~8.9 M registros | Etiqueta de fraude (ground truth) |
| `mcc_codes.json` | JSON | ~900 entradas | Diccionario de categorías de comercio |

No hay datos en streaming ni cambios incrementales: el dataset es histórico y se reprocesa completo en cada corrida.

### 3) Estrategia de ingesta

**Batch** en chunks de 100 k filas usando `pd.read_csv(chunksize=...)`. Cada chunk se enriquece con los datos de referencia (users, cards, mcc, labels) y se escribe incrementalmente a Parquet con `pyarrow.ParquetWriter`. Esto evita cargar 13 M filas en memoria de una sola vez (el OOM que aparecía al hacer `pd.concat` masivo).

Los datos de referencia chicos (users, cards, mcc, labels) se cargan a memoria una sola vez como `dict` para lookup O(1) — más eficiente que un `merge` contra millones de filas.

### 4) Plan de procesamiento (transformaciones)

Cada tarea del DAG aplica una transformación específica:

- **`clean_data`**: convierte fechas (`pd.to_datetime`), limpia montos (`$`, comas), descarta negativos y duplicados (`drop_duplicates`), normaliza booleanos.
- **`build_features`**: calcula `debt_income_ratio = total_debt / yearly_income` y clasifica `transaction_type` (Online vs Swipe).
- **`apply_fraud_rules`**: aplica 4 reglas con un `OR` lógico:
  1. Monto atípico (> percentil 99)
  2. Transacción con `errors` no nulo
  3. Tarjeta presente en dark web (`card_on_dark_web == True`)
  4. Endeudamiento alto (`debt_income_ratio > 3`)
- Manejo de calidad: las filas inválidas (montos negativos, fechas inválidas, duplicados) se descartan con logging. El conteo aparece en el log de cada tarea.

### 5) Arquitectura de almacenamiento del resultado

PostgreSQL es el DWH (servicio `postgres-dwh`, puerto 5433 externamente). El esquema está en `init_sql/init_dwh.sql` y se crea automáticamente al levantar el contenedor por primera vez.

**Detalle**:
- `transactions_processed` — una fila por transacción, con todos los enriquecimientos y la marca `is_suspicious`.

**Agregados** (alimentan directamente al dashboard):
- `fraud_metrics_global` — KPIs únicos (tasa, monto en riesgo, FP/FN).
- `fraud_by_mcc` — fraude por categoría de comercio.
- `fraud_by_card_type` — fraude por marca/tipo/chip.
- `fraud_by_state` — fraude por estado (presenciales).
- `fraud_by_merchant` — top merchants.
- `user_profiles` — perfil agregado por usuario.

**Log**:
- `pipeline_run_log` — historial de ejecuciones (status, filas, fraudes).

### 6) Planificación del flujo de datos

```
validate_files
  >> ingest_data           (lee CSVs + JSON, enriquece, escribe parquet)
  >> clean_data            (fechas, montos, duplicados)
  >> build_features        (debt_income_ratio, transaction_type)
  >> apply_fraud_rules     (4 reglas, marca is_suspicious)
  >> compute_metrics       (acumuladores por chunk, guarda pickle)
  >> load_to_dwh           (UPSERT al DWH en batches de 50k)
  >> quality_check         (verifica integridad y loguea ejecución)
```

- **Secuencial** porque cada tarea consume el output de la anterior.
- **Reintentos** configurados en `default_args` con backoff exponencial.
- **Idempotencia** vía `ON CONFLICT (transaction_id) DO UPDATE` en lugar de `TRUNCATE+INSERT`: reejecutar el DAG no duplica datos ni rompe el dashboard mientras corre.

### 7) Gobernanza y monitoreo

- **Validación de archivos**: `validate_files` verifica existencia y columnas mínimas; falla rápido si falta algo.
- **Validación de DWH**: tarea inicial con `SELECT 1` para confirmar conectividad antes de procesar (evita correr 15 min y morir al cargar).
- **Quality check final**: cuenta montos negativos en el DWH; si encuentra, falla la tarea (Airflow lo marca rojo).
- **Logs**: cada tarea loguea conteos antes/después, los logs son visibles en la UI de Airflow (`/Graph` → click en la tarea → `Logs`).
- **Auditoría**: `pipeline_run_log` registra cada ejecución con timestamp, status y conteos.

*Nota de laboratorio*: las credenciales viven en `docker-compose.yml` (variables de entorno). En producción se usarían secretos gestionados (Vault, AWS Secrets Manager) — el material 7.4 lo recomienda explícitamente.

### 8) Capa de consumo

Dashboard en **Streamlit** (`dashboard/app.py`), expuesto en `localhost:8501`. Consulta directamente las tablas agregadas del DWH vía SQLAlchemy + psycopg2 con queries parametrizadas (sin SQL injection).

Tres paneles:

- **Panel General**: KPIs globales (tasa de fraude, monto en riesgo, FP/FN), fraude por MCC (top 15), choropleth de EEUU por estado, fraude por tipo de tarjeta.
- **Panel de Usuario**: dado un `user_id`, muestra perfil demográfico, métricas agregadas y últimas 100 transacciones.
- **Panel de Comercios**: top N merchants por cantidad de fraude y por monto en riesgo.

El refresh se hace en cada query (cache de 60 s vía `@st.cache_data(ttl=60)`).

---

## Decisiones técnicas

### ¿Por qué batch en chunks y no streaming?

El dataset es **histórico y estático** (1.2 GB). Streaming (Kafka/Flink) aportaría latencia < 1 segundo, pero no hay datos llegando en tiempo real. Batch con chunks de 100 k filas + escritura incremental a Parquet permite procesar 13 M filas con < 2 GB de RAM (el límite de Docker Desktop por defecto).

### ¿Por qué Parquet intermedio y no XCom de Airflow?

XCom serializa en la metadata de Airflow (PostgreSQL); está pensado para mensajes chicos (KB), no para 13 M filas (GB). Parquet en `/tmp` es columnar, comprimido y se lee/escribe con PyArrow en batches.

### ¿Por qué 4 reglas en lugar de un modelo ML?

Para esta entrega académica el alcance se limitó a **reglas explícitas**. La evolución previsible (sección del SRS) contempla entrenar un modelo con `train_fraud_labels.json` (que ya tenemos como ground truth) y reemplazar `apply_fraud_rules` por un `score_model` task.

### Control de calidad

- Filas con monto negativo, fecha inválida o duplicadas se descartan con logging del conteo.
- `quality_check` final cuenta nuevamente y falla la tarea si encuentra montos negativos en el DWH (cinturón + tiradores).
- Se registra cada ejecución en `pipeline_run_log` con `status`, `rows_processed` y `rows_fraud`.

### Resiliencia

- `retries: 3` con `retry_exponential_backoff: True` (60 s → 120 s → 240 s, tope de 5 min).
- `validate_dwh` al inicio: si el DWH no responde, el DAG falla en segundos en lugar de a los 15 minutos.
- Idempotencia con `UPSERT`: reejecutar el DAG actualiza, no duplica.

---

## Arquitectura

```
┌─────────────────────────────────┐
│ Archivos en ./data              │
│  - transactions_data.csv (1.2GB)│
│  - users_data.csv               │
│  - cards_data.csv               │
│  - train_fraud_labels.json      │
│  - mcc_codes.json               │
└────────────────┬────────────────┘
                 │ batch en chunks de 100k
                 ▼
┌─────────────────────────────────┐
│       Airflow (LocalExecutor)   │
│  securelink_fraud_pipeline DAG  │
│  - validate_files               │
│  - ingest_data (parquet)        │
│  - clean_data                   │
│  - build_features               │
│  - apply_fraud_rules            │
│  - compute_metrics              │
│  - load_to_dwh                  │
│  - quality_check                │
└────────────────┬────────────────┘
                 │ UPSERT en batches de 50k
                 ▼
┌─────────────────────────────────┐
│       PostgreSQL DWH            │
│  - transactions_processed       │
│  - fraud_metrics_global         │
│  - fraud_by_mcc                 │
│  - fraud_by_state               │
│  - fraud_by_card_type           │
│  - fraud_by_merchant            │
│  - user_profiles                │
│  - pipeline_run_log             │
└────────────────┬────────────────┘
                 │ SQLAlchemy + queries parametrizadas
                 ▼
┌─────────────────────────────────┐
│       Streamlit Dashboard       │
│  - Panel General                │
│  - Panel de Usuario             │
│  - Panel de Comercios           │
└─────────────────────────────────┘
```

---

## Estructura del proyecto

```
SecureLink/
├── dags/
│   └── securelink_fraud_pipeline.py   # DAG de Airflow (8 tareas)
├── dashboard/
│   └── app.py                          # Streamlit con 3 paneles
├── init_sql/
│   └── init_dwh.sql                    # Schema del DWH (se ejecuta al primer arranque)
├── data/                               # Archivos CSV/JSON de entrada
├── extra/                              # Material del curso (PDFs) y plantilla SRS
├── logs/                               # Logs de Airflow (se crea automáticamente)
├── Dockerfile.airflow                  # Imagen custom de Airflow (con PyArrow, pandas)
├── Dockerfile.streamlit                # Imagen del dashboard
├── docker-compose.yml                  # Orquestación de los 5 servicios
└── README.md
```

---

## Comandos útiles

```bash
# Levantar / reconstruir
docker compose up -d --build

# Ver estado de los contenedores
docker compose ps

# Ver logs del scheduler de Airflow
docker compose logs airflow-scheduler -f

# Detener (conserva los datos del DWH)
docker compose down

# Detener y borrar los datos del DWH (requiere reejecutar el pipeline)
docker compose down -v
```

---

## Escalabilidad y evolución prevista

El material 7.2 plantea escalar con **particionamiento, infraestructura elástica, procesamiento distribuido y formato columnar**. SecureLink ya cumple parcialmente:

- ✅ Formato columnar (Parquet) en archivos intermedios.
- ✅ Procesamiento en chunks (no carga todo en RAM).
- ⚠️ Single-node (LocalExecutor); para mayor volumen migrar a CeleryExecutor + workers.
- ⚠️ No hay particionamiento por fecha; convendría agregarlo en `transactions_processed`.

Evolución prevista (sección SRS 2d):

1. **Modelo de ML**: entrenar con `train_fraud_labels.json` y reemplazar las 4 reglas por un score probabilístico.
2. **API REST**: exponer el score de fraude para consultas en línea desde otros sistemas.
3. **Streaming**: pasar a Kafka + Flink si surge la necesidad de detección en tiempo real.
4. **Geolocalización real**: implementar `distance_km` entre merchant y usuario (hoy queda en `NaN`).

---

## Referencias

- Material 7.1 – Fundamentos de ETL y Pipelines
- Material 7.2 – ETL Avanzado: Estrategias de Ingesta y Calidad
- Material 7.3 – Orquestación de Datos
- Material 7.4 – Apache Airflow: Orquestación Profesional
- Proyecto ejemplo: Black Friday (estructura, contrato del DAG, idempotencia)
