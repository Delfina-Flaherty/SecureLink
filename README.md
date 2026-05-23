# SecureLink – Sistema de Detección de Fraude

Pipeline ETL orquestado con **Apache Airflow** que procesa transacciones financieras históricas, aplica reglas de detección de fraude y publica métricas en un dashboard analítico.

El proyecto sigue el **framework de 8 pasos** visto en clase (material 7.1 – Fundamentos de ETL y Pipelines) y la guía del ejemplo Black Friday.

---

## Primera ejecución (setup inicial)

Si nunca corriste el proyecto, seguí estos pasos en orden. Si ya tenés todo levantado y solo querés volver a arrancar, saltá a [Inicio rápido](#inicio-rápido).

### 1. Requisitos previos

- **Docker Desktop** instalado y corriendo. Descarga: https://www.docker.com/products/docker-desktop/
- ~5 GB libres en disco (imágenes Docker + 1.4 GB de datos crudos + ~3 GB del DWH una vez cargado).
- Puertos `8080`, `8501` y `5433` libres en `localhost`.
- **RAM mínima recomendada en la máquina host: 8 GB**. Con 6 GB o menos vas a tener problemas de memoria (ver [Troubleshooting avanzado](#troubleshooting-avanzado-problemas-de-memoria)).
- Recomendado: dar al menos **4 GB de RAM** a Docker Desktop (Settings → Resources). En Windows con WSL2, esto se configura en `C:\Users\<TuUsuario>\.wslconfig`:

  ```
  [wsl2]
  memory=4GB
  swap=4GB
  ```

  Después de modificarlo, ejecutá `wsl --shutdown` en PowerShell y reiniciá Docker Desktop. El `swap` es importante: permite que el sistema use disco como memoria virtual cuando la RAM se llena, evitando que los contenedores mueran por OOM (Out Of Memory).

### 2. Obtener los archivos de datos

Los 5 archivos de entrada (~1.4 GB en total) **no están en Git** porque GitHub limita archivos a 100 MB. Pedíselos a algún integrante del equipo (Drive, OneDrive, USB, etc.) y ponelos en `./data/` con estos nombres exactos:

```
data/
├── transactions_data.csv      (~1.2 GB)
├── train_fraud_labels.json    (~152 MB)
├── users_data.csv             (~161 KB)
├── cards_data.csv             (~498 KB)
└── mcc_codes.json             (~4.7 KB)
```

Sin estos archivos el pipeline (flujo de procesamiento) falla en la primera tarea (`validate_files`).

### 3. Levantar los contenedores

```bash
docker compose up -d --build
```

La primera vez baja imágenes y construye (~5–10 min). Las siguientes son segundos. Verificá con:

```bash
docker compose ps
```

Tienen que aparecer los 5 servicios en estado `Up`: `postgres-airflow`, `postgres-dwh`, `airflow-init`, `airflow-scheduler`, `airflow-webserver`, `securelink-dashboard`.

### 4. Correr el pipeline por primera vez

El DWH (Data Warehouse / almacén de datos) arranca **vacío** — sin este paso el dashboard muestra "No hay datos aún".

1. Abrir http://localhost:8080 (usuario `admin`, contraseña `admin`).
2. En la lista de DAGs, despausar `securelink_fraud_pipeline` (toggle a la izquierda).
3. Click en ▶️ **Trigger DAG**.
4. Esperar a que termine. Duraciones típicas:
   - **PC moderna (16 GB RAM, SSD)**: 3–8 minutos
   - **PC modesta (8 GB RAM, SSD)**: 8–20 minutos
   - **PC justa (4-6 GB RAM)**: 30–90 minutos (puede haber swap; ver [Troubleshooting avanzado](#troubleshooting-avanzado-problemas-de-memoria))

   Podés ver el progreso en la vista Graph (click en el DAG → Graph).
5. Cuando todas las tareas están en verde (`success`), el DWH ya tiene los datos.

### 5. Verificar que el dashboard funciona

Abrir http://localhost:8501. Deberías ver:
- KPIs con ~13 millones de transacciones procesadas.
- Métricas de fraude (tasa, monto en riesgo, FP / FN).
- Las pestañas del Panel General con gráficos poblados.

### Troubleshooting (qué hacer si algo falla)

| Síntoma | Causa probable | Solución |
|---|---|---|
| Contenedores reiniciando o muriendo | Docker sin recursos suficientes | Subir RAM en Docker Desktop (Settings → Resources, mínimo 4 GB). En WSL2 editar `.wslconfig` (ver [Requisitos previos](#1-requisitos-previos)). |
| El DAG falla en `validate_files` | Faltan archivos en `./data/` o nombres mal escritos | Verificar que estén los 5 archivos con el nombre exacto (paso 2). |
| Dashboard dice "No hay datos aún" | El pipeline no se ejecutó o falló | Revisar logs en Airflow (click en la tarea roja → Logs). |
| Puerto 8080 / 8501 / 5433 ocupado | Otro proceso lo está usando | `docker compose down` y matar el otro proceso, o cambiar el puerto en `docker-compose.yml`. |
| Quiero borrar todo y empezar de cero | — | `docker compose down -v` (el `-v` borra también los volúmenes y los datos del DWH — vas a tener que reejecutar el DAG). |
| `localhost:8080` muestra `ERR_CONNECTION_REFUSED` o `ERR_EMPTY_RESPONSE` | El webserver de Airflow se cayó por presión de memoria | Ver [Troubleshooting avanzado](#troubleshooting-avanzado-problemas-de-memoria). |
| Logs muestran `No response from gunicorn master within X seconds` | El worker del webserver tardó demasiado o fue matado por OOM | Ver [Troubleshooting avanzado](#troubleshooting-avanzado-problemas-de-memoria). |
| Logs muestran `Worker (pid:X) was sent SIGKILL! Perhaps out of memory?` | El kernel mató el worker por falta de RAM | Liberar memoria: apagar el scheduler mientras se usa el webserver y viceversa. |
| `postgres-dwh` queda `unhealthy` después de reiniciar Docker | WAL recovery o health check buscando una BD inexistente | El health check ya está corregido en este proyecto (`pg_isready -U dwh -d securelink`). Si falla, esperar 1-2 min a que PostgreSQL termine la recuperación. |
| `database "dwh" does not exist` en los logs de postgres-dwh | Health check mal configurado (versión vieja del compose) | Ya corregido. Si aparece, verificar que el `docker-compose.yml` use `pg_isready -U dwh -d securelink`. |
| DAG runs zombies en estado "running" tras crash del scheduler | El scheduler murió en medio de una ejecución | Marcarlos como fallidos: ver [Limpiar runs zombies](#limpiar-runs-zombies) abajo. |

---

## Troubleshooting avanzado: problemas de memoria

Este proyecto fue diseñado para correr en máquinas con ≥ 8 GB de RAM. En equipos con menos memoria (típicamente 4-6 GB físicos), Airflow webserver + scheduler + 2 Postgres + Streamlit excede la RAM disponible y el webserver se cae con errores tipo `ERR_CONNECTION_REFUSED` o gunicorn tira `No response from master`.

### Configuración aplicada en este proyecto para mitigarlo

- **Worker `gevent` en el webserver**: el worker `sync` por defecto forkea un proceso completo de Python por worker (~500 MB). `gevent` usa green threads dentro de un solo proceso, reduciendo el uso de memoria a ~150-200 MB. Configurado vía `AIRFLOW__WEBSERVER__WORKER_CLASS=gevent` en `docker-compose.yml` y `gevent==23.9.1` instalado en `Dockerfile.airflow`.
- **Timeouts extendidos**: `AIRFLOW__WEBSERVER__WEB_SERVER_WORKER_TIMEOUT=300` y `WEB_SERVER_MASTER_TIMEOUT=300` dan 5 minutos al worker para inicializar bajo presión de memoria, en lugar de los 120 s por defecto.
- **Health check de `postgres-dwh` corregido**: `pg_isready -U dwh -d securelink` (antes era `-U dwh` solo, que intentaba conectar a una BD inexistente).

### Si aún así el webserver se cae

El pipeline ya se ejecutó al menos una vez (los datos están en el DWH), seguí estos pasos:

#### Opción A — Apagar el scheduler para usar el webserver

Si solo necesitás ver el DAG en la UI (para entrega, screenshots, navegación), no necesitás el scheduler corriendo:

```bash
docker stop airflow-scheduler
docker compose up -d --force-recreate airflow-webserver
```

Esperá 2-3 minutos. El webserver va a arrancar estable con toda la RAM disponible.

#### Opción B — Apagar el webserver para correr el pipeline

Si necesitás triggerar una nueva ejecución del DAG, apagá el webserver primero (el pipeline corre solo en el scheduler):

```bash
docker stop airflow-webserver
# Triggerar via CLI:
docker exec airflow-scheduler airflow dags trigger securelink_fraud_pipeline
```

Cuando termine (monitorearlo con `docker exec airflow-scheduler airflow dags list-runs -d securelink_fraud_pipeline`), volver a levantar el webserver con la Opción A.

#### Opción C — Verificar resultados sin Airflow UI

El dashboard de Streamlit en `localhost:8501` lee directamente del DWH y no necesita Airflow para nada. Si solo querés ver las métricas de fraude, abrí Streamlit y listo.

### Limpiar runs zombies

Si el scheduler crasheó en medio de un run, ese DAG run queda en estado `running` pero nadie lo está ejecutando. Hay que marcarlo como fallido para que no consuma recursos:

```bash
docker exec airflow-scheduler python -c "
from airflow.models import DagRun
from airflow.utils.db import create_session
with create_session() as session:
    runs = session.query(DagRun).filter(
        DagRun.state == 'running',
        DagRun.dag_id == 'securelink_fraud_pipeline'
    ).all()
    for r in runs:
        r.state = 'failed'
        print(f'Marked failed: {r.run_id}')
    session.commit()
"
```

Después reiniciá el scheduler: `docker restart airflow-scheduler`.

### Cuidado al suspender la máquina mientras corre el pipeline

Si Windows entra en suspensión mientras el DAG está procesando `ingest_transactions` (1.2 GB de CSV, 10-15 min en máquinas potentes, hasta varias horas con 4 GB de RAM), Docker puede pausar o matar los contenedores. Antes de triggerar un run largo:

1. Buscá "Configuración de inicio/apagado y suspensión" en el menú inicio
2. Cambiá "Suspender el equipo" a "Nunca" mientras dure el pipeline
3. Restaurá el valor original cuando termine

### Si Docker Desktop tira errores 500 (API)

Si ves errores tipo `request returned 500 Internal Server Error for API route...`, Docker Desktop está colapsando por presión de memoria. Solución:

1. Reiniciá Docker Desktop (clic derecho en el ícono de la barra de tareas → Restart)
2. Esperá a que termine el reinicio (el ícono deja de girar)
3. `docker compose up -d` para levantar todo de nuevo

Los volúmenes con los datos del DWH se conservan, no hace falta reejecutar el pipeline.

---

## Inicio rápido

Si ya hiciste el [setup inicial](#primera-ejecución-setup-inicial) al menos una vez, alcanza con:

```bash
docker compose up -d --build
```

> ⚠️ **IMPORTANTE para los que vienen actualizando desde una versión anterior**:
> esta versión agrega `gevent` (worker del webserver), `polars` (ingesta/transformaciones)
> y `duckdb` (agregaciones) como dependencias nuevas. Después de hacer `git pull` tenés
> que correr `docker compose up -d --build` (no alcanza con `up -d`). El flag `--build`
> rebuildea la imagen de Airflow para instalar las dependencias nuevas; sin esto, el DAG
> falla con `ModuleNotFoundError`. Es solo la primera vez post-pull, después volvés a
> usar `docker compose up -d` normal.

| Servicio | URL | Credenciales |
|---|---|---|
| Airflow UI | http://localhost:8080 | `admin` / `admin` |
| Streamlit Dashboard | http://localhost:8501 | sin auth |
| PostgreSQL DWH | localhost:5433 | `dwh` / `dwh123` |

Si el DWH conserva los datos de una corrida anterior (volumen no borrado), el dashboard funciona de una. Si está vacío, repetir el paso 4 del setup (activar y disparar el DAG).

---

## Los 8 pasos del pipeline aplicados a SecureLink

Esta sección mapea cada paso del material teórico (7.1 pág. 9) a la implementación concreta del proyecto.

| Paso (diseño) | En este proyecto |
|---|---|
| 1. Objetivo del pipeline | Detectar fraude en transacciones y publicar métricas (tasa de fraude, monto en riesgo, FP/FN) en un dashboard analítico |
| 2. Fuentes de datos | 3 archivos CSV (transacciones, usuarios, tarjetas) + 2 archivos JSON (labels de fraude, códigos MCC) |
| 3. Estrategia de ingesta | Batch en chunks de 100 k filas (PyArrow incremental) — sin cargar todo en RAM |
| 4. Procesamiento | Validación de esquema → limpieza ($, comas, fechas, duplicados) → features (debt/income, online vs swipe) → 4 reglas de detección |
| 5. Almacenamiento del resultado | PostgreSQL DWH: tablas de detalle (`transactions_processed`) y agregadas (`fraud_metrics_global`, `fraud_by_mcc`, `fraud_by_state`, `fraud_by_card_type`, `fraud_by_merchant`, `user_profiles`). `COPY` para la tabla de detalle (5-10x más rápido que `INSERT`), `execute_values` para las agregadas. |
| 6. Flujo de datos | DAG con validaciones paralelas + 4 cargas de referencia paralelas + 5 tareas secuenciales (`ingest_transactions` → `transform_data` → `compute_metrics` → `load_to_dwh` → `quality_check`). Retries con backoff exponencial. |
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

**Batch** con **Polars lazy/streaming**. `pl.scan_csv(...).join(...).sink_parquet(...)` lee el CSV en streaming, hace los 4 joins (labels, cards, users, MCC) y escribe Parquet, todo en una sola pasada paralelizada en todos los cores del CPU. Polars no materializa los 13 M filas en RAM — procesa por bloques de forma transparente.

Los datos de referencia chicos (users, cards, mcc, labels) se cargan una sola vez como DataFrames Polars y participan de hash joins (mucho más rápido que `dict.map()` row-by-row sobre 13 M filas).

> **Nota histórica**: la primera versión usaba `pd.read_csv(chunksize=...)` con `pyarrow.ParquetWriter` incremental y dict lookups. Funcionaba pero pandas usa un solo core y los `merge()` por chunk son lentos. La migración a Polars bajó la duración de `ingest_transactions` ~5-10x.

### 4) Plan de procesamiento (transformaciones)

Las tres transformaciones (limpieza, features y reglas) se combinan en la tarea `transform_data` usando una sola pasada Polars sobre el parquet — antes eran 3 tareas separadas que leían y escribían el parquet completo cada una.

- **Limpieza**: convierte fechas, limpia montos (`$`, comas), descarta negativos/inválidos, deduplica por `transaction_id`, normaliza booleanos.
- **Features**: calcula `debt_income_ratio = total_debt / yearly_income` y clasifica `transaction_type` (Online vs Swipe).
- **Reglas de detección**: aplica 4 reglas con un `OR` lógico:
  1. Monto atípico (> percentil 99 — calculado en una pasada lazy adicional sobre el parquet)
  2. Transacción con `errors` no nulo
  3. Tarjeta presente en dark web (`card_on_dark_web == True`)
  4. Endeudamiento alto (`debt_income_ratio > 3`)
- Manejo de calidad: las filas inválidas (montos negativos, fechas inválidas, duplicados) se descartan en la fase de limpieza. El conteo aparece en el log.

> **Nota histórica**: en versiones anteriores estas eran 3 tareas separadas (`clean_data` → `build_features` → `apply_fraud_rules`) que cada una leía y escribía un parquet de ~1 GB. La fusión en `transform_data` ahorra 2 pasadas completas de I/O.

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
validate_files + validate_dwh                          (paralelo)
  >> load_users + load_cards + load_mcc + load_labels  (paralelo)
  >> ingest_transactions    (Polars lazy/streaming: CSV + 4 joins en una pasada)
  >> transform_data         (Polars: clean + features + reglas combinados)
  >> compute_metrics        (DuckDB: agregaciones SQL vectorizadas sobre parquet)
  >> load_to_dwh            (COPY para transacciones + execute_values para agregadas)
  >> quality_check          (verifica integridad y loguea ejecución)
```

- **Secuencial** porque cada tarea consume el output de la anterior.
- **Reintentos** configurados en `default_args` con backoff exponencial.
- **Idempotencia** vía `TRUNCATE + COPY` en una sola transacción: PostgreSQL MVCC garantiza que el dashboard siga viendo los datos viejos hasta el `COMMIT` final. Reejecutar el DAG no duplica datos ni deja la tabla vacía intermedia.
- **Atomic full refresh** sobre las tablas agregadas (fraud_by_mcc, fraud_by_state, etc.) por la misma razón.

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

### ¿Por qué Polars + DuckDB y no pandas para todo el pipeline?

La primera versión del pipeline usaba pandas en todas las tareas. Funcionaba pero era lento en máquinas chicas — la corrida completa con 13M transacciones tomaba ~4 horas en una PC con 4 GB de RAM (y ~15-30 min en una con 16 GB). Los dos cuellos de botella eran `ingest_transactions` (CSV parsing + 4 merges) y `compute_metrics` (groupby con acumuladores Python).

**Cambios concretos:**

| Tarea | Antes (pandas) | Ahora (Polars/DuckDB) | Ganancia |
|---|---|---|---|
| `ingest_transactions` | Lee CSV en chunks de 100k, merge() por chunk, un solo core | `pl.scan_csv` + 4 joins lazy + `sink_parquet`, paralelo en todos los cores | ~5-10x |
| `transform_data` | 3 tareas (clean / features / rules) cada una lee y escribe 1 GB | Una sola tarea Polars que combina todo | ~3x menos I/O |
| `compute_metrics` | `defaultdict` + pandas `groupby` por chunk en Python | DuckDB SQL vectorizado directo sobre el parquet | ~10-50x |
| `load_to_dwh` (transacciones) | `execute_values` con `INSERT ... ON CONFLICT` | `COPY ... FROM STDIN` con CSV streaming desde Polars | ~5-10x |
| `load_to_dwh` (agregadas) | `cursor.execute()` fila por fila en loop | `execute_values` con batches de 1000 | ~50-100x |

**Por qué Polars y no Spark/Dask**: para un dataset de 13M filas (1.2 GB) un motor single-node columnar como Polars es la mejor opción — Spark/Dask agregarían overhead de coordinación distribuida sin necesidad. Polars usa Arrow como representación interna, vectorización SIMD y paralelización automática.

**Por qué DuckDB para agregaciones**: es columnar+vectorizado, lee parquet directamente sin importar a memoria, y compila las queries a planes optimizados. Reemplazó ~200 líneas de Python (defaultdict + groupby + reduce) por ~10 queries SQL. Más rápido, más legible, más fácil de validar.

**Por qué `COPY` y no `INSERT`**: PostgreSQL `COPY` evita el overhead de parseo de SQL por cada fila, no genera entries en el query log, y usa un formato de wire más eficiente. Para bulk loads de millones de filas es 5-10x más rápido que `INSERT` aunque uses `execute_values` con batches.

### ¿Por qué worker `gevent` en el webserver y no `sync`?

El worker `sync` por defecto de gunicorn forkea un proceso completo de Python por cada worker, cargando todo el código de Airflow (~500 MB por worker). En máquinas con poca RAM (≤ 6 GB), esto colisiona con el scheduler y la BD, causando OOM kills del webserver.

`gevent` usa green threads cooperativos dentro de un único proceso, manteniendo el uso de memoria en ~150-200 MB. Como contrapartida requiere que ninguna tarea bloquee el event loop, pero el webserver de Airflow solo sirve HTTP y queries a la metadata DB, así que es seguro.

Configurado via env vars en `docker-compose.yml`:
```yaml
- AIRFLOW__WEBSERVER__WORKERS=1
- AIRFLOW__WEBSERVER__WORKER_CLASS=gevent
- AIRFLOW__WEBSERVER__WEB_SERVER_WORKER_TIMEOUT=300
- AIRFLOW__WEBSERVER__WEB_SERVER_MASTER_TIMEOUT=300
```

Y `gevent==23.9.1` agregado al `Dockerfile.airflow`.

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
│  - validate_files / dwh         │
│  - load_users / cards / mcc /   │
│    labels (paralelo)            │
│  - ingest_transactions (Polars) │
│  - transform_data (Polars)      │
│  - compute_metrics (DuckDB)     │
│  - load_to_dwh (COPY)           │
│  - quality_check                │
└────────────────┬────────────────┘
                 │ COPY (5-10x más rápido que INSERT)
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

# Ver uso de memoria por contenedor (útil para diagnosticar OOM)
docker stats --no-stream

# Ver logs del scheduler de Airflow
docker compose logs airflow-scheduler -f

# Ver logs del webserver de Airflow
docker compose logs airflow-webserver -f

# Apagar solo el scheduler (libera RAM para usar el webserver)
docker stop airflow-scheduler

# Apagar solo el webserver (libera RAM para que corra el pipeline)
docker stop airflow-webserver

# Triggerar un DAG desde la línea de comandos (sin webserver)
docker exec airflow-scheduler airflow dags trigger securelink_fraud_pipeline

# Listar las ejecuciones del DAG (estado, fechas)
docker exec airflow-scheduler airflow dags list-runs -d securelink_fraud_pipeline

# Ver el estado de cada tarea de un run específico
docker exec airflow-scheduler airflow tasks states-for-dag-run securelink_fraud_pipeline <RUN_ID>

# Detener todo (conserva los datos del DWH)
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
3. **Streaming**: pasar a Kafka + Flink si surge la necesidad de detección en tiempo real (ver siguiente sección).
4. **Geolocalización real**: implementar `distance_km` entre merchant y usuario (hoy queda en `NaN`).

---

## Evolución a tiempo real

El nombre completo del proyecto en el SRS es *"SecureLink – Análisis de Transacciones Financieras en **Tiempo Real**"*, y la sección 2.d del SRS lista explícitamente como evolución previsible la *"detección de fraude en tiempo real sobre transacciones entrantes"*. El alcance actual del MVP es **batch analítico diario** sobre datos históricos — esta sección describe cómo se haría la transición de forma **incremental** en dos etapas, sin reescribir la lógica de negocio.

### El espectro de "tiempo real"

"Tiempo real" no es binario: hay varios niveles según la latencia objetivo y la complejidad de infraestructura aceptable.

| Nivel | Patrón | Latencia | Engine típico | Quién lo usa |
|---|---|---|---|---|
| 1. **Batch tradicional** | Procesar todo cada N horas/días | Horas-días | pandas / Polars / Spark batch | **SecureLink MVP actual** (`@daily`) |
| 2. **Micro-batch con CDC** | Mini-corridas frecuentes con cambios incrementales | 1-5 min | Igual que batch + scheduler frecuente | **Proyecto ejemplo Black Friday** (`*/1 * * * *`) |
| 3. **Streaming verdadero** | Procesar cada evento al instante | Milisegundos | Kafka + Flink / Kafka Streams | Sistemas de fraud detection productivos (Visa, Stripe) |

La evolución natural de SecureLink es **1 → 2 → 3**: primero migrar a micro-batch siguiendo el patrón del ejemplo Black Friday (cambio pequeño), después a streaming verdadero cuando la latencia de minutos no alcance (cambio grande de infraestructura).

---

### Etapa 1 — Micro-batch con CDC (estilo Black Friday)

Replica el patrón del proyecto ejemplo: Airflow corre el DAG cada 1-5 minutos, cada corrida procesa solo las transacciones nuevas usando **CDC (Change Data Capture)** por timestamp. **Sin Kafka, sin Flink** — solo cambios en el DAG actual.

**Cambios necesarios respecto al MVP actual:**

| Aspecto | MVP actual (batch diario) | Etapa 1 (micro-batch CDC) |
|---|---|---|
| **Schedule** | `@daily` | `*/1 * * * *` (cada minuto) o `*/5 * * * *` (cada 5 min) |
| **Fuente de datos** | CSV estático en `./data/` | API REST del procesador de pagos o PostgreSQL del banco emisor |
| **Estrategia de ingesta** | Full refresh (lee todo el CSV) | CDC incremental: `WHERE last_updated >= data_interval_start` |
| **Volumen por corrida** | 13 M filas | Decenas-cientos de filas (las nuevas del último minuto) |
| **Engine** | Polars + DuckDB (volumen grande) | pandas / Polars (volumen chico — pandas alcanza) |
| **Patrón de carga al DWH** | `TRUNCATE` + `COPY` (atomic full refresh) | `INSERT ... ON CONFLICT DO UPDATE` (UPSERT incremental) |
| **Comunicación entre tareas** | Parquet/pickle en `/tmp` | XCom (datos chicos por corrida) |
| **Cantidad de tareas** | 11 | ~7-8 (las cargas paralelas de referencia ya no se reejecutan, se cachean) |

**Esquema de tareas resultante** (idéntico al de Black Friday):

```
validate_source + validate_dwh           (paralelo)
  >> extract_transactions_cdc            (consulta API/DB con filtro por last_updated)
  >> transform_unify                     (limpia + enriquece + aplica reglas)
  >> load_aggregate                      (UPSERT al DWH + actualiza métricas)
  >> quality_check                       (control de calidad de la mini-corrida)
```

**Ventajas:**
- Sigue el patrón canónico del material 7.x y del ejemplo Black Friday
- Cambios mínimos en infraestructura: el mismo Airflow + PostgreSQL ya montados
- Latencia de minutos, suficiente para muchos casos de uso de fraude no críticos (revisión de reportes diarios, alertas no bloqueantes)

**Limitaciones:**
- Latencia mínima ~1 minuto (no apto para bloquear transacciones en POS)
- Si una corrida falla, los datos quedan pendientes hasta la próxima (no hay reintento por evento)
- El scheduler de Airflow agrega overhead (parsea el DAG cada minuto)

**Cuándo conviene esta etapa:** cuando el negocio acepta latencia de 1-5 minutos. Por ejemplo: alertas al equipo de fraude para investigación post-hoc, dashboards de monitoreo, generación de reportes near-real-time.

---

### Etapa 2 — Streaming verdadero (Kafka + Flink)

Cuando la latencia de minutos no alcanza (típicamente: bloquear una transacción **en el POS antes de aprobarla**, lo cual exige <1 segundo), hay que pasar a streaming event-driven. **Esto sí cambia la arquitectura de infraestructura**.

**Arquitectura propuesta:**

```
┌─────────────────────────┐
│ APIs de procesadores    │  Visa / Mastercard / banco emisor
│ de pagos                │  publican cada transacción como evento
└───────────┬─────────────┘
            │ ~1 KB por evento
            ▼
┌─────────────────────────┐
│   Kafka (event bus)     │  Topic "transactions"
│   Particionado por      │  particionado por card_id
│   card_id               │  (orden garantizado por tarjeta)
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  Apache Flink           │  Mismas 4 reglas que el batch actual
│  (stream processor)     │  + features en ventanas deslizantes:
│                         │    - txns/usuario en últimos 5 min
│                         │    - distancia entre comercios consecutivos
│                         │    - desviación del monto vs perfil histórico
└─────┬─────────────┬─────┘
      │             │
      ▼             ▼
┌──────────┐  ┌──────────────┐
│  Redis   │  │ Topic Kafka  │  → Sistema de alertas:
│ (state / │  │  "alertas"   │     notifica al cliente,
│  cache)  │  │              │     bloquea tarjeta si aplica,
└────┬─────┘  └──────────────┘     genera ticket de fraude
     │
     ▼
┌─────────────────────────┐
│  PostgreSQL DWH         │  Sink incremental desde Flink
│  (mismo schema actual)  │  en micro-batches de ~30 s
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  Streamlit Dashboard    │  Con auto-refresh corto o
│  (igual al actual)      │  WebSocket para alertas push
└─────────────────────────┘
```

**Diferencia clave con la Etapa 1:** las fuentes ahora **publican** eventos (push), no que el pipeline los **consulte** (pull). El procesador (Flink) mantiene estado continuo en memoria (ventanas deslizantes), garantiza exactly-once con checkpointing, y procesa cada evento en milisegundos.

**Mapeo del pipeline actual a streaming:**

| Tarea actual | Equivalente en Etapa 2 (streaming) |
|---|---|
| `ingest_transactions` (lee CSV) | **Kafka producer** — las APIs publican cada transacción como evento |
| `transform_data` (Polars batch) | **Flink job** — misma lógica de limpieza/features pero sobre el stream |
| `compute_metrics` (DuckDB) | **Flink windowing** — agregaciones continuas en ventanas + state en Redis |
| `load_to_dwh` (`TRUNCATE` + `COPY` masivo) | **Flink JDBC sink** — micro-batches de 30 s al DWH (`INSERT` incremental) |
| `quality_check` (validación final del batch) | **Continuous monitoring** — checks de latencia, throughput y error rate del stream |

**Requisitos adicionales que solo aplican a la Etapa 2:**

1. **Infraestructura distribuida nueva**: cluster Kafka (~3 brokers mínimo) + cluster Flink (~2 task managers) + Redis. El single-node con LocalExecutor del MVP no alcanza.
2. **Modelo de ML** (también en SRS 2.d): las reglas heurísticas dan resultados aceptables en batch/micro-batch, pero en streaming se prefiere un score probabilístico de un modelo entrenado (XGBoost / LightGBM) para calibrar el umbral según el costo de FP vs FN.
3. **Sistema de alertas event-driven**: consumer del topic `alertas` que notifica al cliente, bloquea la tarjeta vía API y genera tickets. No existe en el MVP.
4. **Garantías de orden y exactly-once**: Flink con checkpointing + Kafka con particionado por `card_id` garantizan que no se procese dos veces ni se pierda una transacción.

---

### Lo que se mantiene en ambas etapas

- **Las 4 reglas de detección de fraude**: es lógica de negocio, no de infraestructura. El mismo código Python que evalúa `amount > p99`, `errors not null`, etc., corre tanto en pandas (Etapa 1) como dentro de un operador Flink (Etapa 2).
- **El schema del DWH**: `transactions_processed`, `fraud_metrics_global` y las agregadas no cambian. Lo que cambia es cómo se pueblan (UPSERT incremental vs sink streaming).
- **El dashboard Streamlit**: las queries siguen funcionando contra las mismas tablas.
- **Filosofía de control de calidad y observabilidad**: logs por tarea, métricas de pipeline, validaciones explícitas.

### ¿Por qué el MVP es batch y no micro-batch desde el día 1?

Porque para esta entrega académica el **dataset disponible es histórico y estático** (~1.2 GB de transacciones 2010-2019). No hay un sistema productivo del que extraer cambios incrementales — los datos están todos en un CSV. Implementar CDC sobre datos estáticos sería forzado y no demostraría nada nuevo. El batch actual:

- Demuestra todo el ciclo de un pipeline ETL bien diseñado (8 pasos del material 7.1)
- Procesa el volumen real (13 M filas) con buena performance (Polars + DuckDB + COPY)
- Produce las mismas métricas que tendría el sistema en producción
- **Tiene una migración bien definida a tiempo real en dos etapas** (esta sección lo demuestra)

La transición es **incremental, no destructiva**: la lógica de negocio (reglas, schema, dashboard) se reutiliza en ambas etapas. Solo cambian las fuentes y el engine de procesamiento.

---

## Referencias

- Material 7.1 – Fundamentos de ETL y Pipelines
- Material 7.2 – ETL Avanzado: Estrategias de Ingesta y Calidad
- Material 7.3 – Orquestación de Datos
- Material 7.4 – Apache Airflow: Orquestación Profesional
- Proyecto ejemplo: Black Friday (estructura, contrato del DAG, idempotencia)
