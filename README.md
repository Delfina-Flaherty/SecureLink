# SecureLink – Sistema de Detección de Fraude

Pipeline ETL orquestado con **Apache Airflow** que procesa transacciones financieras históricas, aplica reglas de detección de fraude y publica métricas en un dashboard analítico.

El proyecto está estructurado según el **framework de 8 pasos** para el diseño de pipelines ETL.

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
4. Esperar a que termine. Duraciones típicas (el grueso es `load_to_dwh`, ver [benchmark](#por-qué-polars--duckdb-y-no-pandas-para-todo-el-pipeline)):
   - **PC con 8 GB de RAM, SSD, sin uso concurrente**: ~37 minutos (medido)
   - **PC con 16 GB RAM**: algo menos, el COPY se acelera con más cache
   - **PC justa (4-6 GB RAM) o usada al mismo tiempo**: 1-2+ horas por el swap a disco (ver [Troubleshooting avanzado](#troubleshooting-avanzado-problemas-de-memoria))

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

Esta sección mapea cada paso del framework de diseño a la implementación concreta del proyecto.

| Paso (diseño) | En este proyecto |
|---|---|
| 1. Objetivo del pipeline | Detectar fraude en transacciones y publicar métricas (tasa de fraude, monto en riesgo, FP/FN) en un dashboard analítico |
| 2. Fuentes de datos | 3 archivos CSV (transacciones, usuarios, tarjetas) + 2 archivos JSON (labels de fraude, códigos MCC) |
| 3. Estrategia de ingesta | Batch con Polars lazy/streaming (scan_csv + joins + sink_parquet) — sin cargar todo en RAM |
| 4. Procesamiento | Validación de esquema → limpieza → indicadores derivados (debt/income, online vs swipe, distancia geográfica, frecuencia por tarjeta) → detección por puntaje ponderado |
| 5. Almacenamiento del resultado | PostgreSQL DWH: tablas de detalle (`transactions_processed`) y agregadas (`fraud_metrics_global`, `fraud_by_mcc`, `fraud_by_state`, `fraud_by_card_type`, `fraud_by_merchant`, `user_profiles`). `COPY` para la tabla de detalle (5-10x más rápido que `INSERT`), `execute_values` para las agregadas. |
| 6. Flujo de datos | DAG con validaciones paralelas + 4 cargas de referencia paralelas + 5 tareas secuenciales (`ingest_transactions` → `transform_data` → `compute_metrics` → `load_to_dwh` → `quality_check`). Retries con backoff exponencial. |
| 7. Gobernanza y monitoreo | Validación de conexión a DWH antes de procesar, `quality_check` final (verifica montos negativos, registra ejecución en `pipeline_run_log`), logs detallados en Airflow UI |
| 8. Capa de consumo | Streamlit con 4 paneles: General (KPIs y mapa de EEUU), Usuario (perfil + mapa individual), Comercios (top merchants), Explorador con Filtros |

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

- **Limpieza**: convierte fechas, limpia montos (`$`, comas), descarta negativos/inválidos, normaliza booleanos.
- **Indicadores derivados** (RF06):
  - `debt_income_ratio = total_debt / yearly_income`
  - `transaction_type` (Online vs Swipe) a partir de `use_chip`
  - `distance_km`: distancia geográfica entre el domicilio del usuario (lat/long) y el comercio. Como el dataset no trae coordenadas del comercio, se aproxima al **centroide del estado** (`merchant_state`) y se calcula con la fórmula de **Haversine**. Las transacciones Online no tienen estado → `distance_km` queda en null (RF18).
  - `card_txn_count`: frecuencia de transacciones por tarjeta.
- **Detección por puntaje ponderado** (reemplazó al viejo OR de reglas): cada señal suma puntos según su poder predictivo y se marca `is_suspicious` si el `fraud_score` supera el umbral (4). Señales: online, monto en bandas, monto atípico para el usuario (p99 propio), error en la transacción, tarjeta en dark web, y **distancia geográfica inusual** (> 500 km, RF08). La columna `suspicion_reasons` indica qué señales dispararon.
- Manejo de calidad: las filas inválidas (montos negativos, fechas inválidas) se descartan en la fase de limpieza. El conteo aparece en el log.

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

*Nota*: las credenciales viven en `docker-compose.yml` (variables de entorno) para simplificar el setup local. En producción se usarían secretos gestionados (Vault, AWS Secrets Manager).

### 8) Capa de consumo

Dashboard en **Streamlit** (`dashboard/app.py`), expuesto en `localhost:8501`. Consulta directamente las tablas agregadas del DWH vía SQLAlchemy + psycopg2 con queries parametrizadas (sin SQL injection).

Cuatro paneles:

- **Panel General**: KPIs globales (tasa de fraude, monto en riesgo, FP/FN, matriz de confusión), fraude por MCC, choropleth de EEUU por estado, fraude por tipo y marca de tarjeta, tendencias temporales.
- **Panel de Usuario**: dado un `user_id`, muestra perfil demográfico, métricas agregadas, últimas 100 transacciones y un **mapa coroplético de sus transacciones por estado** (RF13).
- **Panel de Comercios**: top N merchants por cantidad de fraude y por monto en riesgo.
- **Explorador con Filtros** (RF16): filtros combinados por usuario, tarjeta, tipo de transacción, categoría MCC, estado, marca de tarjeta y rango de fechas. Muestra métricas del subconjunto filtrado y una tabla de detalle. Consulta `transactions_processed` con bind params (sin SQL injection).

El refresh se hace en cada query (cache de 60 s vía `@st.cache_data(ttl=60)`).

---

## Decisiones técnicas

### ¿Por qué batch en chunks y no streaming?

El dataset es **histórico y estático** (1.2 GB). Streaming (Kafka/Flink) aportaría latencia < 1 segundo, pero no hay datos llegando en tiempo real. Batch con chunks de 100 k filas + escritura incremental a Parquet permite procesar 13 M filas con < 2 GB de RAM (el límite de Docker Desktop por defecto).

### ¿Por qué Parquet intermedio y no XCom de Airflow?

XCom serializa en la metadata de Airflow (PostgreSQL); está pensado para mensajes chicos (KB), no para 13 M filas (GB). Parquet en `/tmp` es columnar, comprimido y se lee/escribe con PyArrow en batches.

### ¿Por qué puntaje ponderado en lugar de un modelo ML?

Para esta entrega académica el alcance se limitó a un **sistema de puntaje ponderado heurístico** (cada señal suma puntos según su poder predictivo observado en los datos; se marca sospechosa si supera un umbral). Reemplazó a una versión anterior que usaba un `OR` lógico de reglas, que tenía precisión muy baja (~0.4%) porque bastaba una sola señal débil para marcar. La evolución previsible (sección del SRS) contempla entrenar un modelo con `train_fraud_labels.json` (que ya tenemos como ground truth) y reemplazar el puntaje por un score probabilístico.

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

**Benchmark medido** (corrida limpia end-to-end, PC con 8 GB de RAM asignados a Docker, sin uso concurrente):

| Tarea | Duración | Notas |
|---|---|---|
| validate + load_users/cards/mcc | < 1 min | Paralelo |
| `load_labels` | 2 min | Parquet en vez de pickle-dict (evita OOM) |
| `ingest_transactions` | 3.8 min | Polars lazy/streaming + 4 joins |
| `transform_data` | 2.9 min | Limpieza + features + detección por puntaje |
| `compute_metrics` | 0.4 min | DuckDB |
| `load_to_dwh` | 24.4 min | COPY de 12.6M filas (el cuello de botella, ~66% del total) |
| `quality_check` | 1.6 min | |
| **TOTAL** | **~37 min** | |

`load_to_dwh` domina el tiempo (cargar 12.6M filas en PostgreSQL con sus índices es el piso de este hardware). El tuning de Postgres (`shared_buffers=1GB` en `docker-compose.yml`) ayuda al COPY. Se evaluó la estrategia *DROP índices → COPY → CREATE índices* pero resultó más lenta (recrear 6 índices sobre 12.6M filas cuesta más que lo que ahorra el COPY), así que se mantiene el COPY directo.

> **Nota sobre tiempos**: el tiempo total varía mucho según la RAM y el uso concurrente de la máquina. En una PC de 4-6 GB usada para otras cosas al mismo tiempo, el pipeline puede tardar 2+ horas por el swap a disco. El benchmark de arriba es el caso limpio. Para desarrollo en máquinas chicas conviene procesar una muestra del dataset.

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
│  - Panel de Usuario (+ mapa)    │
│  - Panel de Comercios           │
│  - Explorador con Filtros       │
└─────────────────────────────────┘
```

---

## Estructura del proyecto

```
SecureLink/
├── dags/
│   └── securelink_fraud_pipeline.py   # DAG de Airflow (8 tareas)
├── dashboard/
│   └── app.py                          # Streamlit con 4 paneles
├── init_sql/
│   └── init_dwh.sql                    # Schema del DWH (se ejecuta al primer arranque)
├── data/                               # Archivos CSV/JSON de entrada
├── extra/                              # Documentos de referencia y plantilla SRS
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

Las estrategias clásicas de escalamiento son **particionamiento, infraestructura elástica, procesamiento distribuido y formato columnar**. SecureLink ya cumple parcialmente:

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

| Nivel | Patrón | Latencia | Engine típico | Casos de uso típicos |
|---|---|---|---|---|
| 1. **Batch tradicional** | Procesar todo cada N horas/días | Horas-días | pandas / Polars / Spark batch | Análisis histórico, reportes diarios. **SecureLink MVP actual** (`@daily`) |
| 2. **Micro-batch con CDC** | Mini-corridas frecuentes con cambios incrementales | 1-5 min | Airflow con scheduler frecuente + pandas/Polars | Reportes near-real-time, dashboards de monitoreo, alertas no bloqueantes |
| 3. **Streaming verdadero** | Procesar cada evento al instante | Milisegundos | Kafka + Flink / Kafka Streams | Decisión transaccional en POS (bloquear o aprobar la operación), notificación push al cliente |

La evolución natural de SecureLink es **1 → 2 → 3**: primero migrar a micro-batch (cambio pequeño en el DAG actual, sin nueva infraestructura), después a streaming verdadero cuando la latencia de minutos no alcance (cambio grande de infraestructura).

---

### Etapa 1 — Micro-batch con CDC

Airflow corre el DAG cada 1-5 minutos, cada corrida procesa solo las transacciones nuevas usando **CDC (Change Data Capture)** por timestamp. **Sin Kafka, sin Flink** — solo cambios en el DAG actual.

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

**Esquema de tareas resultante**:

```
validate_source + validate_dwh           (paralelo)
  >> extract_transactions_cdc            (consulta API/DB con filtro por last_updated)
  >> transform_unify                     (limpia + enriquece + aplica reglas)
  >> load_aggregate                      (UPSERT al DWH + actualiza métricas)
  >> quality_check                       (control de calidad de la mini-corrida)
```

**Ventajas:**
- Sigue el patrón canónico de Airflow para pipelines incrementales con CDC
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

Porque el **dataset disponible es histórico y estático** (~1.2 GB de transacciones 2010-2019). No hay un sistema productivo del que extraer cambios incrementales — los datos están todos en un CSV. Implementar CDC sobre datos estáticos sería forzado y no demostraría nada nuevo. El batch actual:

- Demuestra todo el ciclo de un pipeline ETL bien diseñado (8 pasos del framework)
- Procesa el volumen real (13 M filas) con buena performance (Polars + DuckDB + COPY)
- Produce las mismas métricas que tendría el sistema en producción
- **Tiene una migración bien definida a tiempo real en dos etapas** (esta sección lo demuestra)

La transición es **incremental, no destructiva**: la lógica de negocio (reglas, schema, dashboard) se reutiliza en ambas etapas. Solo cambian las fuentes y el engine de procesamiento.

---

## Apéndice: trazabilidad académica

> Esta sección está pensada para la evaluación de la cátedra. Si solo necesitás
> usar o desarrollar SecureLink, podés saltearla — el cuerpo del README ya cubre
> todo lo necesario para trabajar con el producto.

### Material del curso aplicado al proyecto

- **7.1 — Fundamentos de ETL y Pipelines**: framework de 8 pasos (objetivo, fuentes, estrategia de ingesta, procesamiento, almacenamiento, flujo, gobernanza, consumo). Aplicado en la sección [Los 8 pasos del pipeline aplicados a SecureLink](#los-8-pasos-del-pipeline-aplicados-a-securelink).
- **7.2 — ETL Avanzado (Estrategias de Ingesta y Calidad)**: ingesta en batch por chunks, formato columnar (Parquet), control de calidad con validaciones explícitas. Aplicado en `_ingest_transactions`, `_transform_data` y `_quality_check`.
- **7.3 — Orquestación de Datos**: DAG con dependencias explícitas (`>>`), tareas paralelas, retries con backoff exponencial, idempotencia.
- **7.4 — Apache Airflow (Orquestación Profesional)**: `PostgresHook` + `PostgresOperator`, `LocalExecutor`, `default_args`, gestión de credenciales por variables de entorno (nota sobre secretos productivos en sección Decisiones técnicas).

### Relación con el proyecto ejemplo Black Friday

SecureLink usa el mismo **esqueleto canónico** del ejemplo en tres dimensiones:

**1. Estructura del DAG**
- Patrón `validate → extract/load_refs → transform → load → quality_check` (idéntico al de Black Friday `validate → extract → transform → load → quality_check`)
- `default_args` con `retries=3` + `retry_exponential_backoff=True`
- Validación de conectividad al DWH antes de procesar (`validate_dwh` con `SELECT 1`)
- `quality_check` final que tira excepción si hay datos inválidos
- Tareas paralelas con `[t1, t2] >> t3`
- Una función `_funcion()` por tarea (convención de prefijo guión bajo)
- Logging por tarea con `logging.getLogger(__name__)`

**2. Infraestructura Docker**
- Mismo esqueleto del `docker-compose.yml`: `postgres-airflow` (metadata) + `postgres-dwh` (DWH) + `airflow-init` (migración + admin user) + `airflow-scheduler` + `airflow-webserver`
- Mismas convenciones: health checks con `pg_isready`, volúmenes `./dags`, `./logs`, `./data`, `depends_on` con `condition: service_healthy`
- Mismas variables de entorno de Airflow (`LocalExecutor`, `FERNET_KEY`, `LOAD_EXAMPLES=False`, `DAGS_ARE_PAUSED_AT_CREATION=True`, `BASIC_AUTH`)
- `Dockerfile.airflow` parte del mismo template (`apache/airflow:2.7.3-python3.11` + constraints URL + mismas dependencias base)

**3. Filosofía de idempotencia**
- El ejemplo usa `ON CONFLICT DO UPDATE` (UPSERT) porque cada corrida agrega pocas filas
- SecureLink usa `TRUNCATE + COPY` atómico (refresh completo) porque cada corrida reemplaza 13M filas. Diferente patrón, mismo objetivo: que reejecutar el DAG no rompa el dashboard ni duplique datos

### Diferencias respecto al ejemplo (con justificación)

| Aspecto | Black Friday | SecureLink | Por qué difiere |
|---|---|---|---|
| **Schedule** | `*/1 * * * *` (cada minuto) | `@daily` | Datos en vivo vs dataset histórico estático |
| **Fuentes** | PostgreSQL + API REST | 3 CSV + 2 JSON | El cliente entrega dataset histórico, no sistemas productivos |
| **Estrategia ingesta** | CDC incremental (`last_updated`) | Full refresh | Dataset estático: no hay cambios incrementales que capturar |
| **Volumen por corrida** | Decenas de filas | 13 M filas | Ordenes de magnitud distintas |
| **Engine** | pandas | Polars + DuckDB | El volumen masivo necesita engines optimizados (pandas es single-thread) |
| **Comunicación entre tareas** | XCom | Parquet/pickle en `/tmp` | XCom serializa en la metadata DB; 13M filas la rompen |
| **Carga al DWH** | `INSERT ON CONFLICT DO UPDATE` | `TRUNCATE + COPY` | UPSERT es lento con 13M filas; full refresh atómico es más eficiente |
| **Servicios extras** | `postgres-source` + `api-sales` | (ninguno) | Las fuentes son archivos, no sistemas externos |
| **Dashboard / BI** | Metabase (tool genérico) | Streamlit (custom) | Quisimos un dashboard con narrativa propia (4 reglas, datasets, recomendaciones) |
| **Webserver Airflow** | Config estándar | Worker `gevent` + timeouts extendidos | Para máquinas del equipo con RAM limitada (4-6 GB) |
| **Dependencias del Dockerfile** | + `polars` + `pyarrow` (preinstaladas por el profe) | + `polars` + `duckdb` + `gevent` | Usamos las que ya venían + agregamos las que necesitábamos |

### Cumplimiento del SRS

- **SRS sección 1 (objetivos)**: detección de fraude en transacciones financieras → implementado con el sistema de puntaje ponderado.
- **SRS sección 2.b (interfaces de usuario)**: dashboard analítico → Streamlit con 5 vistas (General, Usuario, Comercios, Explorador con Filtros, Análisis de Datasets).
- **SRS sección 2.d (evolución previsible)**: detección en tiempo real, modelo ML, alertas → documentado el diseño en la sección [Evolución a tiempo real](#evolución-a-tiempo-real) (no implementado en el MVP).
- **SRS sección 2.c (interfaces de hardware)**: cualquier PC con navegador web actualizado → cumplido (todo corre en Docker, único requisito es Docker Desktop).

**Requisitos funcionales (RF) — cobertura:**

| RF | Estado | Dónde |
|---|---|---|
| RF01-RF05 (carga, validación, limpieza, joins) | ✅ | `validate_files`, `transform_data`, joins en `ingest_transactions` |
| RF06 (indicadores derivados: ratio, frecuencia/tarjeta, distancia) | ✅ | `debt_income_ratio`, `card_txn_count`, `distance_km` (Haversine a centroide de estado) |
| RF07 (labels como verdad) | ✅ | `is_fraud` desde `train_fraud_labels.json` |
| RF08 (reglas: monto, error, distancia) | ✅ | Puntaje ponderado con señal de distancia inusual (>500 km) |
| RF09 (tasa fraude, monto en riesgo, FP/FN) | ✅ | `fraud_metrics_global` + matriz de confusión |
| RF10-RF11 (panel general) | ✅ | Panel General |
| RF12-RF13 (panel usuario + mapa) | ✅ | Panel de Usuario con choropleth de transacciones |
| RF14-RF15 (panel comercios) | ✅ | Panel de Comercios |
| RF16 (filtros combinados) | ✅ | Panel "Explorador con Filtros" |
| RF17 (mapa choropleth EEUU) | ✅ | Panel General |
| RF18 (manejo de Online sin geo) | ✅ | `transaction_type`, distancia null en Online |
| RF19-RF20 (errores claros, recarga de datos) | ✅ | Validaciones + re-ejecución del pipeline |

> **Nota sobre RF06/RF08 (distancia geográfica)**: el dataset no incluye las coordenadas exactas del comercio (solo `merchant_state`). La distancia se aproxima usando el **centroide geográfico del estado** del comercio. Es una aproximación a nivel estado, documentada y suficiente para detectar transacciones lejanas al domicilio del usuario.

---

## Referencias técnicas externas

- Apache Airflow docs: https://airflow.apache.org/docs/
- Polars docs: https://docs.pola.rs/
- DuckDB docs: https://duckdb.org/docs/
- PostgreSQL COPY: https://www.postgresql.org/docs/current/sql-copy.html
