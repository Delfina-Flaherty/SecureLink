# 🔒 SecureLink – Sistema de Detección de Fraude

## Cómo levantar el proyecto desde cero

### Paso 1: Estructura de carpetas
El proyecto tiene que quedar así en tu computadora:

```
securelink/
├── docker-compose.yml
├── Dockerfile.airflow
├── Dockerfile.streamlit
├── init_sql/
│   └── init_dwh.sql
├── dags/
│   └── securelink_fraud_pipeline.py
├── dashboard/
│   └── app.py
├── data/
│   ├── transactions_data.csv
│   ├── users_data.csv
│   ├── cards_data.csv
│   ├── train_fraud_labels.json
│   └── mcc_codes.json
└── logs/               ← Se crea sola
```

---

### Paso 2: Copiar los archivos de datos

Copiá tus 5 archivos de datos a la carpeta `data/`:
- `transactions_data.csv`
- `users_data.csv`
- `cards_data.csv`
- `train_fraud_labels.json`
- `mcc_codes.json`

---

### Paso 3: Levantar todo con Docker

Abrí una terminal en la carpeta `securelink/` y ejecutá:

```bash
docker compose up -d --build
```

Esto va a:
1. Descargar las imágenes de Docker (solo la primera vez, puede tardar unos minutos)
2. Construir las imágenes personalizadas (Airflow con pandas, Streamlit)
3. Levantar todos los contenedores

Para ver si todo está corriendo:
```bash
docker compose ps
```

Deberías ver todos los contenedores con estado `healthy` o `running`.

---

### Paso 4: Entrar a Airflow

Abrí el navegador y andá a:
**http://localhost:8080**

- Usuario: `admin`
- Contraseña: `admin`

---

### Paso 5: Correr el pipeline

1. En Airflow, buscá el DAG llamado `securelink_fraud_pipeline`
2. Si aparece en gris (pausado), hacé click en el toggle para activarlo
3. Hacé click en el botón ▶️ (Trigger DAG) para correrlo manualmente
4. Podés ver el progreso haciendo click en el DAG y luego en "Graph"

El pipeline tiene 8 pasos. Dependiendo del tamaño del dataset puede tardar varios minutos.

---

### Paso 6: Ver el dashboard

Una vez que el pipeline terminó con éxito, abrí:
**http://localhost:8501**

---

### Comandos útiles

```bash
# Ver logs de un contenedor específico
docker compose logs airflow-scheduler -f

# Ver logs del pipeline en tiempo real
docker compose logs airflow-scheduler --tail=50

# Parar todo
docker compose down

# Parar y borrar los datos de la base
docker compose down -v

# Reconstruir la imagen (si modificás el Dockerfile)
docker compose up -d --build
```

---

### ¿Por qué el pipeline tarda?

El archivo `transactions_data.csv` pesa 1.2 GB. El pipeline lo lee en bloques
de 100.000 filas para no quedarse sin memoria. Esto es lo correcto para
datasets grandes, pero lleva su tiempo.


---

### Puertos utilizados

| Servicio | URL |
|---|---|
| Airflow UI | http://localhost:8080 |
| Streamlit Dashboard | http://localhost:8501 |
| PostgreSQL DWH | localhost:5433 (si necesitás conectarte con DBeaver) |
