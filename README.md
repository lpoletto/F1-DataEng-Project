(🛠️⚠️ En desarrollo...)
# F1-DataEng-Project 🏎️ 🏁

Solución End-to-End de Ingeniería de Datos inspirada en la Fórmula 1, integrando tecnologías open source para ingesta, procesamiento, almacenamiento y análisis de datos. El diseño es escalable y modular, permitiendo ejecución local con Docker y fácil migración a la nube (AWS u otros).

---

## 🏗️ Arquitectura del Proyecto

![arquitectura-img](arquitectura.png)

---

## 📋 Prerrequisitos

- Docker y Docker Compose instalados.
- Acceso a DockerHub para descargar imágenes.
- Conexión a internet para descargar drivers y dependencias.
- (🛠️⚠️ En desarrollo...)

---

## 🚀 Instalación

1. **Crear carpetas necesarias**  
   Ejecuta en la raíz del proyecto:
   ```bash
   mkdir -p ./dags ./logs ./plugins ./config ./scripts ./spark_drivers ./data/{raw,staging,processed} ./database
   ```

2. **Permisos recomendados para logs y data**  
   ```bash
   sudo chown -R 50000:50000 ./logs
   sudo chown -R 50000:50000 ./data
   ```

3. **Crear archivo `.env`**  
   Ubícalo junto a `docker-compose.yml` con el siguiente contenido:
   ```bash
   # Variables para Airflow
   AIRFLOW_UID=50000

   # Variables para Postgres
   POSTGRES_HOST=postgres # YOUR_POSTGRES_HOST
   POSTGRES_PORT=5432 # YOUR_POSTGRES_PORT
   POSTGRES_DB=postgres # YOUR_POSTGRES_DB
   POSTGRES_SCHEMA=public # YOUR_POSTGRES_SCHEMA
   POSTGRES_USER=airflow # YOUR_POSTGRES_USER
   POSTGRES_PASSWORD=airflow # YOUR_POSTGRES_PASSWORD
   POSTGRES_URL="jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}?user=${POSTGRES_USER}&password=${POSTGRES_PASSWORD}"
   DRIVER_PATH=/tmp/drivers/postgresql-42.5.2.jar,/tmp/drivers/mysql-connector-j-8.0.32.jar,/tmp/drivers/hadoop-aws-3.3.1.jar,/tmp/drivers/aws-java-sdk-bundle-1.11.375.jar

   F1_DWH=f1_dwh
   BRONZE_SCHEMA=f1_bronze
   SILVER_SCHEMA=f1_silver
   GOLD_SCHEMA=f1_gold
   STG_SCHEMA=f1_stg

   # Variables para MySQL
   MYSQL_ROOT_PASSWORD=rootpassword
   MYSQL_DATABASE=f1db
   MYSQL_USER=f1user
   MYSQL_PASSWORD=f1password
   MYSQL_PORT=3306
   MYSQL_HOST=mysql

   # Variables para MinIO
   MINIO_ROOT_USER=minio
   MINIO_ROOT_PASSWORD=minio123


   # Variables para Notebooks
   DATA_SOURCE="f1db"
   DATA_SOURCE_MANUAL="Carga manual - "
   DATA_SOURCE_API="Ergast API"
   BRONZE_LAYER_PATH="s3a://bronze"
   SILVER_LAYER_PATH="s3a://silver"
   GOLD_LAYER_PATH="s3a://gold"
   ```

4. Descargar Drivers JDBC y JARs

Ve al directorio `spark_drivers` y descargar los JARs necesarios:

```bash
cd /spark_drivers
```

Ejecuta los siguientes comandos para descargar los drivers:

```bash
wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.2/postgresql-42.5.2.jar
wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar
```

5. **Descargar imágenes de Airflow y Spark**  
   Si hay error, haz login en DockerHub.
   ```bash
   docker pull lpoletto/airflow:airflow_2_6_2
   docker pull lpoletto/spark:spark_3_4_1
   ```

6. **(Opcional) Construir imágenes desde Dockerfiles**  
   Los Dockerfiles están en `docker_images/`.

7. **Levantar servicios**  
   ```bash
   docker-compose up -d
   ```

8. Una vez que los servicios estén levantados, ingresar a Airflow en `http://localhost:8080/`.

9. En la pestaña `Admin -> Connections` crear una nueva conexión con los siguientes datos para Postgres:
    * Conn Id: `postgres_default`
    * Conn Type: `Postgres`
    * Host: `postgres` (El nombre del servicio de PostgreSQL (ej. *postgres*), o *host.docker.internal* si la base de datos está fuera de la red Docker.)
    * Schema(Database): `nombre de la db de Postgres`
    * User: `usuario de Postgres`
    * Password: `contraseña de Postgres`
    * Port: `5432`

10. **Conexión a MySQL**  
    - Similar a la anterior.

11. **Conexión a Spark**  
    - Conn Id: `spark_default`  
    - Conn Type: `Spark`  
    - Host: `spark://spark`  
    - Port: `7077`  
    - Extra: `{"queue": "default"}`

12. **Conexión a AWS S3 (MinIO)**  
    - Conn Id: `aws_default`  
    - Conn Type: `Amazon Web Services`  
    - AWS Access Key ID: `minio`  
    - AWS Secret Access Key: contraseña de MinIO  
    - Extra: `{"region_name": "us-east-1","endpoint_url": "http://minio:9000","use_ssl": false}`

12. En la pestaña `Admin -> Variables` crear una nueva variable con los siguientes datos:
    * Key: `driver_class_path`
    * Value: `/tmp/drivers/postgresql-42.5.2.jar:/tmp/drivers/mysql-connector-j-8.0.32.jar:/tmp/drivers/hadoop-aws-3.3.1.jar:/tmp/drivers/aws-java-sdk-bundle-1.11.375.jar`

13. En la pestaña `Admin -> Variables` crear una nueva variable con los siguientes datos:
    * Key: `spark_scripts_dir`
    * Value: `/opt/airflow/scripts`

14. En la pestaña `Admin -> Variables` crear las variables con los siguientes datos:
    * Key: `raw_data_dir`
    * Value: `/opt/airflow/data/raw`
    * Key: `staging_data_dir`
    * Value: `/opt/airflow/data/staging`
    * Key: `processed_data_dir`
    * Value: `/opt/airflow/data/processed`

    **Nota:** Añadir cualquier otra variable que consideren necesaria para sus scripts o DAGs, dependiendo de los requerimientos específicos del proyecto.

15. Ejecutar el DAG: `start_up_init.py` para configurar el entorno de datos (db,schemas,buckets).

## 🐋 Comandos utilies de Docker
Si experimienta algun fallo o que no se visualice algun dag, reiniciar los servicios:
```bash
docker compose down
docker compose up -d
```

Si cambió algo en el `docker-compose.yml`, entonces:
```bash
docker compose down
docker compose up -d --build
```

Si desea volver a generar todos los servicios con sus volumenes:
```bash
docker compose down -v
docker compose up -d
```

---

## 📊 Consultas SQL de Ejemplo

Ejemplos de queries para analizar los datos en las vistas del Data Warehouse:

### Campeonatos de pilotos por temporada

```sql
CREATE OR REPLACE VIEW f1_gold.vw_driver_standings
AS 
WITH ranked_standings AS (
    SELECT ds.driver_id,
        d.driver_name,
        d.driver_nationality,
        ds.points,
        ds.wins,
        ds.rank,
        ds.race_id,
        dd.year AS race_year,
        ds.date_id,
        row_number() OVER (PARTITION BY ds.driver_id, dd.year ORDER BY ds.date_id DESC) AS rn
    FROM f1_gold.fact_driver_standings ds
    LEFT JOIN f1_gold.dim_date dd 
		ON ds.date_id = dd.date_id
    LEFT JOIN f1_gold.dim_driver d 
		ON ds.driver_id = d.driver_id
)
SELECT distinct 
    rs.race_id,
    rs.date_id,
    rs.race_year,
    rs.rank,
    rs.driver_name,
    rs.driver_nationality,
    rs.driver_id,
    dc.constructor_id,
    dc.constructor_name,
    rs.points,
    rs.wins
FROM ranked_standings rs
LEFT JOIN f1_gold.fact_race_results frr 
	ON rs.driver_id = frr.driver_id AND rs.race_id = frr.race_id
INNER JOIN f1_gold.dim_constructor dc 
	ON frr.constructor_id = dc.constructor_id
WHERE rs.rn = 1
ORDER BY rs.date_id DESC, rs.points DESC;
```

### Campeonatos de constructores por temporada

```sql
CREATE OR REPLACE VIEW f1_gold.vw_constructor_standings
AS 
WITH ranked_standings AS (
         SELECT cs.constructor_id,
            c.constructor_name,
            c.constructor_nationality,
            cs.points,
            cs.wins,
            cs.rank,
            cs.race_id,
            dd.year AS race_year,
            cs.date_id,
            row_number() OVER (PARTITION BY cs.constructor_id, dd.year ORDER BY cs.date_id DESC) AS rn
           FROM f1_gold.fact_constructor_standings cs
           LEFT JOIN f1_gold.dim_date dd ON cs.date_id = dd.date_id
           LEFT JOIN f1_gold.dim_constructor c ON cs.constructor_id = c.constructor_id
)
select distinct
    rs.race_id,
    rs.date_id,
    rs.race_year,
    rs.rank,
    dc.constructor_id,
    rs.constructor_name,
    rs.constructor_nationality,
    rs.points,
    rs.wins
FROM ranked_standings rs
LEFT JOIN f1_gold.fact_race_results frr ON rs.constructor_id = frr.constructor_id AND rs.race_id = frr.race_id
JOIN f1_gold.dim_constructor dc ON frr.constructor_id = dc.constructor_id
WHERE rs.rn = 1
ORDER BY rs.date_id DESC, rs.points DESC;
```

### Campeonatos de pilotos histórico

```sql
create or replace view vw_drivers_championships AS
select distinct driver_id, driver_name, count(*) as total_championships
from f1_gold.vw_driver_standings
where "rank" = 1
group by driver_id, driver_name
order by total_championships desc;
```

### Campeonatos de constructores histórico

```sql
CREATE OR REPLACE VIEW f1_gold.vw_constructors_championships
AS 
SELECT distinct
  constructor_id,
  constructor_name,
  count(*) AS total_championships
FROM f1_gold.vw_constructor_standings
WHERE "rank" = 1
GROUP BY constructor_id, constructor_name
ORDER BY total_championships DESC;
```

---

**Notas:**
- Revisa los valores de las variables y rutas según tu entorno.
- Consulta la documentación oficial de cada tecnología para detalles avanzados.