from datetime import datetime, timedelta
from os import environ as env
import logging
from minio import Minio
import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, regexp_extract, concat, date_format, to_timestamp, col,  regexp_replace, length, lpad

# Ruta base para capa bronze
BRONZE_LAYER_PATH = env["BRONZE_LAYER_PATH"]
SILVER_LAYER_PATH = env["SILVER_LAYER_PATH"]
GOLD_LAYER_PATH = env["GOLD_LAYER_PATH"]
DB_NAME = env["POSTGRES_DB"]
DB_USER = env["POSTGRES_USER"]
DB_PASSWORD = env["POSTGRES_PASSWORD"]
DB_HOST = env["POSTGRES_HOST"]
DB_PORT = env["POSTGRES_PORT"] 
F1_DB = env["F1_DWH"]
STG_SCHEMA = env["STG_SCHEMA"]
F1_GOLD_SCHEMA = env["GOLD_SCHEMA"]


def create_bucket(bucket_name, region=None):
    """Crea un bucket en una región especificada.

    Si no se especifica una región, el bucket se crea en la región por defecto
    de S3 (us-east-1).

    :param bucket_name: Bucket a crear
    :param region: Cadena de texto con la región donde crear el bucket, p. ej., 'us-west-2'
    :return: True si el bucket fue creado, en caso contrario False
    """

    # Create bucket
    try:
        client = Minio(
            "minio:9000",
            access_key=env["MINIO_ROOT_USER"],
            secret_key=env["MINIO_ROOT_PASSWORD"],
            secure=False
        )

        minio_bucket = bucket_name

        found = client.bucket_exists(minio_bucket)
        if not found:
            client.make_bucket(minio_bucket)
            print(f"\n################## Bucket s3a://{minio_bucket} created. ##################\n")
            uri_bucket = f"s3a://{minio_bucket}"
            return uri_bucket
        else:
            print(f"\n################## Bucket {minio_bucket} already exists. ##################\n")
            return None
    except Exception as e:
        logging.error(e)
        return None


def get_connection(db_name: str = "postgres"):
    """Establece una conexión a la base de datos PostgreSQL."""
    
    conn = psycopg2.connect(
        dbname=db_name,
        user=env["POSTGRES_USER"],
        host=env["POSTGRES_HOST"],
        password=env["POSTGRES_PASSWORD"],
        port=env["POSTGRES_PORT"],
    )
    return conn


def create_a_database(db_name: str):
    """
    Verifica si una base de datos existe. Si no existe, la crea.
    """
    conn = get_connection() 
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            if cur.fetchone():
                print(f"\n################## La base de datos '{db_name}' ya existe. ##################\n")
                conn.close()
                return db_name
            else:
                cur.execute(f"CREATE DATABASE {db_name}")
                print(f"\n################## Base de datos '{db_name}' creada exitosamente. ##################\n")
                conn.close()
                return db_name
    except Exception as e:
        print(f"\n################## Error al crear la base de datos: {e} ##################\n")
        return None
    finally:
        conn.close()


def execute_sql_query(sql_query: str, db_name: str):  
    conn = None
    # Conectamos a la base de datos
    conn = get_connection(db_name)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql_query)
                print(f"\n################## sql_query ##################\n")
                print(sql_query)
                print("\n################## Query executed successfully ##################\n")
    except Exception as e:
        print(f"\n################## Error executing SQL query: {e} ##################\n")
    finally:
        conn.close()
        print("\n################## Conexión cerrada. ##################\n")


def fetch_sql_query_result(sql_query: str, db_name: str):
    """
    Ejecuta una consulta SQL y retorna los resultados en una lista.
    """
    conn = None
    results = None
    conn = get_connection(db_name)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql_query)
                results = cur.fetchall()
    except Exception as e:
        print(f"\n################## Error fetching SQL query result: {e} ##################\n")
    finally:
        conn.close()
    return results


def get_spark_session() -> SparkSession:
    """
    Crea y retorna una session de Spark.
    """
    # Configuración de SparkSession con soporte s3 y MinIO
    DRIVER_PATH = env["DRIVER_PATH"]

    spark = SparkSession.builder.master("local[1]") \
        .appName("ETL Spark") \
        .config("spark.jars", DRIVER_PATH) \
        .config("spark.executor.extraClassPath", DRIVER_PATH) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", env["MINIO_ROOT_USER"]) \
        .config("spark.hadoop.fs.s3a.secret.key", env["MINIO_ROOT_PASSWORD"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
    
    spark.conf.set("spark.sql.session.timeZone", "America/Argentina/Buenos_Aires")
    
    return spark


def add_ingestion_date(input_df, custom_date_column="ingestion_date"):
    # yesterday = datetime.now() - timedelta(days=1)
    # v_date = yesterday.strftime("%Y-%m-%d") # Formato: 2025-06-10
    v_date = current_timestamp()
    output_df = input_df.withColumn(custom_date_column, v_date)
    return output_df


def ingest_to_bronze(spark,
    table_name: str,
    query: str,
    execution_date: str,
    jdbc_driver: str = "com.mysql.cj.jdbc.Driver"
):
    """
    Función genérica para extraer datos desde MySQL mediante una query SQL
    y guardarlos en la capa Bronze.

    Parámetros:
    ----------
    spark : SparkSession
        Sesión activa de Spark.
    table_name : str
        Nombre de la tabla o nombre lógico del dataset (se usa en el path de salida).
    query : str
        Query SQL a ejecutar en la base de datos origen.
    execution_date : str
        Fecha de ejecución (por ejemplo, '2025-10-22') usada en la ruta de salida.
    jdbc_driver : str
        Driver JDBC (por defecto MySQL, pero puede cambiarse a PostgreSQL, Redshift, etc.)
    """

    v_file_date = execution_date
    print(f"\n################## Starting ingestion of '{table_name}' ##################\n")

    # Configuración conexión JDBC
    jdbc_url = f"jdbc:mysql://mysql:{env['MYSQL_PORT']}/{env['MYSQL_DATABASE']}"
    jdbc_properties = {
        "user": env["MYSQL_USER"],
        "password": env["MYSQL_PASSWORD"],
        "driver": jdbc_driver
    }

    print("\n################## Step 1 - Read data from MySql database ##################\n")
    try:
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("driver", jdbc_properties["driver"])
            .option("query", query)
            .option("user", jdbc_properties["user"])
            .option("password", jdbc_properties["password"])
            .load()
        )
        print("\n✅ Connection successful.\n")

    except Exception as e:
        print(f"\n❌ Connection failed: {str(e)}")
        raise

    # Si el dataframe contiene la columna "time", la normalizamos a formato HH:mm:ss
    if "time" in df.columns:
        # 1 Aseguramos que las horas tengan dos dígitos (ej. "2:05:05.152" → "02:05:05.152")
        df = df.withColumn(
            "time",
            regexp_replace(col("time"), r"^(\d):", r"0\1:")
        )

        # 2️ Convertimos el string a timestamp considerando los milisegundos
        df = df.withColumn(
            "time",
            to_timestamp("time", "HH:mm:ss.SSS")
        )

        # 3️ Lo devolvemos al formato string estándar (opcional)
        df = df.withColumn(
            "time",
            date_format(col("time"), "HH:mm:ss")
        )

    # Mostrar preview
    df.show(10, truncate=False)
    df.printSchema()

    # Path de salida
    output_path = f"{BRONZE_LAYER_PATH}/{v_file_date}/{table_name}"
    print(f"\n################## Step 2 - Writing in Bronze layer ##################\n")
    print(f"Ruta destino: {output_path}")

    try:
        df.write.mode("overwrite").csv(output_path, header=True)
        print(f"\n✅ Saved successfully: {output_path}\n")
    except Exception as e:
        print(f"\n❌ Error saving in Bronze: {str(e)}")
        raise

    # return output_path


def create_dim_date(end_date):
    # Create table if not exists
    sql_query = f"""
    CREATE TABLE IF NOT EXISTS {F1_GOLD_SCHEMA}.dim_date (
        date_id               BIGINT NOT NULL,
        date              	  DATE NOT NULL,
        weekday               VARCHAR(9) NOT NULL,
        weekday_num           INT NOT NULL,
        day_month             INT NOT NULL,
        day_of_year           INT NOT NULL,
        week_of_year          INT NOT NULL,
        iso_week         	  CHAR(10) NOT NULL,
        month_num             INT NOT NULL,
        month_name            VARCHAR(9) NOT NULL,
        month_name_short   	  CHAR(3) NOT NULL,
        quarter      		  INT NOT NULL,
        year              	  INT NOT NULL,
        first_day_of_month    DATE NOT NULL,
        last_day_of_month     DATE NOT NULL,
        yyyymm                CHAR(7) NOT NULL,
        weekend_indr          CHAR(10) NOT NULL,
        CONSTRAINT dim_date_pk PRIMARY KEY (date_id)
    );
    """
    print("\n################## Creating dim_date table if not exists ##################\n")
    execute_sql_query(sql_query, F1_DB)

    sql_query = f"""
    CREATE INDEX IF NOT EXISTS d_date_date_actual_idx
    ON {F1_GOLD_SCHEMA}.dim_date(date);
    """
    print("\n################## Creating index on dim_date.date ##################\n")
    execute_sql_query(sql_query, F1_DB)

    # Insert a default row for unknown date
    sql_query = f"""
    SELECT COUNT(*) FROM {F1_GOLD_SCHEMA}.dim_date WHERE date_id = -1;
    """
    print("\n################## Inserting default row for unknown date if not exists ##################\n")
    result_query = fetch_sql_query_result(sql_query, F1_DB)

    if result_query[0][0] == 0:
        sql_query = f"""
        INSERT INTO {F1_GOLD_SCHEMA}.dim_date
        VALUES(
            19000101,
            '1900-01-01',
            'Monday',
            1,
            1,
            1,
            1,
            '1900-W01-1',
            1,
            'January',
            'Jan',
            1,
            1900,
            '1900-01-01',
            '1900-01-31',
            '190001',
            'weekday'
        );
        """
        execute_sql_query(sql_query, F1_DB)

    print(f"\n################## Inserting all dates from 1950-01-01 to {end_date} ##################\n")
    # Insert all dates from 1950-01-01 to 2026-01-01
    sql_query = f"""
        INSERT INTO {F1_GOLD_SCHEMA}.dim_date
        SELECT 
            TO_CHAR(datum::DATE, 'yyyymmdd')::BIGINT AS date_id,
            datum::DATE AS date,
            TO_CHAR(datum, 'TMDay') AS weekday,
            EXTRACT(ISODOW FROM datum) AS weekday_num,
            EXTRACT(DAY FROM datum) AS day_month,
            EXTRACT(DOY FROM datum) AS day_of_year,
            EXTRACT(WEEK FROM datum) AS week_of_year,
            EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS iso_week,
            EXTRACT(MONTH FROM datum) AS month,
            TO_CHAR(datum, 'TMMonth') AS month_name,
            TO_CHAR(datum, 'Mon') AS month_name_short,
            EXTRACT(QUARTER FROM datum) AS quarter,
            EXTRACT(YEAR FROM datum) AS year,
            datum::DATE + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
            (DATE_TRUNC('MONTH', datum)::DATE + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
            TO_CHAR(datum, 'yyyy-mm') AS mmyyyy,
            CASE
                WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN 'weekend'
                ELSE 'weekday'
            END AS weekend_indr
        FROM (
            SELECT generate_series('1950-01-01'::DATE, '{end_date}'::DATE, INTERVAL '1 day') AS datum
        ) DQ;
        """
    execute_sql_query(sql_query, F1_DB)