from os import environ as env
import logging
from minio import Minio
import psycopg2
from datetime import datetime, timedelta
from pyspark.sql import SparkSession


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
    spark = SparkSession.builder \
        .appName("PySpark's Cluster") \
        .config("spark.jars", env["DRIVER_PATH"]) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", env["MINIO_ROOT_USER"]) \
        .config("spark.hadoop.fs.s3a.secret.key", env["MINIO_ROOT_PASSWORD"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    return spark