from os import environ as env
import logging
from minio import Minio
import psycopg2
from datetime import datetime, timedelta

def create_bucket(bucket_name, region=None) -> bool:
    """Create an bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
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
            print(f"Bucket s3a://{minio_bucket} created.")
        else:
            print(f"Bucket {minio_bucket} already exists.")
            return False
    except Exception as e:
        logging.error(e)
        return False
    return True


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
                print(f"La base de datos '{db_name}' ya existe.")
                conn.close()
                return db_name
            else:
                cur.execute(f"CREATE DATABASE {db_name}")
                print(f"Base de datos '{db_name}' creada exitosamente.")
                conn.close()
                return db_name
    except Exception as e:
        print(f"Error al crear la base de datos: {e}")
        return None
    finally:
        conn.close()


def execute_sql_query(sql_query: str, db_name: str) -> None:  
    conn = None
    # Conectamos a la base de datos
    conn = get_connection(db_name)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql_query)
                print(sql_query)
                print("\nQuery executed successfully")
    except Exception as e:
        print(f"Error executing SQL query: {e}")
    finally:
        conn.close()
        print("Conexión cerrada.")