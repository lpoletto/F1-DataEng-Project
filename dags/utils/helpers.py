from datetime import datetime, timedelta
from os import environ as env
import logging
from minio import Minio
import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, regexp_extract, concat

# Ruta base para capa bronze
BRONZE_LAYER_PATH = env["BRONZE_LAYER_PATH"]


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