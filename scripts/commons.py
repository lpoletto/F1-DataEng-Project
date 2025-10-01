from os import environ as env
from pyspark.sql import SparkSession

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