from datetime import datetime
import sys
from os import environ as env
from helpers import *


if __name__ == "__main__":
    spark = get_spark_session() # Configuración de SparkSession con soporte S3
    execution_date = sys.argv[1] # datetime.now().strftime("%Y-%m-%d")
    table_name = "races"
    
    print(f"\nFecha de ejecución: {execution_date}")
    print(f"Tabla/Query a procesar: {table_name}")

    sql_query = f"SELECT * FROM f1db.{table_name}"
   
    ingest_to_bronze(spark, table_name, sql_query, execution_date)   
    # Detener la sesión de Spark
    spark.sparkContext.stop()