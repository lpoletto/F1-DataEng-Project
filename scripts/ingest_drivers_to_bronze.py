from datetime import datetime
import sys
from os import environ as env
from helpers import *


if __name__ == "__main__":
    spark = get_spark_session()
    execution_date = sys.argv[1].strip()
    table_name = "drivers"

    sql_query = f"SELECT * FROM f1db.{table_name}"

    print(f"\nFecha de ejecución: {execution_date}")
    print(f"Tabla/Query a procesar: {table_name}")

    ingest_to_bronze(spark, table_name, sql_query, execution_date)
    spark.sparkContext.stop()