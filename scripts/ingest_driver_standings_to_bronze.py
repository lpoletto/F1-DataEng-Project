from datetime import datetime
import sys
from os import environ as env
from helpers import *


if __name__ == "__main__":
    spark = get_spark_session()
    execution_date = sys.argv[1].strip()
    table_name = "driverStandings"
    
    print(f"\nFecha de ejecución: {execution_date}")
    print(f"Tabla/Query a procesar: {table_name}")

    sql_query = f"""
    SELECT ds.*
    FROM f1db.{table_name} ds
    INNER JOIN f1db.races r on ds.raceId = r.raceId
    WHERE r.`date` BETWEEN DATE_SUB('{execution_date}', INTERVAL 30 DAY) and '{execution_date}'
    """
   
    ingest_to_bronze(spark, "driver_standings", sql_query, execution_date)   
    # Detener la sesión de Spark
    spark.sparkContext.stop()