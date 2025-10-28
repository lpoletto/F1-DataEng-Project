from datetime import datetime
import sys
from os import environ as env
from helpers import *


if __name__ == "__main__":
    spark = get_spark_session()
    execution_date = sys.argv[1]
    table_name = "pitStops"
    
    print(f"\nFecha de ejecución: {execution_date}")
    print(f"Tabla/Query a procesar: {table_name}")

    sql_query = f"""
    SELECT ps.raceId, 
        driverId, 
        stop, 
        lap, 
        ps.`time`, 
        duration, 
        milliseconds
    FROM f1db.{table_name} ps
    INNER JOIN f1db.races r on ps.raceId = r.raceId
    WHERE r.`date` = '{execution_date}'
    """
   
    ingest_to_bronze(spark, "pit_stops", sql_query, execution_date)
    # Detener la sesión de Spark
    spark.sparkContext.stop()