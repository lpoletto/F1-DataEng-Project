from datetime import datetime
import sys
from os import environ as env
from helpers import *


if __name__ == "__main__":
    spark = get_spark_session() # Configuración de SparkSession con soporte S3
    execution_date = sys.argv[1].strip() 
    table_name = "results"
    
    print(f"\nFecha de ejecución: {execution_date}")
    print(f"Tabla/Query a procesar: {table_name}")

    sql_query = f"""
    SELECT resultId, 
        res.raceId, 
        driverId, 
        constructorId, 
        `number`, 
        grid, 
        `position`, 
        positionText, 
        positionOrder, 
        points, 
        laps, 
        res.`time`, 
        milliseconds, 
        fastestLap, 
        `rank`, 
        fastestLapTime, 
        fastestLapSpeed, 
        statusId
    FROM f1db.{table_name} res
    INNER join f1db.races r on res.raceId = r.raceId
    WHERE r.`date` = '{execution_date}'
    """
   
    ingest_to_bronze(spark, table_name, sql_query, execution_date)   
    # Detener la sesión de Spark
    spark.sparkContext.stop()