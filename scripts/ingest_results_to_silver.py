import sys
from datetime import datetime
from os import environ as env
from helpers import *

from pyspark.sql.functions import count, sum, when, col, desc, row_number, asc, lit
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType


BRONZE_LAYER_PATH = env["BRONZE_LAYER_PATH"]
SILVER_LAYER_PATH = env["SILVER_LAYER_PATH"]

def ingest_results_to_silver(spark, execution_date, output_path):
    v_file_date = execution_date.strip()
    v_data_source = f"{BRONZE_LAYER_PATH}/{v_file_date}/results"
    input_path = v_data_source

    # Defenición del esquema para el DataFrame
    results_schema = StructType(fields=[
        StructField("resultId", IntegerType(), False),
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), False),
        StructField("constructorId", IntegerType(), False),
        StructField("number", IntegerType(), True),
        StructField("grid", IntegerType(), False),
        StructField("position", IntegerType(), True),
        StructField("positionText", StringType(), False),
        StructField("positionOrder", IntegerType(), False),
        StructField("points", FloatType(), False),
        StructField("laps", IntegerType(), False),
        StructField("time", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
        StructField("fastestLap", IntegerType(), True),
        StructField("rank", IntegerType(), True),
        StructField("fastestLapTime", StringType(), True),
        StructField("fastestLapSpeed", StringType(), True),
        StructField("statusId", IntegerType(), False)
    ])

    print("\n################## Step 1 - Read the results data from the Bronze layer ##################\n")   
    
    results_df = spark.read.option("header", True) \
    .schema(results_schema) \
    .format("csv") \
    .load(input_path)

    print("\n################## Step 2 - Rename the columns as required ##################\n")
    
    results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .withColumnRenamed("statusId", "status_id") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date).cast(DateType()))

    print("\n################## Step 3 - Add race_timestamp and ingestion date to the dataframe (audit field) ##################\n")
    results_final_df = add_ingestion_date(results_with_columns_df)

    print("\n################## Step 4 - Order columns in a DataFrame ##################\n")
    
    column_order = ["result_id",
        "race_id",
        "driver_id",
        "constructor_id",
        "status_id",
        "grid",
        "position",
        "position_text",
        "position_order",
        "points",
        "laps",
        "time",
        "milliseconds",
        "fastest_lap",
        "rank",
        "fastest_lap_time",
        "fastest_lap_speed",
        "data_source",
        "file_date",
        "ingestion_date"
    ]

    # Ordena las columnas usando select y *column_order:
    results_final_df = results_final_df.select(*column_order)

    print("\n################## Step 5 - Drop duplicate rows ##################\n")
    
    results_final_df.createOrReplaceTempView("results_final")
    sql_query = """SELECT race_id, driver_id, count(1) 
    FROM results_final
    GROUP BY race_id, driver_id
    HAVING count(1) > 1
    ORDER BY race_id, driver_id DESC
    """
    spark.sql(sql_query).show(5)

    # Definimos la ventana particionada por race_id y driver_id, ordenada por result_id descendente
    window_spec = Window.partitionBy("race_id", "driver_id").orderBy(desc("result_id"))

    # Agregamos una columna con el número de fila
    results_ranked_df = results_final_df.withColumn("row_num", row_number().over(window_spec))

    # Filtramos solo la fila con row_num = 1, es decir, la de mayor result_id por grupo
    results_deduplicated_df = results_ranked_df.filter("row_num = 1").drop("row_num")

    ### Test ###
    # results_deduplicated_df.filter(
    #     (col("driver_id") == 579) & col("race_id").between(799, 807)
    # ).orderBy(col("race_id").asc()).show()

    print("\n################## Step 6 - Write data to datalake as parquet ##################\n")
    
    # Activar overwrite dinámico en la sesión de Spark
    # Borra y reemplaza únicamente las carpetas de partición
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic") 
    
    final_output_path = f"{SILVER_LAYER_PATH}/{v_file_date}/{output_path}"
    print(f"\n################## Writing data to: {final_output_path} ##################\n")
    # Verificamos si el DF NO está vacío
    if results_df.head(1):
        # Si tiene datos, escribimos particionado por race_id
        results_deduplicated_df.write \
        .mode("overwrite") \
        .partitionBy("race_id") \
        .parquet(final_output_path)
    else:
        # Al quitar el partitionBy, Spark escribirá un archivo parquet
        # que solo contiene los metadatos del esquema (headers) pero 0 filas
        results_deduplicated_df.write \
        .mode("overwrite") \
        .parquet(final_output_path)

    print("\n################## Data successfully saved to MinIO. ##################\n")
    print(f"\n################## {final_output_path} ##################\n")


if __name__ == "__main__":
    spark = get_spark_session()
    execution_date = sys.argv[1].strip()
    output_path = sys.argv[2].strip()
    ingest_results_to_silver(spark, execution_date, output_path)
    # Detener la sesión de Spark
    spark.sparkContext.stop()