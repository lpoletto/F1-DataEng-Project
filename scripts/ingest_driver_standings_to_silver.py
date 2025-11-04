from datetime import datetime
from os import environ as env
import sys
from helpers import *
from pyspark.sql.functions import count, sum, when, col, desc, rank, asc, row_number, lit, max
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType

BRONZE_LAYER_PATH = env["BRONZE_LAYER_PATH"]
SILVER_LAYER_PATH = env["SILVER_LAYER_PATH"]

def ingest_driver_standings_to_silver(spark, execution_date):
    v_file_date = execution_date # Parametro
    v_data_source = f"{BRONZE_LAYER_PATH}/{v_file_date}/driver_standings"
    input_path = v_data_source

    driver_standings_schema = StructType(fields=[StructField("driverStandingsId", IntegerType(), False),
                                        StructField("raceId", IntegerType(), True),
                                        StructField("driverId", IntegerType(), True),
                                        StructField("points", FloatType(), True),
                                        StructField("position", IntegerType(), True),
                                        StructField("positionText", StringType(), True),
                                        StructField("wins", IntegerType(), True)
    ])

    # # Obtenemos las carreras
    # file_path_races = f"{BRONZE_LAYER_PATH}/{v_file_date}/races"

    # races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
    #                                  StructField("year", IntegerType(), True),
    #                                  StructField("round", IntegerType(), True),
    #                                  StructField("circuitId", IntegerType(), True),
    #                                  StructField("name", StringType(), True),
    #                                  StructField("date", DateType(), True),
    #                                  StructField("time", StringType(), True),
    #                                  StructField("url", StringType(), True)                 
    # ])
    
    # races_df = spark.read.option("header", True) \
    # .schema(races_schema) \
    # .format("csv") \
    # .load(file_path_races) \
    # .select(
    #     col("raceId").alias("race_id"),
    #     col("year").alias("race_year"),
    #     col("round")
    # )

    # Obtenemos los últimos resultados
    file_path = f"{SILVER_LAYER_PATH}/{v_file_date}/results"

    results_df = spark.read.parquet(file_path)

    max_race_id = results_df.select(max("race_id")).collect()[0][0]
    # print(max_race_id)

    # Cargamos los datos de driver_standings desde Bronze
    driver_standings_df = spark.read.option("header", True) \
    .schema(driver_standings_schema) \
    .format("csv") \
    .load(input_path)

    driver_standings_renamed_df = driver_standings_df.withColumnRenamed("driverStandingsId", "driver_standings_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("positionText", "position_text")

    # We obtain the final scores for each season.
    # Using spark.sql
    driver_standings_renamed_df.createOrReplaceTempView("driver_standings")

    driver_standings_df = spark.sql(f"""
                select distinct 
                    ds.driver_standings_id
                    ,ds.race_id
                    ,ds.driver_id
                    ,ds.points
                    ,ds.position
                    ,ds.position_text
                    ,ds.wins
                from driver_standings ds
                where race_id <= {max_race_id}
                order by ds.race_id desc
    """)

    driver_standings_with_ingestion_date_df = add_ingestion_date(driver_standings_df)

    driver_standings_final_df = driver_standings_with_ingestion_date_df \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date).cast(DateType()))

    column_order = [
        "driver_standings_id",
        "race_id",
        "driver_id",
        "points",
        "position",
        "position_text",
        "wins",
        "data_source",
        "file_date",
        "ingestion_date"
    ]

    # Ordena las columnas usando select y *column_order:
    driver_standings_final_df = driver_standings_final_df.select(*column_order)
    driver_standings_final_df.show(5, truncate=False)

    
    output_path = f"{SILVER_LAYER_PATH}/{v_file_date}/driver_standings"
    
    # Activar overwrite dinámico en la sesión de Spark
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic") 

    driver_standings_final_df.write \
    .mode("overwrite") \
    .partitionBy("race_id") \
    .parquet(output_path)

    print("\n################## Data successfully saved to MinIO. ##################\n")
    print(f"\n################## {output_path} ##################\n")


if __name__ == "__main__":
    spark = get_spark_session()
    execution_date = sys.argv[1]
    ingest_driver_standings_to_silver(spark, execution_date)
    # Detener la sesión de Spark
    spark.sparkContext.stop()