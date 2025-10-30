from datetime import datetime
from os import environ as env
import sys
from helpers import *
from pyspark.sql.functions import to_timestamp, lit, concat, col, when,coalesce,trim
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType,FloatType

BRONZE_LAYER_PATH = env["BRONZE_LAYER_PATH"]
SILVER_LAYER_PATH = env["SILVER_LAYER_PATH"]

def ingest_constructor_standings_to_silver(spark, execution_date):
    v_file_date = execution_date # Parametro
    v_data_source = f"{BRONZE_LAYER_PATH}/{v_file_date}/constructor_standings"
    input_path = v_data_source

    constructor_standings_schema = StructType(fields=[StructField("constructorStandingsId", IntegerType(), False),
                                     StructField("raceId", IntegerType(), True),
                                     StructField("constructorId", IntegerType(), True),
                                     StructField("points", FloatType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("positionText", StringType(), True),
                                     StructField("wins", IntegerType(), True)                
    ])

    constructor_standings_df = spark.read.option("header", True) \
    .schema(constructor_standings_schema) \
    .format("csv") \
    .load(input_path)

    constructor_standings_renamed_df = constructor_standings_df.withColumnRenamed("constructorStandingsId","constructor_standings_id") \
    .withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("constructorId","constructor_id") \
    .withColumnRenamed("positionText","position_text")

    # We obtain the final scores for each season
    # Obtenemos los últimos resultados
    file_path = f"{SILVER_LAYER_PATH}/{v_file_date}/results"

    results_df = spark.read.parquet(file_path)

    max_race_id = results_df.select(max("race_id")).collect()[0][0]

    # Using spark.sql
    constructor_standings_renamed_df.createOrReplaceTempView("constructor_standings")

    constructor_standings_df = spark.sql(f"""
                select distinct 
                    constructor_standings_id
                    ,race_id
                    ,constructor_id
                    ,points
                    ,position
                    ,position_text
                    ,wins
                from constructor_standings
                where race_id <= {max_race_id}
                order by race_id desc
    """)

    # check data
    constructor_standings_df.filter("race_id=1142").show(3,truncate=False)

    constructor_standings_with_ingestion_date_df = add_ingestion_date(constructor_standings_df)
    
    constructor_standings_final_df = constructor_standings_with_ingestion_date_df \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date).cast(DateType()))

    column_order = ["constructor_standings_id",
    "race_id",
    "constructor_id",
    "points",
    "position",
    "position_text",
    "wins",
    "data_source",
    "file_date",
    "ingestion_date"]

    # Ordena las columnas usando select y *column_order:
    constructor_standings_final_df = constructor_standings_final_df.select(*column_order)

    output_path = f"{SILVER_LAYER_PATH}/{v_file_date}/constructor_standings"
    # Activar overwrite dinámico en la sesión de Spark
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic") 

    constructor_standings_final_df.write \
    .mode("overwrite") \
    .partitionBy("race_id") \
    .parquet(output_path)

    print("\n################## Datos guardados en MinIO con éxito. ##################\n")
    print(f"\n################## {output_path} ##################\n")


if __name__ == "__main__":
    spark = get_spark_session()
    execution_date = sys.argv[1]
    ingest_constructor_standings_to_silver(spark, execution_date)
    # Detener la sesión de Spark
    spark.sparkContext.stop()