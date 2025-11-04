from datetime import datetime
from os import environ as env
import sys
from helpers import *
from pyspark.sql.functions import count, sum, when, col, desc, rank, asc, row_number, lit, max
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType

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

    print("\n################## Data successfully saved to MinIO. ##################\n")
    print(f"\n################## {output_path} ##################\n")

    # # Testing data
    # races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
    #                                  StructField("year", IntegerType(), True),
    #                                  StructField("round", IntegerType(), True),
    #                                  StructField("circuitId", IntegerType(), True),
    #                                  StructField("name", StringType(), True),
    #                                  StructField("date", DateType(), True),
    #                                  StructField("time", StringType(), True),
    #                                  StructField("url", StringType(), True)                 
    # ])

    # file_path_races = f"{BRONZE_LAYER_PATH}/{v_file_date}/races"

    # races_df = spark.read.option("header", True) \
    # .schema(races_schema) \
    # .format("csv") \
    # .load(file_path_races) \
    # .select(
    #     col("raceId").alias("race_id"),
    #     col("year").alias("race_year"),
    #     col("round")
    # )

    # races_df_agg = races_df.groupBy("race_year").agg(
    #     max("round").alias("last_round"),
    #     max("race_id").alias("race_id")
    # )

    # constructor_standings_final_df.createOrReplaceTempView("constructor_standings")
    # races_df_agg.createOrReplaceTempView("races")

    # df = spark.sql(f"""
    # select distinct
    #     r.race_year,
    #     cs.*
    # from constructor_standings cs
    # inner join races r on cs.race_id = r.race_id
    # where cs.`position`=1 and cs.constructor_id=6
    # """)

    # df.show(20, truncate=False)
    # df.count()


if __name__ == "__main__":
    spark = get_spark_session()
    execution_date = sys.argv[1]
    ingest_constructor_standings_to_silver(spark, execution_date)
    # Detener la sesión de Spark
    spark.sparkContext.stop()