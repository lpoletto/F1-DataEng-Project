from os import environ as env
import sys
from helpers import *
from pyspark.sql.functions import lit, col, when, upper, trim, date_format
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, TimestampType

SILVER_LAYER_PATH = env["SILVER_LAYER_PATH"]
GOLD_LAYER_PATH = env["GOLD_LAYER_PATH"]
DB_NAME = env["POSTGRES_DB"]
DB_USER = env["POSTGRES_USER"]
DB_PASSWORD = env["POSTGRES_PASSWORD"]
DB_HOST = env["POSTGRES_HOST"]
DB_PORT = env["POSTGRES_PORT"] 
F1_DB = env["F1_DWH"]
STG_SCHEMA = env["STG_SCHEMA"]
F1_GOLD_SCHEMA = env["GOLD_SCHEMA"]


def ingest_fact_race_results_to_gold(spark, execution_date):
    v_file_date = execution_date # Parametro
    
    print("\n################## Step 1 - Read data from results table ##################\n")
    print(f"\n################## {SILVER_LAYER_PATH}/{v_file_date}/results ##################\n")
    # Filtramos por fecha v_file_date
    results_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/{v_file_date}/results") \
    .filter(f"file_date = '{v_file_date}'") \
    .withColumnRenamed("race_id", "results_race_id") \
    .withColumnRenamed("ingestion_date", "created_at") \
    .withColumn("updated_at", lit(None).cast(TimestampType())) \
    .drop(col("file_date")) \
    .drop(col("data_source"))
    
    print("\n################## results_df ##################\n")
    results_df.show(5, truncate=False)

    print("\n################## Step 1 - Read data from races table ##################\n")
    print(f"\n################## {SILVER_LAYER_PATH}/{v_file_date}/races ##################\n")

    races_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/{v_file_date}/races") \
    .filter(f"file_date = '{v_file_date}'") \
    .select(
        col("race_id"),
        date_format(col("race_date"),"yyyyMMdd").cast('int').alias("date_id")
    )
    
    print("\n################## races_df ##################\n")
    races_df.show(5, truncate=False)
    # races_df.printSchema()

    results_join_df = results_df.join(races_df,results_df.results_race_id ==  races_df.race_id,"left") \
    .drop(col("results_race_id"))

    fact_results_final_df = results_join_df.withColumn(
        "position_text",
        when(trim(upper(col("position_text"))) == "D", "disqualified")
        .when(trim(upper(col("position_text"))) == "E", "excluded")
        .when(trim(upper(col("position_text"))) == "F", "failed to qualify")
        .when(trim(upper(col("position_text"))) == "N", "not classified")
        .when(trim(upper(col("position_text"))) == "R", "retired")
        .when(trim(upper(col("position_text"))) == "W", "withdrew")
        .otherwise(col("position_text"))
    )

    column_order = [
        "result_id",
        "race_id",
        "driver_id",
        "constructor_id",
        "status_id",
        "date_id",
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
        "created_at",
        "updated_at"
    ]

    # Ordena las columnas usando select y *column_order:
    fact_race_results_df = fact_results_final_df.select(column_order)

    print("\n################## fact_race_results_df ##################\n")
    fact_race_results_df.show(5, truncate=False)
    
    print("\n################## Step 2 - Write data to datalake as parquet ##################\n")
    
    output_path = f"{GOLD_LAYER_PATH}/fact_race_results"

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic") 

    fact_race_results_df.write.mode("overwrite").partitionBy("race_id").parquet(output_path)
    
    print("\n################## Data successfully saved to MinIO. ##################\n")
    print(f"\n################## {output_path} ##################\n")


if __name__ == "__main__":
    spark = get_spark_session()
    execution_date = sys.argv[1].strip()
    ingest_fact_race_results_to_gold(spark, execution_date)
    spark.sparkContext.stop()