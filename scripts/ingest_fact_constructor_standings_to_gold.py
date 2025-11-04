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


def ingest_fact_constructor_standings_to_gold(spark, execution_date):
    v_file_date = execution_date # Parametro

    print("\n################## Step 1 - Read data from constructor_standings table ##################\n")
    
    constructor_standings_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/{v_file_date}/constructor_standings") \
    .withColumnRenamed("ingestion_date", "created_at") \
    .withColumnRenamed("race_id", "constructor_standings_race_id") \
    .withColumn("updated_at", lit(None).cast(TimestampType())) \
    .drop(col("data_source")) \
    .drop(col("file_date")) \
    # .drop(col("position")) \
    # .drop(col("position_text"))

    print("\n################## Step 2 - Read data from races table ##################\n")

    races_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/{v_file_date}/races") \
    .filter(f"file_date = '{v_file_date}'") \
    .select(
        col("race_id"),
        date_format(col("race_date"),"yyyyMMdd").cast('int').alias("date_id")
    )

    # Join constructor_standings with races
    constructor_standings_join_df = constructor_standings_df.join(
        races_df,
        constructor_standings_df.constructor_standings_race_id ==  races_df.race_id,
        "left"
    ) \
    .drop(col("constructor_standings_race_id"))

    print("\n################## constructor_standings_join_df ##################\n")
    constructor_standings_join_df = constructor_standings_join_df.withColumnRenamed("position", "rank")
    constructor_standings_join_df.show(5, truncate=False)

    column_order = [
        "constructor_standings_id",
        "race_id",
        "constructor_id",
        "date_id",
        "points",
        "wins",
        "rank",
        "created_at",
        "updated_at"
    ]

    # Ordena las columnas usando select y *column_order:
    fact_constructor_standings_df = constructor_standings_join_df.select(*column_order)
    # Test print
    fact_constructor_standings_df.filter("race_id=35").orderBy("rank").show(1, truncate=False)

    print("\n################## Step 3 - Write data to datalake as parquet ##################\n")
    
    output_path = f"{GOLD_LAYER_PATH}/fact_constructor_standings"
    
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic") 

    fact_constructor_standings_df.write.mode("overwrite") \
        .partitionBy("race_id") \
        .parquet(output_path)
    
    print("\n################## Data successfully saved to MinIO. ##################\n")
    print(f"\n################## {output_path} ##################\n")


if __name__ == "__main__":
    spark = get_spark_session()
    execution_date = sys.argv[1]
    ingest_fact_constructor_standings_to_gold(spark, execution_date)
    spark.sparkContext.stop()