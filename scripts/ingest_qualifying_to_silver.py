from datetime import datetime
from os import environ as env
import sys
from helpers import *
from pyspark.sql.functions import to_timestamp, lit, concat, col, when,coalesce,trim
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

BRONZE_LAYER_PATH = env["BRONZE_LAYER_PATH"]
SILVER_LAYER_PATH = env["SILVER_LAYER_PATH"]

def ingest_qualifying_to_silver(spark, execution_date):
    v_file_date = execution_date # Parametro
    v_data_source = f"{BRONZE_LAYER_PATH}/{v_file_date}/qualifying"
    input_path = v_data_source

    print("\n################## Step 1 - Read the pit_stops data from the Bronze layer ##################\n")

    qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), False),
                                      StructField("constructorId", IntegerType(), False),
                                      StructField("number", IntegerType(), False),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True)
    ])

    qualifying_df = spark.read.option("header", True) \
    .schema(qualifying_schema) \
    .format("csv") \
    .load(input_path)

    print("\n################## Step 2 - Rename columns and add new columns ##################\n")

    qualifying_with_columns_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id")

    print("\n################## Step 3 - Add race_timestamp and ingestion date to the dataframe (audit field) ##################\n")

    qualifying_with_aud_columns_df = add_ingestion_date(qualifying_with_columns_df)
    qualifying_final_df = qualifying_with_aud_columns_df.withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date).cast(DateType()))

    print("\n################## Step 4 - Order columns in a DataFrame ##################\n")

    column_order = ["qualify_id",
    "race_id",
    "driver_id",
    "constructor_id",
    "number",
    "position",
    "q1",
    "q2",
    "q3",
    "data_source",
    "file_date",
    "ingestion_date"]

    # Ordena las columnas usando select y *column_order:
    qualifying_final_df = qualifying_final_df.select(*column_order)

    print("\n################## Step 5 - Write data to datalake as parquet ##################\n")
    output_path = f"{SILVER_LAYER_PATH}/{v_file_date}/qualifying"
    qualifying_final_df.write.mode("overwrite").parquet(output_path)
    
    print("\n################## Data successfully saved to MinIO. ##################\n")
    print(f"\n################## {output_path} ##################\n")


if __name__ == "__main__":
    spark = get_spark_session()
    execution_date = sys.argv[1]
    ingest_qualifying_to_silver(spark, execution_date)
    # Detener la sesión de Spark
    spark.sparkContext.stop()