from datetime import datetime
from os import environ as env
import sys
from helpers import *
from pyspark.sql.functions import to_timestamp, lit, concat, col, when,coalesce,trim
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

BRONZE_LAYER_PATH = env["BRONZE_LAYER_PATH"]
SILVER_LAYER_PATH = env["SILVER_LAYER_PATH"]

def ingest_races_to_silver(spark, execution_date, output_path):
    v_file_date = execution_date # Parametro
    v_data_source = f"{BRONZE_LAYER_PATH}/{v_file_date}/races"
    input_path = v_data_source

    races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("year", IntegerType(), True),
                                     StructField("round", IntegerType(), True),
                                     StructField("circuitId", IntegerType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("date", DateType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("url", StringType(), True)                 
    ])

    print("\n################## Step 1 - Read the races data from the Bronze layer ##################\n")
    
    races_df = spark.read.option("header", True) \
    .schema(races_schema) \
    .format("csv") \
    .load(input_path)

    print("\n################## Step 2 - Rename the columns as required ##################\n")

    races_renamed_df = races_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
    .withColumnRenamed("date", "race_date") \
    .withColumnRenamed("round", "race_round") \
    .withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("name", "race_name") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date).cast(DateType()))

    print("\n################## Step 3 - Add race_timestamp and ingestion date to the dataframe (audit field) ##################\n")

    races_with_timestamp_df = races_renamed_df.withColumn(
        "race_timestamp",
        to_timestamp(
            concat(
                col("race_date").cast(StringType()),
                lit(" "),
                when(
                    (col("time").isNotNull()) & (trim(col("time")) != ""), 
                    col("time")
                ).otherwise(lit("00:00:00"))
            ),
            "yyyy-MM-dd HH:mm:ss"
        )
    )

    races_with_ingestion_date_df = add_ingestion_date(races_with_timestamp_df)

    print("\n################## Step 4 - Select only the required columns ##################\n")

    races_final_df = races_with_ingestion_date_df \
    .select(
        col("race_id"), 
        col("race_year"), 
        col("race_round"), 
        col("circuit_id"), 
        col("race_name"), 
        col("race_date"), 
        col("race_timestamp"), 
        col("ingestion_date"), 
        col("data_source"), 
        col("file_date")
    )

    print("\n################## Step 5 - Order columns in a DataFrame ##################\n")

    column_order = [
        "race_id",
        "race_year",
        "race_round",
        "circuit_id",
        "race_name",
        "race_date",
        "race_timestamp",
        "data_source",
        "file_date",
        "ingestion_date"
    ]

    # Ordena las columnas usando select y *column_order:
    races_final_df = races_final_df.select(*column_order)
    races_final_df.show(5, truncate=False)
    races_final_df.printSchema()

    print("\n################## Step 6 - Write data to datalake as parquet ##################\n")
    # Activar overwrite dinámico en la sesión de Spark
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    races_final_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{output_path}")

    
    print("\n################## Data successfully saved to MinIO. ##################\n")
    print(f"\n################## {output_path} ##################\n")


if __name__ == "__main__":
    spark = get_spark_session()
    execution_date = sys.argv[1]
    output_path = sys.argv[2]
    ingest_races_to_silver(spark, execution_date, output_path)
    # Detener la sesión de Spark
    spark.sparkContext.stop()