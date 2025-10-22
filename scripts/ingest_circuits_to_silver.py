from datetime import datetime
from os import environ as env
import sys
from helpers import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import lit, col

BRONZE_LAYER_PATH = env["BRONZE_LAYER_PATH"]
SILVER_LAYER_PATH = env["SILVER_LAYER_PATH"]


def ingest_circuits_to_silver(spark, execution_date):
    v_file_date = execution_date # Parametro
    v_data_source = f"{BRONZE_LAYER_PATH}/{v_file_date}/circuits"
    input_path = v_data_source
    
    circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)                 
    ])
 
    print("\n################## Step 1 - Read the circuits data from the Bronze layer ##################\n")
    circuits_df = spark.read.option("header", True) \
    .schema(circuits_schema) \
    .format("csv") \
    .load(input_path)

    print("\n################## Step 2 - Select only the required columns ##################\n")
    circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

    print("\n################## Step 3 - Rename the columns as required and add data_source column ##################\n")
    circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("name", "circuit_name") \
    .withColumnRenamed("location", "circuit_location") \
    .withColumnRenamed("country", "circuit_country") \
    .withColumnRenamed("lat", "circuit_latitude") \
    .withColumnRenamed("lng", "circuit_longitude") \
    .withColumnRenamed("alt", "circuit_altitude") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date).cast(DateType()))

    print("\n################## Step 4 - Add ingestion date to the dataframe (audit field) ##################\n")
    circuits_final_df = add_ingestion_date(circuits_renamed_df)

    print("\n################## Step 5 - Order columns in a DataFrame ##################\n")
    column_order = ["circuit_id",
        "circuit_ref",
        "circuit_name",
        "circuit_location",
        "circuit_country",
        "circuit_latitude",
        "circuit_longitude",
        "circuit_altitude",
        "data_source",
        "file_date",
        "ingestion_date"
    ]

    # Ordena las columnas usando select y *column_order:
    circuits_final_df = circuits_final_df.select(*column_order)
    circuits_final_df.show(5, truncate=False)
    circuits_final_df.printSchema()
    
    print("\n################## Step 6 - Write data to datalake as parquet ##################\n")
    output_path = f"{SILVER_LAYER_PATH}/{v_file_date}/circuits"
    circuits_final_df.write.mode("overwrite").parquet(output_path)
    
    print("\n################## Datos guardados en MinIO con éxito. ##################\n")
    print(f"\n################## {output_path} ##################\n")


if __name__ == "__main__":
    # Configuración de SparkSession con soporte S3
    spark = get_spark_session()
    execution_date = sys.argv[1] # datetime.now().strftime("%Y-%m-%d")
    ingest_circuits_to_silver(spark, execution_date)
    # Detener la sesión de Spark
    spark.sparkContext.stop()