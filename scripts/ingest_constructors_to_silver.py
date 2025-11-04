from datetime import datetime
from os import environ as env
import sys
from helpers import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import lit, col

BRONZE_LAYER_PATH = env["BRONZE_LAYER_PATH"]
SILVER_LAYER_PATH = env["SILVER_LAYER_PATH"]


def ingest_constructors_to_silver(spark, execution_date):
    v_file_date = execution_date # Parametro
    v_data_source = f"{BRONZE_LAYER_PATH}/{v_file_date}/constructors"
    input_path = v_data_source

    constructors_schema = StructType(fields=[StructField("constructorId", IntegerType(), False),
                                     StructField("constructorRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("nationality", StringType(), True),
                                     StructField("url", StringType(), True)                 
    ])

    print("\n################## Step 1 - Read the constructors data from the Bronze layer ##################\n")

    constructors_df = spark.read.option("header", True) \
    .schema(constructors_schema) \
    .format("csv") \
    .load(input_path)

    print("\n################## Step 2 - Drop unwanted columns from the dataframe ##################\n")

    constructors_dropped_df = constructors_df.drop(col("url"))
    
    print("\n################## Step 3 - Rename the columns as required and add data_source column ##################\n")

    constructors_withColumn_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref") \
    .withColumnRenamed("name", "constructor_name") \
    .withColumnRenamed("nationality", "constructor_nationality") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date).cast(DateType()))
    
    print("\n################## Step 4 - Add ingestion date to the dataframe (audit field) ##################\n")

    constructors_final_df = add_ingestion_date(constructors_withColumn_df)

    print("\n################## Step 5 - Order columns in a DataFrame ##################\n")
    
    column_order = [
        "constructor_id",
        "constructor_ref",
        "constructor_name",
        "constructor_nationality",
        "data_source",
        "file_date",
        "ingestion_date"
    ]
    # Ordena las columnas usando select y *column_order:
    constructors_final_df = constructors_final_df.select(*column_order)
    
    print("\n################## Step 6 - Write data to datalake as parquet ##################\n")
    
    output_path = f"{SILVER_LAYER_PATH}/{v_file_date}/constructors"
    constructors_final_df.write.mode("overwrite").parquet(output_path)
    
    print("\n################## Data successfully saved to MinIO. ##################\n")
    print(f"\n################## {output_path} ##################\n")


if __name__ == "__main__":
    # Configuración de SparkSession con soporte S3
    spark = get_spark_session()
    execution_date = sys.argv[1]
    ingest_constructors_to_silver(spark, execution_date)
    # Detener la sesión de Spark
    spark.sparkContext.stop()