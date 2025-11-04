from datetime import datetime
from os import environ as env
import sys
from helpers import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, concat, lit

BRONZE_LAYER_PATH = env["BRONZE_LAYER_PATH"]
SILVER_LAYER_PATH = env["SILVER_LAYER_PATH"]


def ingest_drivers_to_silver(spark, execution_date):
    v_file_date = execution_date # Parametro
    v_data_source = f"{BRONZE_LAYER_PATH}/{v_file_date}/drivers"
    input_path = v_data_source

    drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("forename", StringType(), True),
                                    StructField("surname", StringType(), True),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)
    ])


    print("\n################## Step 1 - Read the drivers data from the Bronze layer ##################\n")
    drivers_df = spark.read.option("header", True) \
    .schema(drivers_schema) \
    .format("csv") \
    .load(input_path)

    print("\n################## Step 2 - Rename the columns as required and add data_source column ##################\n")

    drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("code", "driver_code") \
    .withColumnRenamed("dob", "driver_dob") \
    .withColumnRenamed("nationality", "driver_nationality") \
    .withColumn("driver_name", concat(col("forename"), lit(" "), col("surname"))) \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date).cast(DateType()))

    drivers_with_aud_columns_df = add_ingestion_date(drivers_with_columns_df)

    print("\n################## Step 3 - Drop unwanted columns from the dataframe ##################\n")

    drivers_final_df = drivers_with_aud_columns_df.drop(col("forename"), col("surname"), col("url"))

    print("\n################## Step 5 - Order columns in a DataFrame ##################\n")
    column_order = ["driver_id",
        "driver_ref",
        "driver_number",
        "driver_code",
        "driver_dob",
        "driver_nationality",
        "driver_name",
        "data_source",
        "file_date",
        "ingestion_date"
    ]

    # Ordena las columnas usando select y *column_order:
    drivers_final_df = drivers_final_df.select(*column_order)
    drivers_final_df.show(5, truncate=False)
    drivers_final_df.printSchema()

    print("\n################## Step 6 - Write data to datalake as parquet ##################\n")
    output_path = f"{SILVER_LAYER_PATH}/{v_file_date}/drivers"
    drivers_final_df.write.mode("overwrite").parquet(output_path)

    print("\n################## Data successfully saved to MinIO. ##################\n")
    print(f"\n################## {output_path} ##################\n")
    

if __name__ == "__main__":
    # Configuración de SparkSession con soporte S3
    spark = get_spark_session()
    execution_date = sys.argv[1] 
    ingest_drivers_to_silver(spark, execution_date)
    # Detener la sesión de Spark
    spark.sparkContext.stop()