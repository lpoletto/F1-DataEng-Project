from os import environ as env
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, when, col, desc, rank, asc, lit, row_number, max
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, TimestampType
from commons import *
import sys

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


def ingest_circuits_to_gold(spark, execution_date):
    v_file_date = execution_date # Parametro

    print("\n################## Step 1 - Read data from circuits table ##################\n")
    circuits_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/{v_file_date}/circuits")
    circuits_final_df = circuits_df.withColumnRenamed("ingestion_date", "created_at") \
        .withColumn("updated_at", lit(None).cast(TimestampType()))
    
    column_order = [
        "circuit_id",
        "circuit_ref",
        "circuit_name",
        "circuit_location",
        "circuit_country",
        "circuit_latitude",
        "circuit_longitude",
        "circuit_altitude",
        "created_at",
        "updated_at"
    ]

    # Ordena las columnas usando select y *column_order:
    dim_circuits_df = circuits_final_df.select(column_order)
    
    print("\n################## Step 2 - Write data to datalake as parquet ##################\n")
    dim_circuits_df.write.mode("overwrite").parquet(f"{GOLD_LAYER_PATH}/dim_circuits")

    print("\n################## Step 3 - Write data to PostgreSQL ##################\n")
    dim_circuits_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{F1_DB}") \
        .option("dbtable", f"{STG_SCHEMA}.dim_circuit") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()



if __name__ == "__main__":
    spark = get_spark_session()
    execution_date = sys.argv[1] # datetime.now().strftime("%Y-%m-%d")
    ingest_circuits_to_gold(spark, execution_date)
    spark.sparkContext.stop()