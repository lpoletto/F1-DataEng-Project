from os import environ as env
from pyspark.sql.functions import count, sum, when, col, desc, rank, asc, lit, row_number, max
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, TimestampType
import sys
from helpers import *

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
    dim_circuit_df = circuits_final_df.select(column_order)
    
    print("\n################## Step 2 - Write data to datalake as parquet ##################\n")
    dim_circuit_df.write.mode("overwrite").parquet(f"{GOLD_LAYER_PATH}/dim_circuit")

    print("\n################## Step 3 - Write data to PostgreSQL ##################\n")
    print("\n################## Saving data to STG Table ##################\n")
    dim_circuit_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{F1_DB}") \
        .option("dbtable", f"{STG_SCHEMA}.dim_circuit") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    print("\n################## Step 4 - Merge ##################\n")
    sql_query = f"""
        CREATE TABLE IF NOT EXISTS {F1_GOLD_SCHEMA}.dim_circuit (
        circuit_sk INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        circuit_id INT NOT NULL,
        circuit_ref TEXT NOT NULL,
        circuit_name TEXT NOT NULL,
        circuit_location TEXT NOT NULL,
        circuit_country TEXT NOT NULL,
        circuit_latitude NUMERIC(10, 6) NOT NULL,
        circuit_longitude NUMERIC(10, 6) NOT NULL,
        circuit_altitude INTEGER,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NULL,
        CONSTRAINT dim_circuit_uk UNIQUE (circuit_id)
    );
    """
    execute_sql_query(sql_query, F1_DB)

    # Insert a default row for unknown driver
    sql_query = f"""
    SELECT COUNT(*) FROM {F1_GOLD_SCHEMA}.dim_circuit WHERE circuit_id = -1;
    """

    result_query = fetch_sql_query_result(sql_query, F1_DB)

    if result_query[0][0] == 0:
        sql_query = f"""
        INSERT INTO {F1_GOLD_SCHEMA}.dim_circuit
        (
            circuit_id,
            circuit_ref,
            circuit_name,
            circuit_location,
            circuit_country,
            circuit_latitude,
            circuit_longitude,
            circuit_altitude,
            created_at,
            updated_at
        )
        VALUES(
            -1, 'unknown', 'unknown','unknown','unknown',-1,-1,-1,CURRENT_TIMESTAMP, NULL
        );
        """
        execute_sql_query(sql_query, F1_DB)
    
    # Merge data into gold table
    sql_query = f"""
    MERGE INTO {F1_GOLD_SCHEMA}.dim_circuit as tgt
    USING {STG_SCHEMA}.dim_circuit as stg
    ON tgt.circuit_id = stg.circuit_id
    WHEN NOT MATCHED THEN
        INSERT(
            circuit_id,
            circuit_ref,
            circuit_name,
            circuit_location,
            circuit_country,
            circuit_latitude,
            circuit_longitude,
            circuit_altitude,
            created_at
        )
        values
        (
            stg.circuit_id,
            stg.circuit_ref,
            stg.circuit_name,
            stg.circuit_location,
            stg.circuit_country,
            stg.circuit_latitude,
            stg.circuit_longitude,
            stg.circuit_altitude,
            stg.created_at
        )
    WHEN MATCHED AND (
        COALESCE(tgt.circuit_name, ' ') <> COALESCE(stg.circuit_name, ' ') OR 
        COALESCE(tgt.circuit_longitude, -1) <> COALESCE(stg.circuit_longitude, -1)
    ) THEN
    UPDATE set circuit_ref=stg.circuit_ref,
        circuit_name=stg.circuit_name,
        circuit_location=stg.circuit_location,
        circuit_country=stg.circuit_country,
        circuit_latitude=stg.circuit_latitude,
        circuit_longitude=stg.circuit_longitude,
        circuit_altitude=stg.circuit_altitude,
        updated_at=CURRENT_TIMESTAMP
    ;
    """
    execute_sql_query(sql_query, F1_DB)


if __name__ == "__main__":
    spark = get_spark_session()
    execution_date = sys.argv[1] # datetime.now().strftime("%Y-%m-%d")
    ingest_circuits_to_gold(spark, execution_date)
    spark.sparkContext.stop()