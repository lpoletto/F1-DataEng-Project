import sys
from os import environ as env
from helpers import *

from pyspark.sql.functions import count, sum, when, col, desc, rank, asc, lit, row_number, max
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


def ingest_dim_driver_to_gold(spark, execution_date):
    v_file_date = execution_date # Parametro

    print("\n################## Step 1 - Read data from drivers table ##################\n")
    driver_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/{v_file_date}/drivers")

    driver_final_df = driver_df.withColumnRenamed("ingestion_date", "created_at") \
    .withColumn("updated_at", lit(None).cast(TimestampType())) \
    .drop(col("data_source")) \
    .drop(col("file_date"))

    column_order = [
        "driver_id",
        "driver_ref",
        "driver_number",
        "driver_code",
        "driver_name",
        "driver_nationality",
        "driver_dob",
        "created_at",
        "updated_at"
    ]

    # Ordena las columnas usando select y *column_order:
    dim_driver_df = driver_final_df.select(column_order)

    print("\n################## Step 2 - Write data to datalake as parquet ##################\n")
    dim_driver_df.write.mode("overwrite").parquet(f"{GOLD_LAYER_PATH}/dim_driver")

    print("\n################## Step 3 - Write data to PostgreSQL ##################\n")
    print("\n################## Saving data to STG Table ##################\n")
    # Saving data to STG Table
    dim_driver_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{F1_DB}") \
        .option("dbtable", f"{STG_SCHEMA}.dim_driver") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    
    print("\n################## Step 4 - Merge ##################\n")
    sql_query = f"""
    CREATE TABLE IF NOT EXISTS {F1_GOLD_SCHEMA}.dim_driver (
        driver_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        driver_id int not null,
        driver_ref text not null,
        driver_number int null,
        driver_code varchar(10) null,
        driver_name text NOT NULL,
        driver_nationality text NOT NULL,
        driver_dob date NOT NULL,
        created_at timestamp NOT NULL,
        updated_at timestamp NULL,
        CONSTRAINT dim_driver_uk UNIQUE (driver_id)
    );
    """
    execute_sql_query(sql_query, F1_DB)

    # Insert a default row for unknown driver
    sql_query = f"""
    SELECT COUNT(*) FROM {F1_GOLD_SCHEMA}.dim_driver WHERE driver_id = -1;
    """
    result_query = fetch_sql_query_result(sql_query, F1_DB)

    if result_query[0][0] == 0:
        sql_query = f"""
        INSERT INTO {F1_GOLD_SCHEMA}.dim_driver
        (
            driver_id,
            driver_ref,
            driver_number,
            driver_code,
            driver_name,
            driver_nationality,
            driver_dob,
            created_at,
            updated_at
        )
        VALUES(
            -1, 'unknown', -1, 'unknown', 'unknown', 'unknown', '1900-01-01', CURRENT_TIMESTAMP, NULL
        );
        """
        execute_sql_query(sql_query, F1_DB)
        
        # Merge data into gold table
        sql_query = f"""
        MERGE INTO {F1_GOLD_SCHEMA}.dim_driver as tgt
        USING {STG_SCHEMA}.dim_driver as stg
        ON tgt.driver_id = stg.driver_id
        WHEN NOT MATCHED THEN
        INSERT(
            driver_id,
            driver_ref,
            driver_number,
            driver_code,
            driver_name,
            driver_nationality,
            driver_dob,
            created_at
        ) 
        values
        (
            stg.driver_id,
            stg.driver_ref,
            stg.driver_number,
            stg.driver_code,
            stg.driver_name,
            stg.driver_nationality,
            stg.driver_dob,
            stg.created_at
        )
        WHEN MATCHED AND (
            COALESCE(tgt.driver_number, -1) <> COALESCE(stg.driver_number, -1) OR 
            COALESCE(tgt.driver_code, ' ') <> COALESCE(stg.driver_code, ' ')
        ) THEN
        UPDATE set driver_ref=stg.driver_ref,
            driver_name=stg.driver_name,
            driver_number=stg.driver_number,
            driver_code=stg.driver_code,
            driver_nationality=stg.driver_nationality,
            driver_dob=stg.driver_dob,
            updated_at=CURRENT_TIMESTAMP
        ;
        """
        execute_sql_query(sql_query, F1_DB)
    

if __name__ == "__main__":
    spark = get_spark_session()
    execution_date = sys.argv[1].strip()
    ingest_dim_driver_to_gold(spark, execution_date)
    spark.sparkContext.stop()