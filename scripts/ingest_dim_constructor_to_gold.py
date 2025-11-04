import sys
from os import environ as env
from pyspark.sql.functions import count, sum, when, col, desc, rank, asc, lit, row_number, max
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, TimestampType
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


def ingest_dim_constructor_to_gold(spark, execution_date):
    v_file_date = execution_date # Parametro
    print("\n################## Step 1 - Read data from constructors table ##################\n")
    constructors_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/{v_file_date}/constructors")
    constructors_final_df = constructors_df.withColumnRenamed("ingestion_date", "created_at") \
    .withColumn("updated_at", lit(None).cast(TimestampType())) \
    .drop(col("data_source")) \
    .drop(col("file_date"))

    column_order = [
        "constructor_id",
        "constructor_ref",
        "constructor_name",
        "constructor_nationality",
        "created_at",
        "updated_at"
    ]

    # Ordena las columnas usando select y *column_order:
    dim_constructor_df = constructors_final_df.select(column_order)
    print("\n################## Step 2 - Write data to datalake as parquet ##################\n")
    dim_constructor_df.write.mode("overwrite").parquet(f"{GOLD_LAYER_PATH}/dim_constructor")
    print("\n################## Step 3 - Write data to PostgreSQL ##################\n")
    print("\n################## Saving data to STG Table ##################\n")
    # Saving data to STG Table
    dim_constructor_df.write \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{F1_DB}") \
    .option("dbtable", f"{STG_SCHEMA}.dim_constructor") \
    .option("user", DB_USER) \
    .option("password", DB_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

    print("\n################## Step 4 - Merge ##################\n")
    sql_query = f"""
    CREATE TABLE IF NOT EXISTS {F1_GOLD_SCHEMA}.dim_constructor (
        constructor_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        constructor_id int not null,
        constructor_ref text not null,
        constructor_name text NOT NULL,
        constructor_nationality text NOT NULL,
        created_at timestamp NOT NULL,
        updated_at timestamp NULL,
        CONSTRAINT dim_constructor_uk UNIQUE (constructor_id)
    );
    """
    execute_sql_query(sql_query, F1_DB)

    # Insert a default row for unknown driver
    sql_query = f"""
    SELECT COUNT(*) FROM {F1_GOLD_SCHEMA}.dim_constructor WHERE constructor_id = -1;
    """

    result_query = fetch_sql_query_result(sql_query, F1_DB)

    if result_query[0][0] == 0:
        sql_query = f"""
        INSERT INTO {F1_GOLD_SCHEMA}.dim_constructor(
            constructor_id, 
            constructor_ref, 
            constructor_name, 
            constructor_nationality,
            created_at,
            updated_at
        )
        VALUES(
            -1, 'unknown', 'unknown', 'unknown', CURRENT_TIMESTAMP, NULL
        );
        """
        execute_sql_query(sql_query, F1_DB)
    
    # Merge data into gold table
    sql_query = f"""
        MERGE INTO {F1_GOLD_SCHEMA}.dim_constructor as tgt
        USING {STG_SCHEMA}.dim_constructor as stg
        ON tgt.constructor_id = stg.constructor_id
        WHEN NOT MATCHED THEN
        INSERT (
            constructor_id,
            constructor_ref,
            constructor_name,
            constructor_nationality,
            created_at
        )
        values
        (
            stg.constructor_id,
            stg.constructor_ref,
            stg.constructor_name,
            stg.constructor_nationality,
            stg.created_at
        )
        WHEN MATCHED AND (
            COALESCE(tgt.constructor_name, ' ') <> COALESCE(stg.constructor_name, ' ') OR 
            COALESCE(tgt.constructor_ref, ' ') <> COALESCE(stg.constructor_ref, ' ')
        ) THEN
        UPDATE set constructor_ref=stg.constructor_ref,
        constructor_name=stg.constructor_name,
        constructor_nationality=stg.constructor_nationality,
        updated_at=CURRENT_TIMESTAMP
    ;
    """
    execute_sql_query(sql_query, F1_DB)


if __name__ == "__main__":
    spark = get_spark_session()
    execution_date = sys.argv[1]
    ingest_dim_constructor_to_gold(spark, execution_date)
    spark.sparkContext.stop()