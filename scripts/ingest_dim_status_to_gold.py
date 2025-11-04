from os import environ as env
from pyspark.sql.functions import count, sum, when, col, desc, rank, asc, lit, row_number, max
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, TimestampType, StructType, StructField, IntegerType, StringType
import sys
from helpers import *

BRONZE_LAYER_PATH = env["BRONZE_LAYER_PATH"]
GOLD_LAYER_PATH = env["GOLD_LAYER_PATH"]
DB_NAME = env["POSTGRES_DB"]
DB_USER = env["POSTGRES_USER"]
DB_PASSWORD = env["POSTGRES_PASSWORD"]
DB_HOST = env["POSTGRES_HOST"]
DB_PORT = env["POSTGRES_PORT"] 
F1_DB = env["F1_DWH"]
STG_SCHEMA = env["STG_SCHEMA"]
F1_GOLD_SCHEMA = env["GOLD_SCHEMA"]

def ingest_dim_status_to_gold(spark, execution_date):
    v_file_date = execution_date # Parametro
    v_data_source = f"{BRONZE_LAYER_PATH}/{v_file_date}/status"
    input_path = v_data_source

    print("\n################## Step 1 - Read data from status table ##################\n")
    status_schema = StructType([
        StructField("statusId", IntegerType(), nullable=False),
        StructField("status", StringType(), nullable=False)
    ])
    
    status_df = spark.read.option("header", True) \
    .schema(status_schema) \
    .format("csv") \
    .load(input_path)

    status_final_df = status_df.withColumnRenamed("statusId", "status_id") \
    .withColumnRenamed("status", "status_name") \
    .withColumn("updated_at", lit(None).cast(TimestampType()))

    status_final_df = add_ingestion_date(status_final_df, "created_at")

    
    column_order = [
        "status_id",
        "status_name",
        "created_at",
        "updated_at"
    ]

    # Ordena las columnas usando select y *column_order:
    dim_status_df = status_final_df.select(*column_order)
    
    print("\n################## Step 2 - Write data to datalake as parquet ##################\n")
    dim_status_df.write.mode("overwrite").parquet(f"{GOLD_LAYER_PATH}/dim_status")

    print("\n################## Step 3 - Write data to PostgreSQL ##################\n")
    print("\n################## Saving data to STG Table ##################\n")

    dim_status_df.write \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{F1_DB}") \
    .option("dbtable", f"{STG_SCHEMA}.dim_status") \
    .option("user", DB_USER) \
    .option("password", DB_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

    print("\n################## Step 4 - Merge ##################\n")
    # Create table if not exists
    sql_query = f"""
    CREATE TABLE IF NOT EXISTS {F1_GOLD_SCHEMA}.dim_status (
        status_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        status_id int not null,
        status_name text NOT NULL,
        created_at timestamp NOT NULL,
        updated_at timestamp NULL,
        CONSTRAINT dim_status_uk UNIQUE (status_id)
    );
    """
    execute_sql_query(sql_query, F1_DB)

    # Insert a default row for unknown driver
    sql_query = f"""
    SELECT COUNT(*) FROM {F1_GOLD_SCHEMA}.dim_status WHERE status_id = -1;
    """

    result_query = fetch_sql_query_result(sql_query, F1_DB)

    if result_query[0][0] == 0:
        sql_query = f"""
        INSERT INTO {F1_GOLD_SCHEMA}.dim_status(
            status_id,
            status_name,
            created_at,
            updated_at
        )
        VALUES(
            -1, 'unknown', CURRENT_TIMESTAMP, NULL
        );
        """
        execute_sql_query(sql_query, F1_DB)

    # Merge data into gold table
    sql_query = f"""
    MERGE INTO {F1_GOLD_SCHEMA}.dim_status as tgt
    USING {STG_SCHEMA}.dim_status as stg
    ON tgt.status_id = stg.status_id
    WHEN NOT MATCHED THEN
    INSERT(
        status_id,
        status_name,
        created_at
    ) 
    VALUES
    (
        stg.status_id,
        stg.status_name,
        stg.created_at
    )
    WHEN MATCHED AND COALESCE(tgt.status_name, ' ') <> COALESCE(stg.status_name, ' ') THEN
    UPDATE set status_name=stg.status_name,
        updated_at=CURRENT_TIMESTAMP
    ;
    """
    
    execute_sql_query(sql_query, F1_DB)


if __name__ == "__main__":
    spark = get_spark_session()
    execution_date = sys.argv[1]
    ingest_dim_status_to_gold(spark, execution_date)
    spark.sparkContext.stop()