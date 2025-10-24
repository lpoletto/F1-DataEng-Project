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


def ingest_dim_race_to_gold(spark, execution_date):
    v_file_date = execution_date # Parametro
    print("\n################## Step 1 - Read data from races table ##################\n")
    races_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/{v_file_date}/races")
    races_final_df = races_df.withColumnRenamed("ingestion_date", "created_at") \
    .withColumn("updated_at", lit(None).cast(TimestampType())) \
    .drop(col("data_source")) \
    .drop(col("file_date"))

    column_order = [
        "race_id",
        "race_year",
        "race_date",
        "race_timestamp",
        "race_round",
        "circuit_id",
        "race_name",
        "created_at",
        "updated_at"
    ]

    # Ordena las columnas usando select y *column_order:
    dim_race_df = races_final_df.select(column_order)
    
    print("\n################## Step 2 - Write data to datalake as parquet ##################\n")
    # Activar overwrite dinámico en la sesión de Spark
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic") 

    dim_race_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{GOLD_LAYER_PATH}/dim_race")
    
    print("\n################## Step 3 - Write data to PostgreSQL ##################\n")
    print("\n################## Saving data to STG Table ##################\n")
    dim_race_df.write \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{F1_DB}") \
    .option("dbtable", f"{STG_SCHEMA}.dim_race") \
    .option("user", DB_USER) \
    .option("password", DB_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

    print("\n################## Step 4 - Merge ##################\n")
    sql_query = f"""
    CREATE TABLE IF NOT EXISTS {F1_GOLD_SCHEMA}.dim_race (
        race_sk INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        race_id int NOT NULL,
        race_year int NOT NULL,
        race_date date NOT NULL,
        race_timestamp timestamp NULL,
        race_round int NOT NULL,
        circuit_id int NOT NULL,
        race_name text NOT NULL,
        created_at timestamp NOT NULL,
        updated_at timestamp NULL,
        CONSTRAINT dim_race_uk UNIQUE (race_id),
        CONSTRAINT fk_circuit_id FOREIGN KEY (circuit_id) REFERENCES {F1_GOLD_SCHEMA}.dim_circuit (circuit_id)
    );
    """
    execute_sql_query(sql_query, F1_DB)

    # Insert a default row for unknown driver
    sql_query = f"""
    SELECT COUNT(*) FROM {F1_GOLD_SCHEMA}.dim_race WHERE race_id = -1;
    """

    result_query = fetch_sql_query_result(sql_query, F1_DB)

    if result_query[0][0] == 0:
        sql_query = f"""
        INSERT INTO {F1_GOLD_SCHEMA}.dim_race
        (        
            race_id,
            race_year,
            race_date,
            race_timestamp,
            race_round,
            circuit_id,
            race_name,
            created_at,
            updated_at
        )
        VALUES(
            -1, 1900, '1900-01-01','1900-01-01 00:00:00.000',-1,-1,'unknown',CURRENT_TIMESTAMP, NULL
        );
        """
        execute_sql_query(sql_query, F1_DB)

    # Merge data into gold table
    sql_query = f"""
        MERGE INTO {F1_GOLD_SCHEMA}.dim_race as tgt
        USING {STG_SCHEMA}.dim_race as stg
        ON tgt.race_id = stg.race_id
        WHEN NOT MATCHED THEN
        INSERT (
            race_id,
            race_year,
            race_date,
            race_timestamp,
            race_round,
            circuit_id,
            race_name,
            created_at
        )
        values
        (
            stg.race_id,
            stg.race_year,
            stg.race_date,
            stg.race_timestamp,
            stg.race_round,
            stg.circuit_id,
            stg.race_name,
            stg.created_at
        )
        WHEN MATCHED AND (
            COALESCE(tgt.race_name, ' ') <> COALESCE(stg.race_name, ' ') OR 
            COALESCE(tgt.race_round, -1) <> COALESCE(stg.race_round, -1)
        ) THEN
        UPDATE set race_year=stg.race_year,
            race_date=stg.race_date,
            race_timestamp=stg.race_timestamp,
            race_round=stg.race_round,
            circuit_id=stg.circuit_id,
            race_name=stg.race_name,
            updated_at=CURRENT_TIMESTAMP
        ;
    """
    execute_sql_query(sql_query, F1_DB)


if __name__ == "__main__":
    spark = get_spark_session()
    execution_date = sys.argv[1] # datetime.now().strftime("%Y-%m-%d")
    ingest_dim_race_to_gold(spark, execution_date)
    spark.sparkContext.stop()