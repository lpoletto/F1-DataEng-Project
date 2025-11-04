from os import environ as env
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


def merge_fact_race_results_to_gold(spark):
    
    print("\n################## Step 1 - Read data from gold layer ##################\n")
    fact_race_results_df = spark.read.parquet(f"{GOLD_LAYER_PATH}/fact_race_results")

       
    print("\n################## Step 2 - Write data to PostgreSQL ##################\n")
    print("\n################## Saving data to STG Table ##################\n")
    # Saving data to STG Table
    fact_race_results_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{F1_DB}") \
        .option("dbtable", f"{STG_SCHEMA}.fact_race_results") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    
    print("\n################## Step 3 - CREATE TABLE IF NOT EXISTS ##################\n")
    # Create table if not exists
    sql_query = f"""
    CREATE TABLE IF NOT EXISTS {F1_GOLD_SCHEMA}.fact_race_results (
        result_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        result_id int4 NOT NULL,
        race_id int4 NOT NULL,
        driver_id int4 NOT NULL,
        constructor_id int4 NOT NULL,
        status_id int4 NOT NULL,
        date_id int4 NOT NULL,
        grid int4 NULL,
        "position" int4 NULL,
        position_text text NULL,
        position_order int4 NULL,
        points float4 NULL,
        laps int4 NULL,
        "time" text NULL,
        milliseconds int4 NULL,
        fastest_lap int4 NULL,
        "rank" int4 NULL,
        fastest_lap_time text NULL,
        fastest_lap_speed text NULL,
        created_at timestamp NOT NULL,
        updated_at timestamp NULL,
        CONSTRAINT fact_race_results_uk UNIQUE (result_id),
        CONSTRAINT fk_race_id FOREIGN KEY (race_id) REFERENCES {F1_GOLD_SCHEMA}.dim_race (race_id),
        CONSTRAINT fk_driver_id FOREIGN KEY (driver_id) REFERENCES {F1_GOLD_SCHEMA}.dim_driver (driver_id),
        CONSTRAINT fk_constructor_id FOREIGN KEY (constructor_id) REFERENCES {F1_GOLD_SCHEMA}.dim_constructor (constructor_id),
        CONSTRAINT fk_status_id FOREIGN KEY (status_id) REFERENCES {F1_GOLD_SCHEMA}.dim_status (status_id),
        CONSTRAINT fk_date_id FOREIGN KEY (date_id) REFERENCES {F1_GOLD_SCHEMA}.dim_date (date_id)
    );
    """
    execute_sql_query(sql_query, F1_DB)
    
    # Insert a default row for unknown driver
    sql_query = f"""
    SELECT COUNT(*) FROM {F1_GOLD_SCHEMA}.fact_race_results WHERE result_id = -1;
    """

    result_query = fetch_sql_query_result(sql_query, F1_DB)

    if result_query[0][0] == 0:
        sql_query = f"""
        INSERT INTO {F1_GOLD_SCHEMA}.fact_race_results
        (
            result_id,
            race_id,
            driver_id,
            constructor_id,
            status_id,
            date_id,
            grid,
            position,
            position_text,
            position_order,
            points,
            laps,
            time,
            milliseconds,
            fastest_lap,
            rank,
            fastest_lap_time,
            fastest_lap_speed,
            created_at,
            updated_at
        )
        VALUES(
            -1, -1, -1, -1, -1, 19000101, -1, -1, 'unknown', -1, -1, -1, 'unknown', -1, -1, -1, 'unknown', 'unknown', CURRENT_TIMESTAMP, NULL
        );
        """
        execute_sql_query(sql_query, F1_DB)
    
    print("\n################## Step 4 - Merge data into gold table ##################\n")
    # Merge data into gold table
    sql_query = f"""
    MERGE INTO {F1_GOLD_SCHEMA}.fact_race_results as tgt
    USING {STG_SCHEMA}.fact_race_results as stg
    ON tgt.result_id = stg.result_id
    WHEN NOT MATCHED THEN
    INSERT(
        result_id,
        race_id,
        driver_id,
        constructor_id,
        status_id,
        date_id,
        grid,
        position,
        position_text,
        position_order,
        points,
        laps,
        time,
        milliseconds,
        fastest_lap,
        rank,
        fastest_lap_time,
        fastest_lap_speed,
        created_at
    ) 
    VALUES
    (
        stg.result_id,
        stg.race_id,
        stg.driver_id,
        stg.constructor_id,
        stg.status_id,
        stg.date_id,
        stg.grid,
        stg.position,
        stg.position_text,
        stg.position_order,
        stg.points,
        stg.laps,
        stg.time,
        stg.milliseconds,
        stg.fastest_lap,
        stg.rank,
        stg.fastest_lap_time,
        stg.fastest_lap_speed,
        stg.created_at
    )
    WHEN MATCHED AND (
        COALESCE(tgt.position, -1) <> COALESCE(stg.position, -1)
        OR 
        COALESCE(tgt.position_text, ' ') <> COALESCE(stg.position_text, ' ') 
    ) THEN
    UPDATE set grid=stg.grid,
        position=stg.position,
        position_text=stg.position_text,
        position_order=stg.position_order,
        points=stg.points,
        laps=stg.laps,
        time=stg.time,
        milliseconds=stg.milliseconds,
        fastest_lap=stg.fastest_lap,
        rank=stg.rank,
        fastest_lap_time=stg.fastest_lap_time,
        fastest_lap_speed=stg.fastest_lap_speed,
        updated_at=CURRENT_TIMESTAMP
    ;
    """
    execute_sql_query(sql_query, F1_DB)


if __name__ == "__main__":
    spark = get_spark_session()
    merge_fact_race_results_to_gold(spark)
    spark.sparkContext.stop()