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


def merge_fact_constructor_standings_to_gold(spark):
    
    print("\n################## Step 1 - Read data from gold layer ##################\n")
    fact_constructor_standings_df = spark.read.parquet(f"{GOLD_LAYER_PATH}/fact_constructor_standings")

    print("\n################## Step 2 - Write data to PostgreSQL ##################\n")
    print("\n################## Saving data to STG Table ##################\n")
    # Saving data to STG Table
    fact_constructor_standings_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{F1_DB}") \
        .option("dbtable", f"{STG_SCHEMA}.fact_constructor_standings") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    
    print("\n################## Step 3 - CREATE TABLE IF NOT EXISTS ##################\n")
    sql_query = f"""
    CREATE TABLE IF NOT EXISTS {F1_GOLD_SCHEMA}.fact_constructor_standings (
        constructor_standings_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        constructor_standings_id int4 NOT NULL,
        race_id int4 NOT NULL,
        constructor_id int4 NOT NULL,
        date_id int4 NOT NULL,
        points float4 NULL,
        wins int4 NULL,
        "rank" int4 NULL,
        created_at timestamp NOT NULL,
        updated_at timestamp NULL,
        CONSTRAINT fact_constructor_standings_uk UNIQUE (constructor_standings_id),
        CONSTRAINT fk_constructor_standings_race_id FOREIGN KEY (race_id) REFERENCES {F1_GOLD_SCHEMA}.dim_race (race_id),
        CONSTRAINT fk_constructor_standings_constructor_id FOREIGN KEY (constructor_id) REFERENCES {F1_GOLD_SCHEMA}.dim_constructor (constructor_id),
        CONSTRAINT fk_constructor_standings_date_id FOREIGN KEY (date_id) REFERENCES {F1_GOLD_SCHEMA}.dim_date (date_id)
    );
    """
    execute_sql_query(sql_query, F1_DB)

    # Insert a default row for unknown driver
    sql_query = f"""
    SELECT COUNT(*) FROM {F1_GOLD_SCHEMA}.fact_constructor_standings WHERE constructor_standings_id = -1;
    """

    result_query = fetch_sql_query_result(sql_query, F1_DB)

    if result_query[0][0] == 0:
        sql_query = f"""
        INSERT INTO {F1_GOLD_SCHEMA}.fact_constructor_standings (
            constructor_standings_id,
            race_id,
            constructor_id,
            date_id,
            points,
            wins,
            "rank",
            created_at,
            updated_at
        )
        VALUES(
            -1, -1, -1, 19000101, 0, 0, -1, CURRENT_TIMESTAMP, NULL
        );
        """
        execute_sql_query(sql_query, F1_DB)

    print("\n################## Step 4 - Merge data into gold table ##################\n")

    # Merge data into gold table
    sql_query = f"""
    MERGE INTO {F1_GOLD_SCHEMA}.fact_constructor_standings as tgt
    USING {STG_SCHEMA}.fact_constructor_standings as stg
    ON tgt.constructor_standings_id = stg.constructor_standings_id
    WHEN NOT MATCHED THEN
    INSERT(
        constructor_standings_id,
        race_id,
        constructor_id,
        date_id,
        points,
        wins,
        "rank",
        created_at
    ) 
    VALUES
    (
        stg.constructor_standings_id,
        stg.race_id,
        stg.constructor_id,
        stg.date_id,
        stg.points,
        stg.wins,
        stg.rank,
        stg.created_at
    )
    WHEN MATCHED AND (
        COALESCE(tgt.points, -1) <> COALESCE(stg.points, -1)
        OR 
        COALESCE(tgt.rank, -1) <> COALESCE(stg.rank, -1) 
    ) THEN
    UPDATE set points=stg.points,
        wins=stg.wins,
        rank=stg.rank,
        updated_at=CURRENT_TIMESTAMP
    ;
    """
    execute_sql_query(sql_query, F1_DB)


if __name__ == "__main__":
    spark = get_spark_session()
    merge_fact_constructor_standings_to_gold(spark)
    spark.sparkContext.stop()