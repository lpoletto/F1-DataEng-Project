from os import environ as env
import sys
from helpers import *
from pyspark.sql.functions import lit, col, when, upper, trim, date_format
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


def ingest_fact_race_results_to_gold(spark, execution_date):
    v_file_date = execution_date # Parametro
    
    print("\n################## Step 1 - Read data from races table ##################\n")
    # Filtramos por fecha v_file_date
    results_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/{v_file_date}/results") \
    .filter(f"file_date = '{v_file_date}'") \
    .withColumnRenamed("race_id", "results_race_id") \
    .withColumnRenamed("ingestion_date", "created_at") \
    .withColumn("updated_at", lit(None).cast(TimestampType())) \
    .drop(col("file_date")) \
    .drop(col("data_source"))
    
    print("\n################## results_df ##################\n")
    results_df.show(5, truncate=False)

    races_df = spark.read.parquet(f"{SILVER_LAYER_PATH}/{v_file_date}/races") \
    .filter(f"file_date = '{v_file_date}'") \
    .select(
        col("race_id"),
        date_format(col("race_date"),"yyyyMMdd").cast('int').alias("date_id")
    )
    
    print("\n################## races_df ##################\n")
    races_df.show(5, truncate=False)
    # races_df.printSchema()

    results_join_df = results_df.join(races_df,results_df.results_race_id ==  races_df.race_id,"left") \
    .drop(col("results_race_id"))

    fact_results_final_df = results_join_df.withColumn(
        "position_text",
        when(trim(upper(col("position_text"))) == "D", "disqualified")
        .when(trim(upper(col("position_text"))) == "E", "excluded")
        .when(trim(upper(col("position_text"))) == "F", "failed to qualify")
        .when(trim(upper(col("position_text"))) == "N", "not classified")
        .when(trim(upper(col("position_text"))) == "R", "retired")
        .when(trim(upper(col("position_text"))) == "W", "withdrew")
        .otherwise(col("position_text"))
    )

    column_order = [
        "result_id",
        "race_id",
        "driver_id",
        "constructor_id",
        "status_id",
        "date_id",
        "grid",
        "position",
        "position_text",
        "position_order",
        "points",
        "laps",
        "time",
        "milliseconds",
        "fastest_lap",
        "rank",
        "fastest_lap_time",
        "fastest_lap_speed",
        "created_at",
        "updated_at"
    ]

    # Ordena las columnas usando select y *column_order:
    fact_race_results_df = fact_results_final_df.select(column_order)

    print("\n################## fact_race_results_df ##################\n")
    fact_race_results_df.show(5, truncate=False)
    
    print("\n################## Step 2 - Write data to datalake as parquet ##################\n")
    
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic") 

    output_path = f"{GOLD_LAYER_PATH}/fact_race_results"
    fact_race_results_df.write.mode("overwrite").partitionBy("race_id").parquet(output_path)
    
    print("\n################## Datos guardados en MinIO con éxito. ##################\n")
    print(f"\n################## {output_path} ##################\n")
    
    # print("\n################## Step 3 - Write data to PostgreSQL ##################\n")
    # print("\n################## Saving data to STG Table ##################\n")
    # # Saving data to STG Table
    # fact_race_results_df.write \
    #     .format("jdbc") \
    #     .option("url", f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{F1_DB}") \
    #     .option("dbtable", f"{STG_SCHEMA}.fact_race_results") \
    #     .option("user", DB_USER) \
    #     .option("password", DB_PASSWORD) \
    #     .option("driver", "org.postgresql.Driver") \
    #     .mode("overwrite") \
    #     .save()

    # print("\n################## Step 4 - Merge ##################\n")
    # # Create table if not exists
    # sql_query = f"""
    # CREATE TABLE IF NOT EXISTS {F1_GOLD_SCHEMA}.fact_race_results (
    #     result_sk INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    #     result_id int4 NOT NULL,
    #     race_id int4 NOT NULL,
    #     driver_id int4 NOT NULL,
    #     constructor_id int4 NOT NULL,
    #     status_id int4 NOT NULL,
    #     date_id int4 NOT NULL,
    #     grid int4 NULL,
    #     "position" int4 NULL,
    #     position_text text NULL,
    #     position_order int4 NULL,
    #     points float4 NULL,
    #     laps int4 NULL,
    #     "time" text NULL,
    #     milliseconds int4 NULL,
    #     fastest_lap int4 NULL,
    #     "rank" int4 NULL,
    #     fastest_lap_time text NULL,
    #     fastest_lap_speed text NULL,
    #     created_at timestamp NOT NULL,
    #     updated_at timestamp NULL,
    #     CONSTRAINT fact_race_results_uk UNIQUE (result_id),
    #     CONSTRAINT fk_race_id FOREIGN KEY (race_id) REFERENCES {F1_GOLD_SCHEMA}.dim_race (race_id),
    #     CONSTRAINT fk_driver_id FOREIGN KEY (driver_id) REFERENCES {F1_GOLD_SCHEMA}.dim_driver (driver_id),
    #     CONSTRAINT fk_constructor_id FOREIGN KEY (constructor_id) REFERENCES {F1_GOLD_SCHEMA}.dim_constructor (constructor_id),
    #     CONSTRAINT fk_status_id FOREIGN KEY (status_id) REFERENCES {F1_GOLD_SCHEMA}.dim_status (status_id),
    #     CONSTRAINT fk_date_id FOREIGN KEY (date_id) REFERENCES {F1_GOLD_SCHEMA}.dim_date (date_id)
    # );
    # """
    # execute_sql_query(sql_query, F1_DB)
    
    # # Insert a default row for unknown driver
    # sql_query = f"""
    # SELECT COUNT(*) FROM {F1_GOLD_SCHEMA}.fact_race_results WHERE result_id = -1;
    # """

    # result_query = fetch_sql_query_result(sql_query, F1_DB)

    # if result_query[0][0] == 0:
    #     sql_query = f"""
    #     INSERT INTO {F1_GOLD_SCHEMA}.fact_race_results
    #     (
    #         result_id,
    #         race_id,
    #         driver_id,
    #         constructor_id,
    #         status_id,
    #         date_id,
    #         grid,
    #         position,
    #         position_text,
    #         position_order,
    #         points,
    #         laps,
    #         time,
    #         milliseconds,
    #         fastest_lap,
    #         rank,
    #         fastest_lap_time,
    #         fastest_lap_speed,
    #         created_at,
    #         updated_at
    #     )
    #     VALUES(
    #         -1, -1, -1, -1, -1, 19000101, -1, -1, 'unknown', -1, -1, -1, 'unknown', -1, -1, -1, 'unknown', 'unknown', CURRENT_TIMESTAMP, NULL
    #     );
    #     """
    #     execute_sql_query(sql_query, F1_DB)

    # # Merge data into gold table
    # sql_query = f"""
    # MERGE INTO {F1_GOLD_SCHEMA}.fact_race_results as tgt
    # USING {STG_SCHEMA}.fact_race_results as stg
    # ON tgt.result_id = stg.result_id
    # WHEN NOT MATCHED THEN
    # INSERT(
    #     result_id,
    #     race_id,
    #     driver_id,
    #     constructor_id,
    #     status_id,
    #     date_id,
    #     grid,
    #     position,
    #     position_text,
    #     position_order,
    #     points,
    #     laps,
    #     time,
    #     milliseconds,
    #     fastest_lap,
    #     rank,
    #     fastest_lap_time,
    #     fastest_lap_speed,
    #     created_at
    # ) 
    # VALUES
    # (
    #     stg.result_id,
    #     stg.race_id,
    #     stg.driver_id,
    #     stg.constructor_id,
    #     stg.status_id,
    #     stg.date_id,
    #     stg.grid,
    #     stg.position,
    #     stg.position_text,
    #     stg.position_order,
    #     stg.points,
    #     stg.laps,
    #     stg.time,
    #     stg.milliseconds,
    #     stg.fastest_lap,
    #     stg.rank,
    #     stg.fastest_lap_time,
    #     stg.fastest_lap_speed,
    #     stg.created_at
    # )
    # WHEN MATCHED AND (
    #     COALESCE(tgt.position, -1) <> COALESCE(stg.position, -1)
    #     OR 
    #     COALESCE(tgt.position_text, ' ') <> COALESCE(stg.position_text, ' ') 
    # ) THEN
    # UPDATE set grid=stg.grid,
    #     position=stg.position,
    #     position_text=stg.position_text,
    #     position_order=stg.position_order,
    #     points=stg.points,
    #     laps=stg.laps,
    #     time=stg.time,
    #     milliseconds=stg.milliseconds,
    #     fastest_lap=stg.fastest_lap,
    #     rank=stg.rank,
    #     fastest_lap_time=stg.fastest_lap_time,
    #     fastest_lap_speed=stg.fastest_lap_speed,
    #     updated_at=CURRENT_TIMESTAMP
    # ;
    # """
    # execute_sql_query(sql_query, F1_DB)


if __name__ == "__main__":
    spark = get_spark_session()
    execution_date = sys.argv[1] # datetime.now().strftime("%Y-%m-%d")
    ingest_fact_race_results_to_gold(spark, execution_date)
    spark.sparkContext.stop()