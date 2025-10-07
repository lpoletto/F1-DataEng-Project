from os import environ as env
from datetime import datetime
import sys
from commons import get_spark_session

BRONZE_LAYER_PATH = env["BRONZE_LAYER_PATH"]


def ingest_circuits_to_bronze(spark, execution_date):
    
    v_file_date = execution_date # Fecha de corte para carga inicial  

    # Path del driver de MySQL/Postgres para Spark (JDBC) (También sirve para Redshift)
    JDBC_DRIVER = "com.mysql.cj.jdbc.Driver" # "org.postgresql.Driver"

    # Conexión a MySQL
    mysql_url = f"jdbc:mysql://mysql:3306/{env['MYSQL_DATABASE']}"
    mysql_properties = {
        "user": env["MYSQL_USER"],
        "password": env["MYSQL_PASSWORD"],
        "driver": JDBC_DRIVER
    }

    print("\n################## Step 1 - Read data from MySql database ##################\n")
    try:
        # Leer una tabla de la base de datos f1db
        sql_query = """
        SELECT *
        FROM f1db.circuits
        """

        df = spark.read \
            .format("jdbc") \
            .option("url", mysql_url) \
            .option("driver", mysql_properties["driver"]) \
            .option("query", sql_query) \
            .option("user", mysql_properties["user"]) \
            .option("password", mysql_properties["password"]) \
            .load()

        print("\n################## ✅ Connection successful ##################\n")
    except Exception as e:
        print(f"\n❌ Connection failed: {str(e)}")

    # Mostrar el esquema y las primeras filas del DataFrame
    print("\n################## Mostrar el esquema y las primeras filas del DataFrame ##################\n")
    df.show(10, truncate=False)
    df.printSchema()

    output_path = f"{BRONZE_LAYER_PATH}/{v_file_date}/circuits"
    # Guardar en MinIO en formato CSV
    df.write.csv(output_path, header=True, mode="overwrite")
    print("\n################## Datos guardados en MinIO con éxito. ##################\n")
    print(f"\n################## {output_path} ##################\n")
    # return output_path


if __name__ == "__main__":
    # Configuración de SparkSession con soporte S3
    spark = get_spark_session()
    execution_date = sys.argv[1] # datetime.now().strftime("%Y-%m-%d")
    print(f"Fecha de ejecución recibida: {execution_date}")
    ingest_circuits_to_bronze(spark, execution_date)
    # Detener la sesión de Spark
    spark.sparkContext.stop()