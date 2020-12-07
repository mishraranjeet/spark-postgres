from pyspark.sql import SparkSession
import os

sparkClassPath = 'postgresql-42.2.17.jar'

def spark_db_connct():
    spark = SparkSession.builder.config('spark.driver.extraClassPath', sparkClassPath).getOrCreate()
    url = "jdbc:postgresql://127.0.0.1:5432/localdb"
    properties = {'user': 'postgres', 'password': ''}
    df = spark.read.jdbc(url=url, table='session', properties=properties)
    df.createOrReplaceTempView("session")

    sql = spark.sql("select * from user limit 5").toPandas()
    print(sql.to_dict())
    return 'cool'


if __name__ == "__main__":
    print(spark_db_connct())