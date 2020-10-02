from pyspark.sql import SparkSession
import os

psql_user = os.environ['POSTGRESQL_USER']
psql_pw = os.environ['POSTGRESQL_PASSWORD']

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/home/ubuntu/postgresql-42.2.16.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://10.0.0.14:5432/heatwave") \
    .option("dbtable", "stations") \
    .option("user", psql_user ) \
    .option("password", psql_pw) \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.printSchema()