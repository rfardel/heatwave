from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/home/ubuntu/postgresql-42.2.16.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://10.0.0.14:5432/ubuntu") \
    .option("dbtable", "stations") \
    .option("user", "testsp") \
    .option("password", "testsp") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.show()

df.write \
    .format("jdbc") \
    .mode("append") \
    .option("url", "jdbc:postgresql://10.0.0.14:5432/ubuntu") \
    .option("dbtable", "station3") \
    .option("user", "testsp") \
    .option("password", "testsp") \
    .option("driver", "org.postgresql.Driver") \
    .save()

df2 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://10.0.0.14:5432/ubuntu") \
    .option("dbtable", "station3") \
    .option("user", "testsp") \
    .option("password", "testsp") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df2.show()

#df.printSchema()