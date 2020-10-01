#!/usr/bin/python


class WriteWeatherData:

    def __init__(self):
        from pyspark.sql import SparkSession
        import os

        self.psql_user = os.environ['POSTGRESQL_USER']
        self.psql_pw = os.environ['POSTGRESQL_PASSWORD']

        self.spark = SparkSession \
            .builder \
            .appName("Write weather data to DB") \
            .getOrCreate()


    def define_schema(self):
        # Return a schema for the yyyy.CSV file
        from pyspark.sql.types import StructType, StructField, IntegerType, StringType
        schema = StructType([
            StructField("station", StringType(), True),
            StructField("date", IntegerType(), True),
            StructField("measurement", StringType(), True),
            StructField("value", IntegerType(), True),
            StructField("e1", StringType(), True),
            StructField("e2", StringType(), True),
            StructField("e3", StringType(), True),
            StructField("e4", StringType(), True)])
        return schema

    def main(self, d, file):

        spark = d.spark
        weather_schema = self.define_schema()
        df = spark.read.csv(file, schema=weather_schema, header=False)

        dft = df.filter(df.measurement == 'TAVG')

        print(dft.show(20))

        # Write to a new table in PostgreSQL DB
        dft.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", "jdbc:postgresql://10.0.0.14:5432/ubuntu") \
            .option("dbtable", "weather") \
            .option("user", self.psql_user) \
            .option("password", self.psql_pw) \
            .option("driver", "org.postgresql.Driver") \
            .save()

        # Read back that table and show first 10 rows
        df4 = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://10.0.0.14:5432/ubuntu") \
            .option("dbtable", "weather") \
            .option("user", self.psql_user) \
            .option("password", self.psql_pw) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        df4.show(20)

        spark.stop()


if __name__ == "__main__":
    import sys
    d = WriteWeatherData()
    input_file = str(sys.argv[1])
    d.main(d, input_file)
