#!/usr/bin/python


class ImportWeatherStations:

    def __init__(self):
        from pyspark.sql import SparkSession
        import os
        import json

        self.psql_user = os.environ['POSTGRESQL_USER']
        self.psql_pw = os.environ['POSTGRESQL_PASSWORD']

        self.spark = SparkSession \
            .builder \
            .appName("Write weather stations to DB") \
            .getOrCreate()

        config_file = open('spark_config.json', 'rt')
        self.conf = json.load(config_file)
        config_file.close()

    def main(self, d, file):

        from pyspark.sql.types import FloatType, IntegerType

        spark = d.spark

        # Read the input file from S3
        df = spark.read.text(file)

        # Extract fields from fixed-width text file
        df2 = df.select(
            df.value.substr(1, 11).alias('station_id'),
            df.value.substr(13, 8).alias('latitude'),
            df.value.substr(22, 9).alias('longitude'),
            df.value.substr(32, 6).alias('unknown_1'),
            df.value.substr(39, 2).alias('state'),
            df.value.substr(42, 30).alias('name'),
            df.value.substr(73, 3).alias('unknown_2'),
            df.value.substr(77, 3).alias('unknown_3'),
            df.value.substr(81, 5).alias('unknown_4'),
        )

        # Convert types
        df3 = df2.withColumn("latitude", df2["latitude"].cast(FloatType())) \
            .withColumn("longitude", df2["longitude"].cast(FloatType())) \
            .withColumn("unknown_1", df2["unknown_1"].cast(FloatType())) \
            .withColumn("unknown_4", df2["unknown_4"].cast(IntegerType()))

        # Write to a new table in PostgreSQL DB
        df3.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", self.conf['postgresql_url']) \
            .option("dbtable", "stations") \
            .option("user", self.psql_user) \
            .option("password", self.psql_pw) \
            .option("driver", "org.postgresql.Driver") \
            .save()

        spark.stop()


if __name__ == "__main__":
    import sys
    d = ImportWeatherStations()
    input_file = str(sys.argv[1])
    d.main(d, input_file)
