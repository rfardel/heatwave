#!/usr/bin/python


class AppendWeatherData:

    def __init__(self):
        from pyspark.sql import SparkSession
        import os

        self.psql_user = os.environ['POSTGRESQL_USER']
        self.psql_pw = os.environ['POSTGRESQL_PASSWORD']

        self.spark = SparkSession \
            .builder \
            .appName("Write weather data to DB") \
            .getOrCreate()


    def create_final_schema(self):

        # Return a schema for the yyyy.CSV file
        from pyspark.sql.types import StructType, StructField,\
            IntegerType, StringType, DateType

        schema = StructType([
            StructField("station", StringType(), True),
            StructField("date", DateType(), True),
            StructField("measurement", StringType(), True),
            StructField("value", IntegerType(), True)
        ])

        return schema

    def create_input_schema(self):

        # Return a schema for the yyyy.CSV file
        from pyspark.sql.types import StructType, StructField,\
            IntegerType, StringType

        schema = StructType([
            StructField("station", StringType(), True),
            StructField("date", StringType(), True),
            StructField("measurement", StringType(), True),
            StructField("value", IntegerType(), True),
            StructField("e1", StringType(), True),
            StructField("e2", StringType(), True),
            StructField("e3", StringType(), True),
            StructField("e4", StringType(), True)
        ])

        return schema

    def process_year(self, d, vintage):

        from pyspark.sql.functions import unix_timestamp, to_date, col

        spark = d.spark
        weather_schema = self.create_input_schema()

        # Read the input file from S3
        file = 's3a://data-engineer.club/csv/' + str(vintage) + '.csv'
        df = spark.read.csv(file, schema=weather_schema, header=False)
        df.show(22)

        # Fix date format
        df = df.withColumn('date', to_date(unix_timestamp(col('date'),
                                           'yyyyMMdd').cast("timestamp")))

        # Filter only the average T measurement
        dft = df.filter(df.measurement == 'TAVG')

        dft = dft.select(
            dft.station,
            dft.date,
            dft.measurement,
            dft.value
        )

        dft.show(24)

        return dft

    def main(self, d, first_vintage, last_vintage):

        if first_vintage > last_vintage:
            raise Exception("First year cannot be after the last year")
        if (first_vintage < 1968) or (first_vintage > 2018):
            raise Exception("First year out of range")
        if (last_vintage < 1968) or (last_vintage > 2018):
            raise Exception("Last year out of range")

        main_df = d.spark.createDataFrame([], self.create_final_schema())
        main_df.show()

        vintages = range(first_vintage, last_vintage + 1)
        for vintage in vintages:
            print(vintage)
            new_df = self.process_year(d, vintage)
            main_df = main_df.union(new_df)

        main_df.show(20)

        # Write to a new table in PostgreSQL DB
        main_df.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", "jdbc:postgresql://10.0.0.14:5432/heatwave") \
            .option("dbtable", "weather") \
            .option("user", self.psql_user) \
            .option("password", self.psql_pw) \
            .option("driver", "org.postgresql.Driver") \
            .save()

        d.spark.stop()

        return


if __name__ == "__main__":
    import sys

    d = AppendWeatherData()
    first_vintage = int(sys.argv[1])
    last_vintage = int(sys.argv[2])
    d.main(d, first_vintage, last_vintage)
