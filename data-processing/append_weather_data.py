#!/usr/bin/python


class AppendWeatherData:

    def __init__(self):
        from pyspark.sql import SparkSession
        import os
        import json

        self.psql_user = os.environ['POSTGRESQL_USER']
        self.psql_pw = os.environ['POSTGRESQL_PASSWORD']

        self.spark = SparkSession \
            .builder \
            .appName("Write weather data to DB") \
            .getOrCreate()

        config_file = open('spark_config.json', 'rt')
        self.conf = json.load(config_file)
        config_file.close()

    def create_final_schema(self):
        '''
        Create schema for the final storage in the database

        :return: Schema
        '''
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
        '''
        Create schema found in the weather data files.

        :return: Schema
        '''

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
        '''
        Read from S3 the weather file from the year passed as argument and
        filter the entries to keep only the average temperature.

        :param d: Class instance
        :param vintage: 4-digit year
        :return: Dataframe containing one year of average temperature data
        '''
        from pyspark.sql.functions import unix_timestamp, to_date, col

        spark = d.spark
        weather_schema = self.create_input_schema()

        # Read the input file from S3
        file = self.conf['weather_path'] + str(vintage) + '.csv'
        df = spark.read.csv(file, schema=weather_schema, header=False)

        # Fix date format
        df = df.withColumn('date', to_date(unix_timestamp(col('date'),
                                           'yyyyMMdd').cast("timestamp")))

        # Filter only the average T measurement
        df_tavg = df.filter(df.measurement == 'TAVG')

        df_tavg = df_tavg.select(
            df_tavg.station,
            df_tavg.date,
            df_tavg.measurement,
            df_tavg.value
        )

        return df_tavg

    def main(self, d, first_vintage, last_vintage):
        '''
        Load in a Dataframe a range of weather data year and
        append it to the weather table in PostgreSQL

        :param d: Class instance
        :param first_vintage: first year in the range
        :param last_vintage:  last year in the range, inclusive
        :return: -
        '''
        if first_vintage > last_vintage:
            raise Exception("First year cannot be after the last year")
        if (first_vintage < 1968) or (first_vintage > 2018):
            raise Exception("First year out of range")
        if (last_vintage < 1968) or (last_vintage > 2018):
            raise Exception("Last year out of range")

        # Create empty dataframe to gather all years
        main_df = d.spark.createDataFrame([], self.create_final_schema())

        # Load year by year and append to the main dataframe
        vintages = range(first_vintage, last_vintage + 1)
        for vintage in vintages:
            print(vintage)
            new_df = self.process_year(d, vintage)
            main_df = main_df.union(new_df)

        # Write to PostgreSQL
        main_df.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", self.conf['postgresql_url']) \
            .option("dbtable", "weather") \
            .option("user", self.psql_user) \
            .option("password", self.psql_pw) \
            .option("batchsize", 1000) \
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
