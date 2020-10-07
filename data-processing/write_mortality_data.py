#!/usr/bin/python


class WriteMortalityData:

    def __init__(self):
        from pyspark.sql import SparkSession
        import os

        self.psql_user = os.environ['POSTGRESQL_USER']
        self.psql_pw = os.environ['POSTGRESQL_PASSWORD']

        self.spark = SparkSession \
            .builder \
            .appName("Write mortality data to DB") \
            .getOrCreate()

#            .config("spark.jars", "/home/ubuntu/postgresql-42.2.16.jar") \

    def main(self, d, file):

        from pyspark.sql.types import FloatType, IntegerType
        from pyspark.sql import functions as F
        from pyspark.sql.functions import concat, col, lit, unix_timestamp, to_date

        spark = d.spark

        # Read the input file from S3
        df = spark.read.text(file)

        # Extract fields from fixed-width text file
        df2 = df.select(
            df.value.substr(21, 2).alias('state'),
            df.value.substr(23, 3).alias('county_fips'),
            df.value.substr(102, 4).alias('year'),
            df.value.substr(65, 2).alias('month'),
            df.value.substr(85, 1).alias('weekday'),
            df.value.substr(107, 1).alias('manner')
        )

        #Concatenate date


        df2 = df2.withColumn('date', to_date(unix_timestamp( \
                  concat(df2.year, df2.month), 'yyyyMM').cast('timestamp')))

        df2.show(20)

        # Convert types
        df3 = df2.withColumn('county_fips', df2['county_fips'].cast(IntegerType())) \
                 .withColumn('year', df2['year'].cast(IntegerType())) \
                 .withColumn('month', df2['month'].cast(IntegerType())) \
                 .withColumn('weekday', df2['weekday'].cast(IntegerType()))

        # Sum up death counts for each combination of parameters
        df3 = df3.groupby(df3.date, df3.weekday, df3.state, df3.county_fips, df3.manner) \
                 .agg(F.count(df3.date).alias('number')) \
                 .sort(df3.date, df3.weekday, df3.state, df3.county_fips, df3.manner)

        df3.show(30)

        #dft = df3.filter(df3.month == 5)
        #print(dft.show(20))

        # Write to a new table in PostgreSQL DB
        df3.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", "jdbc:postgresql://10.0.0.14:5432/heatwave") \
            .option("dbtable", "mortality") \
            .option("user", self.psql_user) \
            .option("password", self.psql_pw) \
            .option("driver", "org.postgresql.Driver") \
            .save()

        # Read back that table and show first 10 rows
        # df4 = spark.read \
        #     .format("jdbc") \
        #     .option("url", "jdbc:postgresql://10.0.0.14:5432/heatwave") \
        #     .option("dbtable", "mortality") \
        #     .option("user", self.psql_user) \
        #     .option("password", self.psql_pw) \
        #     .option("driver", "org.postgresql.Driver") \
        #     .load()

        #df4.show(20)

        spark.stop()


if __name__ == "__main__":
    import sys, time

    start_time = time.time()
    d = WriteMortalityData()
    input_file = str(sys.argv[1])
    d.main(d, input_file)
    print('Execution time : ' + str(round(time.time() - start_time)))
