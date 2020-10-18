#!/usr/bin/python


class CleanMortalityData:

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

    def load_schema(self, vintage):
        import json
        f = open('../mort_schema.json', 'rt')
        j = json.load(f)
        return j[str(vintage)]

    def transform_to_schema(self, df, schema):
        from pyspark.sql.types import StringType
        from pyspark.sql.functions import lit

        df = df.select(
            df.value.substr(schema['state_s'], schema['state_l']).alias('state'),
            df.value.substr(schema['county_s'], schema['county_l']).alias('county_fips'),
            df.value.substr(schema['year_s'], schema['year_l']).alias('year'),
            df.value.substr(schema['month_s'], schema['month_l']).alias('month')
        )

        # Calendar day
        try:
            df = df.value.substr(schema['day_s'], schema['day_l']).alias('day')
        except AttributeError:
            print('"Day" field does not exists')
            df = df.withColumn('day', lit(None).cast(StringType()))

        # Weekday
        try:
            df = df.value.substr(schema['weekday_s'], schema['weekday_l']).alias('weekday')
        except AttributeError:
            print('"Weekday" field does not exists')
            df = df.withColumn('weekday', lit(None).cast(StringType()))

        # Manner
        try:
            df = df.value.substr(schema['manner_s'], schema['manner_l']).alias('manner')
        except AttributeError:
            print('"Manner" field does not exists')
            df = df.withColumn('manner', lit(None).cast(StringType()))

        df.printSchema()

        df2 = df
        df2.show(10)
        return df2

    def main(self, d, vintage):

        from pyspark.sql.types import IntegerType
        from pyspark.sql import functions as F
        from pyspark.sql.functions import concat, unix_timestamp, to_date, lit

        spark = d.spark

        # Read the input file from S3
        file = 's3a://data-engineer.club/mort/mort' + str(vintage) + '.txt'
        df = spark.read.text(file)
        df.show(20)

        # Extract fields from fixed-width text file
        schema = self.load_schema(vintage)
        df2 = self.transform_to_schema(df, schema)
        df2.show(20)

        print(schema['year_l'])
        if schema['year_l'] == 2:
            df2 = df2.withColumn('year', concat(lit('19'), df2.year))

        # # Create date from year and month
        df2 = df2.withColumn('date', to_date(unix_timestamp(
                   concat(df2.year, df2.month), 'yyyyMM').cast('timestamp')))
        #
        # df2.show(20)
        #
        # # Convert types
        df3 = df2.withColumn('county_fips', df2['county_fips'].cast(IntegerType())) \
                 .withColumn('year', df2['year'].cast(IntegerType())) \
                 .withColumn('month', df2['month'].cast(IntegerType())) \
                 .withColumn('weekday', df2['weekday'].cast(IntegerType()))

        # # Sum up death counts for each combination of parameters
        df3 = df3.groupby(df3.date, df3.weekday, df3.state, df3.county_fips, df3.manner) \
                 .agg(F.count(df3.date).alias('number')) \
                 .sort(df3.date, df3.weekday, df3.state, df3.county_fips, df3.manner)
        #
        df3.show(30)
        #
        # #dft = df3.filter(df3.month == 5)
        # #print(dft.show(20))




        spark.stop()

        print(file)
        print(type(file))

if __name__ == "__main__":
    import sys
    d = CleanMortalityData()
    input_file = str(sys.argv[1])
    d.main(d, input_file)
