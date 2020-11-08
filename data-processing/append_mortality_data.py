#!/usr/bin/python


class AppendMortalityData:

    def __init__(self):
        from pyspark.sql import SparkSession
        import os
        import json

        self.psql_user = os.environ['POSTGRESQL_USER']
        self.psql_pw = os.environ['POSTGRESQL_PASSWORD']

        self.spark = SparkSession \
            .builder \
            .appName("Write mortality data to DB") \
            .getOrCreate()

        config_file = open('spark_config.json', 'rt')
        self.conf = json.load(config_file)
        config_file.close()

    def create_schema(self):
        '''
        Create schema for the final storage in the database

        :return: Schema
        '''
        from pyspark.sql.types import StructType, StructField, \
            IntegerType, DateType

        schema = StructType([
            StructField('state', IntegerType(), True),
            StructField('county_fips', IntegerType(), True),
            StructField('date', DateType(), True),
            StructField('number', IntegerType(), True)
        ])

        return schema

    def load_field_positions(self, vintage):
        '''
        Read from a JSON file the schema for a given mortality year.
        This is how the evolving schema is handled.

        :param vintage: Year for which to retrieve schema
        :return: Dictonary of starting position and length
            for each exiting field of that year
        '''

        import json
        f = open('mort_schema.json', 'rt')
        j = json.load(f)
        return j[str(vintage)]

    def transform_to_schema(self, df, schema):
        '''
        Splits a fixed-with string dataframe into a specified schema

        :param df: Input dataframne
        :param schema: Dictionary of starting position and length for each field
        :return: Dataframe with columns
        '''

        df = df.select(
            df.value.substr(schema['state_s'], schema['state_l']).alias('state'),
            df.value.substr(schema['county_s'], schema['county_l']).alias('county_fips'),
            df.value.substr(schema['month_s'], schema['month_l']).alias('month'),
        )

        return df

    def get_county_concordance(self, vintage):
        '''
        Load county concordance file according to the decade between 1960's and 1990's.

        :param vintage: Year to convert counties from
        :return: Crosswalk dataframe
        '''

        import math
        decade = int(math.floor(vintage / 10.0)) * 10

        file = self.conf['crosswalk_path'] + str(decade) + '.csv'
        xwalk = d.spark.read.csv(file, inferSchema=True, header=True)
        xwalk = xwalk.select(xwalk.fips_state.alias('st_orig'),
                             xwalk.fips_county.alias('co_orig'),
                             xwalk.fips_state_1990.alias('st_ref'),
                             xwalk.fips_county_1990.alias('co_ref'))
        xwalk = xwalk.withColumn('st_orig', xwalk.st_orig / 10) \
            .withColumn('co_orig', xwalk.co_orig / 10) \
            .withColumn('st_ref', xwalk.st_ref / 10) \
            .withColumn('co_ref', xwalk.co_ref / 10)

        return xwalk

    def process_year(self, d, vintage):
        '''
        Read from S3 the mortality file from the year passed as argument,
        create a date column, convert original counties to 1990 reference counties,
        and calculate the mortality count for each date and location.

        :param d: Class instance
        :param vintage: 4-digit year
        :return: Dataframe containing one year of mortality data
        '''
        from pyspark.sql.types import IntegerType
        from pyspark.sql import functions as F
        from pyspark.sql.functions import concat, unix_timestamp, to_date, lit
        from pyspark.sql.functions import when

        spark = d.spark

        # Read the input file from S3
        file = self.conf['mort_path'] + str(vintage) + '.txt'
        df_nondelimited = spark.read.text(file)

        # Extract fields from fixed-width text file
        field_pos = self.load_field_positions(vintage)
        df = self.transform_to_schema(df_nondelimited, field_pos)

        # Get year from the filename, not from the content
        vintage_str = str(vintage)

        # Deal with unknown days encoded as 99
        if 'day_s' in self.load_field_positions(vintage):
            df = df.withColumn('day', when(df.day > 31, '01').otherwise(df.day))
        # And with missing day for newer years
        else:
            df = df.withColumn('day', lit('01'))

        # Create date from year, month, and day
        df = df.withColumn('date', to_date(unix_timestamp(
            concat(lit(vintage_str), df.month, df.day), 'yyyyMMdd').cast('timestamp')))

        # Transform historical county number into 1990 ref standardized number
        df = df.withColumnRenamed('county_fips', 'county_fips_orig')

        # Join with crosswalk Dataframe to get the counties in their reference state as of 1990.
        xwalk = self.get_county_concordance(vintage)
        df = df.join(xwalk, [df.state == xwalk.st_orig,
                               df.county_fips_orig == xwalk.co_orig])

        # Convert types
        dfc = df.withColumn('state', df['state'].cast(IntegerType())) \
            .withColumn('county_fips_orig', df['county_fips_orig'].cast(IntegerType())) \
            .withColumn('co_ref', df['co_ref'].cast(IntegerType())) \
            .withColumn('month', df['month'].cast(IntegerType())) \
            .withColumn('day', df['day'].cast(IntegerType()))

        dfc = dfc.withColumnRenamed('co_ref', 'county_fips')

        # Sum up death counts for each combination of parameters
        dfc = dfc.groupby(dfc.state, dfc.county_fips, dfc.date) \
            .agg(F.count(dfc.date).alias('number')) \
            .sort(dfc.state, dfc.county_fips, dfc.date)

        return dfc

    def main(self, d, first_vintage, last_vintage):
        '''
        Load in a Dataframe a range of mortality data year and
        append it to the mortality table in PostgreSQL

        :param d: Class instance
        :param first_vintage: first year in the range
        :param last_vintage: last year in the range, inclusive
        :return: -
        '''
        if first_vintage > last_vintage:
            raise Exception("First year cannot be after the last year")
        if (first_vintage < 1968) or (first_vintage > 2018):
            raise Exception("First year out of range")
        if (last_vintage < 1968) or (last_vintage > 2018):
            raise Exception("Last year out of range")

        # Create empty dataframe to gather all years
        main_df = d.spark.createDataFrame([], self.create_schema())

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
            .option("dbtable", "mortality") \
            .option("user", self.psql_user) \
            .option("password", self.psql_pw) \
            .option("driver", "org.postgresql.Driver") \
            .save()

        d.spark.stop()

        return


if __name__ == "__main__":
    import sys

    d = AppendMortalityData()
    first_vintage = int(sys.argv[1])
    last_vintage = int(sys.argv[2])
    d.main(d, first_vintage, last_vintage)
