#!/usr/bin/python


class ShowMortalityData:

    def __init__(self):
        from pyspark.sql import SparkSession

        self.spark = SparkSession \
            .builder \
            .appName("Write mortality data to DB") \
            .getOrCreate()

#            .config("spark.jars", "/home/ubuntu/postgresql-42.2.16.jar") \

    def main(self, d, file):

        from pyspark.sql.types import FloatType, IntegerType
        spark = d.spark

        # Read the input file from S3
        df = spark.read.text(file)

        # Extract fields from fixed-width text file
        df2 = df.select(
            df.value.substr(21, 2).alias('state'),
            df.value.substr(23, 3).alias('county'),
            df.value.substr(102, 4).alias('year'),
            df.value.substr(65, 2).alias('month'),
            df.value.substr(85, 1).alias('weekday'),
            df.value.substr(107, 1).alias('manner')
        )

        # Convert types
        df3 = df2.withColumn("year", df2["year"].cast(IntegerType())) \
            .withColumn("month", df2["month"].cast(IntegerType())) \
            .withColumn("weekday", df2["weekday"].cast(IntegerType()))

        # Show first 20 rows
        df3.show(20)

        print('count = ' + str(df3.count()))

        df4 = df3.groupby(df3.year, df3.month).agg(count(df3.manner))

        # Aggregate by state, county, month,  year, weekday, manner


        #dft = df3.filter(df3.month == 5)
        #print(dft.show(20))





        spark.stop()


if __name__ == "__main__":
    import sys
    d = ShowMortalityData()
    input_file = str(sys.argv[1])
    d.main(d, input_file)
