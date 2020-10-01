#!/usr/bin/python

class ImportWeather:

    def __init__(self):
        # Initialize the spark session
        from pyspark.sql import SparkSession
        self.spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .getOrCreate()

    def define_schema(self):
        # Return a schema for the ESID file
        from pyspark.sql.types import StructType, StructField, IntegerType, StringType
        schema = StructType([
            StructField("Station", StringType(), True),
            StructField("Date", StringType(), True),
            StructField("Measurement", StringType(), True),
            StructField("Value", IntegerType(), True),
            StructField("E1", StringType(), True),
            StructField("E2", StringType(), True),
            StructField("E3", StringType(), True),
            StructField("E4", StringType(), True)])
        return schema

    def main(self, d, file):
        # Main function: load the file file and shows 5 lines
        from pyspark.sql.functions import col, unix_timestamp, to_date

        spark = d.spark
        weatherSchema = self.define_schema()
        df = spark.read.csv(file, schema=weatherSchema, header=False)

        df = df.withColumn('Date',
                           to_date(unix_timestamp(col('Date'), 'yyyyMMdd').cast("timestamp")))

        dft = df.filter(df.Measurement == 'TAVG')
        print(dft.show(20))

        spark.stop()

if __name__ == "__main__":
    import sys
    d = ImportWeather()
    input_file = str(sys.argv[1])
    d.main(d, input_file)