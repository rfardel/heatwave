#!/usr/bin/python


class ImportWeatherInv:

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
            StructField("Date", IntegerType(), True),
            StructField("Measurement", StringType(), True),
            StructField("Value", IntegerType(), True),
            StructField("E1", StringType(), True),
            StructField("E2", StringType(), True),
            StructField("E3", StringType(), True),
            StructField("E4", StringType(), True)])
        return schema

    def main(self, d, file):
        # Main function: load the file file and shows 5 lines
        spark = d.spark
        weatherSchema = self.define_schema()

        df = spark.read.text(file)
        df2 = df.select(
            df.value.substr(1,10).alias('id'),
            df.value.substr(12,5).alias('other')
        )

        df2.show(10)
        #df = spark.read.csv(file, schema=weatherSchema, header=False)

        #dft = df.filter(df.Measurement == 'TAVG')
        #print(dft.show(20))

        spark.stop()

if __name__ == "__main__":
    import sys
    d = ImportWeatherInv()
    input_file = str(sys.argv[1])
    d.main(d, input_file)