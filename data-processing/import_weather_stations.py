#!/usr/bin/python


class ImportWeatherStations:

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
        from pyspark.sql.types import FloatType, IntegerType
        # Main function: load the file file and shows 5 lines
        spark = d.spark
        weatherSchema = self.define_schema()

        df = spark.read.text(file)
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

        df3 = df2.withColumn("latitude", df2["latitude"].cast(FloatType()))\
                 .withColumn("longitude", df2["longitude"].cast(FloatType())) \
                 .withColumn("unknown_1", df2["unknown_1"].cast(FloatType())) \
                .withColumn("unknown_4", df2["unknown_4"].cast(IntegerType()))

        df3.show(10)
        #df = spark.read.csv(file, schema=weatherSchema, header=False)

        #dft = df.filter(df.Measurement == 'TAVG')
        #print(dft.show(20))

        spark.stop()

if __name__ == "__main__":
    import sys
    d = ImportWeatherStations()
    input_file = str(sys.argv[1])
    d.main(d, input_file)