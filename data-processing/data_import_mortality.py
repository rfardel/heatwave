#!/usr/bin/python

class DataImportTurnstile:

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
            StructField("ESTRID", IntegerType(), True),
            StructField("ESID", IntegerType(), True),
            StructField("street_lang", IntegerType(), True),
            StructField("canton_abbr", StringType(), True),
            StructField("OFS_no", IntegerType(), True),
            StructField("NPA", IntegerType(), True),
            StructField("NPA_add", IntegerType(), True),
            StructField("town", StringType(), True),
            StructField("street", StringType(), True),
            StructField("street_abbr", StringType(), True),
            StructField("index_ref", StringType(), True),
            StructField("name_official", StringType(), True),
            StructField("street_kind", IntegerType(), True),
            StructField("street_no", IntegerType(), True),
            StructField("date_export", StringType(), True)])
        return schema

    def main(self, d, file):
        # Main function: load the ESID file and shows 5 lines
        spark = d.spark
        df = spark.read.text(file)
        print(df.take(5))

        df.select(
            df.value.substr(23, 3).alias('county'),
            df.value.substr(102, 4).alias('year'),
            df.value.substr(65, 2).alias('month'),
            df.value.substr(85, 1).alias('weekday'),
            df.value.substr(107, 1).alias('manner')
        ).show(20)


if __name__ == "__main__":
    d = DataImportTurnstile()
    file_location = "Mort2018US.PubUse.txt"
    d.main(d, file_location)
