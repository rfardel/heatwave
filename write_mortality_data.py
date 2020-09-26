#!/usr/bin/python


class WriteMortalityData:

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
            df.value.substr(23,3).alias('county'),
            df.value.substr(102,4).alias('year'),
            df.value.substr(65, 2).alias('month'),
            df.value.substr(85,1).alias('weekday'),
            df.value.substr(107,1).alias('manner')
        )

        # Convert types
        df3 = df2.withColumn("year", df2["year"].cast(IntegerType())) \
            .withColumn("month", df2["month"].cast(IntegerType())) \
            .withColumn("weekday", df2["weekday"].cast(IntegerType()))

        # Show first 20 rows
        df3.show(20)

        dft = df3.filter(df3.month == 5)
        print(dft.show(20))

        # Write to a new table in PostgreSQL DB
        dft.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", "jdbc:postgresql://10.0.0.14:5432/ubuntu") \
            .option("dbtable", "mortality") \
            .option("user", "testsp") \
            .option("password", "testsp") \
            .option("driver", "org.postgresql.Driver") \
            .save()

        # Read back that table and show first 10 rows
        df4 = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://10.0.0.14:5432/ubuntu") \
            .option("dbtable", "mortality") \
            .option("user", "testsp") \
            .option("password", "testsp") \
            .option("driver", "org.postgresql.Driver") \
            .load()

        df4.show(20)

        spark.stop()


if __name__ == "__main__":
    import sys
    d = WriteMortalityData()
    input_file = str(sys.argv[1])
    d.main(d, input_file)
