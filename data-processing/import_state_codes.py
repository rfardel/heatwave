#!/usr/bin/python


class ImportStateCodes:

    def __init__(self):
        from pyspark.sql import SparkSession
        import os

        self.psql_user = os.environ['POSTGRESQL_USER']
        self.psql_pw = os.environ['POSTGRESQL_PASSWORD']

        self.spark = SparkSession \
            .builder \
            .appName("Write mortality data to DB") \
            .getOrCreate()

    def main(self, d):

        file = 's3a://data-engineer.club/aux/state_codes.csv'
        states = d.spark.read.csv(file, inferSchema=True, header=True)
        states = states.select(states.number.alias('state_num'),
                               states.code.alias('state'))
        states.show()

        states.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", "jdbc:postgresql://10.0.0.14:5432/heatwave") \
            .option("dbtable", "states") \
            .option("user", self.psql_user) \
            .option("password", self.psql_pw) \
            .option("driver", "org.postgresql.Driver") \
            .save()

        d.spark.stop()

        return


if __name__ == "__main__":

    d = ImportStateCodes()
    d.main(d)
