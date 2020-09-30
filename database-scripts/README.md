# Tools installation

## Setup PostgreSQL + PostGIS

run `install-postgis_1.sh` \
This will reboot the instance \
run `install-postgis_2.sh`

## Connect Spark to Postgres

https://zheguang.github.io/blog/systems/2019/02/16/connect-spark-to-postgres.html

https://severalnines.com/database-blog/big-data-postgresql-and-apache-spark

https://medium.com/@usmanazhar4/how-to-read-and-write-from-database-in-spark-using-pyspark-150d39cdbb72

https://stackoverflow.com/questions/34948296/using-pyspark-to-connect-to-postgresql

https://jdbc.postgresql.org/about/about.html

https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html

https://www.psycopg.org/docs/install.html

## Add supporting files

To run a sql file called `file.sql`, open psql with `psql` and type `\i file.sql`.

1. Import weather stations \
`submit-stations2db.sh`, which calls `write_weather_stations.py`, both located in `../data-processing/`
1. Import county shapefiles \
`import_county_shapes.sh` \
`extract_fips.sql`
1. Convert station lat/long into GIS points \
`stations_to_geom.sql`
1. Create joined tables with counties and contained stations
`join_stations.sql`