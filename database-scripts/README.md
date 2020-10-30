# Database: adding supporting files, joining, and querying

## Set up

### Setup PostgreSQL + PostGIS

1. run `install-postgis_1.sh` \
This will reboot the instance
1. run `install-postgis_2.sh`

## How to run 

Note: To run a sql file called `file.sql`, open psql with `psql` and type `\i file.sql`.

### Load the data from Spark

1. Once PostgreSQL + PostGIS is ready, load the weather and mortality data from Spark, see `data-processing` folder.'

### Add supporting files

1. Load from S3 and import county shapefiles: `import_county_shapes.sh`

### Preprocess

1. Convert station lat/long into GIS points with `stations_to_geom.sql`.
1. Aggregate weather data by month with `weather_monthly.sql`.
1. Aggregate mortality data by month with `mortality_monthly.sql`.

### Join

1. Join counties and stations with `join_counties_stations.sql`. This is the spatial join using `STContains`.
1. Join weather and counties with `join_weather_counties.sql`.
1. Join mortality with weather by counties with `join_mortality.sql`.