# Database joining and querying

## Set up

### Setup PostgreSQL + PostGIS

1. run `install-postgis_1.sh` \
This will reboot the instance
1. run `install-postgis_2.sh`

## How to run 

### Initial run
1. Add files in S3 and edit the file location in the script.
1. Download the counties shapefiles from S3.
1. Load counties with `import_county_shapes_co1990p020.sh`
1. Join counties and stations with `join_counties_stations.sql`.


### Add supporting files

To run a sql file called `file.sql`, open psql with `psql` and type `\i file.sql`.

1. Import county shapefiles \
`import_county_shapes.sh` \
`extract_fips.sql`
1. Convert station lat/long into GIS points \
`stations_to_geom.sql`
1. Create joined tables with counties and contained stations
`join_stations.sql`