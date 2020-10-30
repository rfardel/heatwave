#!/bin/sh

# To be run on the PostgreSQL instance

# Get shapefiles from S3
aws s3 cp s3://data-engineer.club/gis_shapes . --recursive

# Transform into .sql and load into database
shp2pgsql -s 4269 co1990p020.shp > co1990p020.sql counties
psql -h localhost -d heatwave -U $POSTGRESQL_USER -f co1990p020.sql
